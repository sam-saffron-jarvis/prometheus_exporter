# frozen_string_literal: true

require "logger"
require "socket"
require "stringio"
require "timeout"
require "zlib"

module PrometheusExporter::Server
  class WebServer
    MAX_REQUEST_LINE_BYTES = 8 * 1024
    MAX_HEADER_LINE_BYTES = 8 * 1024
    MAX_HEADER_BYTES = 64 * 1024
    MAX_HEADER_COUNT = 100
    MAX_BODY_CHUNK_BYTES = 1024 * 1024
    DEFAULT_MAX_CONNECTIONS = 100
    DEFAULT_HEADER_TIMEOUT = 5
    DEFAULT_BODY_READ_TIMEOUT = 30
    DEFAULT_WRITE_TIMEOUT = 5
    DEFAULT_STOP_TIMEOUT = 1
    ACCEPT_RETRY_INITIAL_DELAY = 0.01
    ACCEPT_RETRY_MAX_DELAY = 0.25
    private_constant :ACCEPT_RETRY_INITIAL_DELAY, :ACCEPT_RETRY_MAX_DELAY

    Request = Struct.new(:method, :path, :version, :headers, keyword_init: true)
    private_constant :Request

    class HTTPError < StandardError
      attr_reader :status, :headers

      def initialize(status, message, headers = {})
        @status = status
        @headers = headers
        super(message)
      end
    end
    private_constant :HTTPError

    class SocketReader
      def initialize(socket)
        @socket = socket
        @buffer = +""
        @buffer.force_encoding(Encoding::BINARY)
      end

      def read_headers(max_bytes, deadline)
        loop do
          if (ending = @buffer.index("\r\n\r\n"))
            length = ending + 4
            raise HTTPError.new(431, "Request headers are too large") if length > max_bytes

            return consume(length)
          end

          ensure_before_deadline(deadline)
          raise HTTPError.new(400, "Malformed request headers") if invalid_newline?(@buffer)
          raise HTTPError.new(431, "Request headers are too large") if @buffer.bytesize >= max_bytes

          read_more(deadline, max_bytes - @buffer.bytesize)
        end
      end

      def read_line(max_bytes, deadline)
        loop do
          if (ending = @buffer.index("\r\n"))
            raise HTTPError.new(400, "Request line is too large") if ending + 2 > max_bytes

            return consume(ending + 2).byteslice(0, ending)
          end

          ensure_before_deadline(deadline)
          raise HTTPError.new(400, "Malformed request framing") if invalid_newline?(@buffer)
          raise HTTPError.new(400, "Request line is too large") if @buffer.bytesize >= max_bytes

          read_more(deadline, max_bytes - @buffer.bytesize)
        end
      end

      def read_exact(bytes, deadline)
        while @buffer.bytesize < bytes
          ensure_before_deadline(deadline)
          read_more(deadline, bytes - @buffer.bytesize)
        end
        consume(bytes)
      end

      private

      def consume(bytes)
        result = @buffer.byteslice(0, bytes)
        @buffer = @buffer.byteslice(bytes, @buffer.bytesize - bytes) || +""
        result
      end

      def invalid_newline?(string)
        offset = 0
        while (newline = string.index("\n", offset))
          return true if newline.zero? || string.getbyte(newline - 1) != 13

          offset = newline + 1
        end
        false
      end

      def ensure_before_deadline(deadline)
        return if Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline

        raise HTTPError.new(408, "Request timed out")
      end

      def read_more(deadline, maximum)
        loop do
          ensure_before_deadline(deadline)
          result = @socket.read_nonblock([16 * 1024, maximum].min, exception: false)
          case result
          when :wait_readable
            wait_for_io(:read, deadline)
          when :wait_writable
            wait_for_io(:write, deadline)
          when nil, ""
            raise HTTPError.new(400, "Unexpected end of request")
          else
            @buffer << result
            return
          end
        end
      rescue EOFError
        raise HTTPError.new(400, "Unexpected end of request")
      end

      def wait_for_io(direction, deadline)
        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        raise HTTPError.new(408, "Request timed out") if remaining <= 0

        readers = direction == :read ? [@socket] : nil
        writers = direction == :write ? [@socket] : nil
        unless IO.select(readers, writers, nil, remaining)
          raise HTTPError.new(408, "Request timed out")
        end
      end
    end
    private_constant :SocketReader

    STATUS_TEXT = {
      200 => "OK",
      400 => "Bad Request",
      401 => "Unauthorized",
      404 => "Not Found",
      405 => "Method Not Allowed",
      408 => "Request Timeout",
      411 => "Length Required",
      413 => "Payload Too Large",
      417 => "Expectation Failed",
      422 => "Unprocessable Entity",
      429 => "Too Many Requests",
      431 => "Request Header Fields Too Large",
      500 => "Internal Server Error",
      503 => "Service Unavailable",
      505 => "HTTP Version Not Supported",
    }.freeze
    private_constant :STATUS_TEXT

    attr_reader :collector

    def accept_error
      @state_mutex.synchronize { @accept_error }
    end

    PAGESIZE =
      begin
        `getconf PAGESIZE`.to_i
      rescue StandardError
        4096
      end
    private_constant :PAGESIZE

    def initialize(opts)
      @port = opts[:port] || PrometheusExporter::DEFAULT_PORT
      @bind = opts[:bind] || PrometheusExporter::DEFAULT_BIND_ADDRESS
      @timeout = opts[:timeout] || PrometheusExporter::DEFAULT_TIMEOUT
      @header_timeout = opts.fetch(:header_timeout, DEFAULT_HEADER_TIMEOUT)
      @body_read_timeout = opts.fetch(:body_read_timeout, DEFAULT_BODY_READ_TIMEOUT)
      @write_timeout = opts.fetch(:write_timeout, DEFAULT_WRITE_TIMEOUT)
      @stop_timeout = opts.fetch(:stop_timeout, DEFAULT_STOP_TIMEOUT)
      @max_connections = opts.fetch(:max_connections, DEFAULT_MAX_CONNECTIONS)
      {
        header_timeout: @header_timeout,
        body_read_timeout: @body_read_timeout,
        write_timeout: @write_timeout,
        stop_timeout: @stop_timeout,
      }.each do |name, value|
        unless value.is_a?(Numeric) && value.finite? && value.positive?
          raise ArgumentError, "#{name} must be a positive number"
        end
      end
      @max_header_bytes = opts.fetch(:max_header_bytes, MAX_HEADER_BYTES)
      @max_body_chunk_bytes = opts.fetch(:max_body_chunk_bytes, MAX_BODY_CHUNK_BYTES)
      {
        max_connections: @max_connections,
        max_header_bytes: @max_header_bytes,
        max_body_chunk_bytes: @max_body_chunk_bytes,
      }.each do |name, value|
        unless value.is_a?(Integer) && value.positive?
          raise ArgumentError, "#{name} must be a positive integer"
        end
      end
      @verbose = opts[:verbose] || false
      @auth = opts[:auth]
      @realm = opts[:realm] || PrometheusExporter::DEFAULT_REALM
      @pid = Process.pid

      @metrics_total =
        PrometheusExporter::Metric::Counter.new(
          "collector_metrics_total",
          "Total metrics processed by exporter web.",
        )

      @sessions_total =
        PrometheusExporter::Metric::Counter.new(
          "collector_sessions_total",
          "Total send_metric sessions processed by exporter web.",
        )

      @bad_metrics_total =
        PrometheusExporter::Metric::Counter.new(
          "collector_bad_metrics_total",
          "Total mis-handled metrics by collector.",
        )

      @metrics_total.observe(0)
      @sessions_total.observe(0)
      @bad_metrics_total.observe(0)

      log_target = opts[:log_target] || (@verbose ? $stderr : File::NULL)
      @logger = Logger.new(log_target)
      @logger.info "Using Basic Authentication via #{@auth}" if @verbose && @auth

      if %w[ALL ANY].include?(@bind)
        @logger.info "Listening on both 0.0.0.0/:: network interfaces" if @verbose
        @bind = nil
      end

      @collector = opts[:collector] || Collector.new(logger: @logger)
      @ssl_context = build_ssl_context(opts[:tls_cert_file], opts[:tls_key_file])

      @state_mutex = Mutex.new
      @workers = {}
      @clients = {}
      @stopping = false
      @accept_error = nil
      @listeners = build_listeners
      @wakeup_reader, @wakeup_writer = IO.pipe
    end

    def start
      @state_mutex.synchronize do
        return @runner if @runner

        @accept_error = nil
        @runner = Thread.new { accept_loop }
      end
    end

    def stop
      runner = nil
      listeners = nil
      wakeup_writer = nil

      @state_mutex.synchronize do
        return if @stopping

        @stopping = true
        listeners = @listeners
        @listeners = nil
        wakeup_writer = @wakeup_writer
        runner = @runner
      end

      Array(listeners).each { |listener| close_quietly(listener) }
      stop_deadline = monotonic_deadline(@stop_timeout)
      begin
        wakeup_writer&.write_nonblock(".")
      rescue IOError, SystemCallError
        nil
      end
      accept_threads = [runner].compact - [Thread.current]
      join_threads_until(accept_threads, stop_deadline)
      accept_threads.each { |thread| thread.kill if thread.alive? }
      join_threads_until(accept_threads, monotonic_deadline(0.1))

      clients = @state_mutex.synchronize { @clients.keys }
      clients.each { |socket| close_quietly(socket) }

      workers = @state_mutex.synchronize { @workers.keys - [Thread.current] }
      join_threads_until(workers, stop_deadline)
      workers.each { |worker| worker.kill if worker.alive? }
      join_threads_until(workers, monotonic_deadline(0.1))

      close_quietly(@wakeup_reader)
      close_quietly(@wakeup_writer)
      @state_mutex.synchronize do
        @runner = nil
        @wakeup_reader = nil
        @wakeup_writer = nil
      end
      nil
    end

    # Retained for callers which used WebServer#handle_metrics with a request
    # object exposing WEBrick's streaming #body API.
    def handle_metrics(req, res)
      @sessions_total.observe
      failed = false
      req.body do |chunk|
        begin
          process_metrics_chunk(chunk)
        rescue => e
          failed = true
          log_collector_error(e)
          res.body = "Bad Metrics #{e}"
          res.status = collector_error_status(e)
          break
        end
      end

      unless failed
        res.body = "OK"
        res.status = 200
      end
    end

    def metrics
      metric_text = nil
      begin
        Timeout.timeout(@timeout) { metric_text = @collector.prometheus_metrics_text }
      rescue Timeout::Error
        @logger.error "Generating Prometheus metrics text timed out"
      end

      metrics = []

      metrics << add_gauge(
        "collector_working",
        "Is the master process collector able to collect metrics",
        metric_text && metric_text.length > 0 ? 1 : 0,
      )

      metrics << add_gauge("collector_rss", "total memory used by collector process", get_rss)

      metrics << @metrics_total
      metrics << @sessions_total
      metrics << @bad_metrics_total

      <<~TEXT
      #{metrics.map(&:to_prometheus_text).join("\n\n")}
      #{metric_text}
      TEXT
    end

    def get_rss
      begin
        File.read("/proc/#{@pid}/statm").split(" ")[1].to_i * PAGESIZE
      rescue StandardError
        0
      end
    end

    def add_gauge(name, help, value)
      gauge = PrometheusExporter::Metric::Gauge.new(name, help)
      gauge.observe(value)
      gauge
    end

    # Retained for compatibility with request/response objects that provide the
    # small subset of WEBrick's header API used here.
    def authenticate(req, res)
      authorization = Array(req.header["authorization"]).first
      return true if valid_authorization?(authorization)

      res.status = 401
      res.body = "Unauthorized"
      res["WWW-Authenticate"] = basic_auth_challenge if res.respond_to?(:[]=)
      false
    end

    private

    def build_ssl_context(cert_file, key_file)
      if cert_file.nil? != key_file.nil?
        raise ArgumentError, "tls_cert_file and tls_key_file must be supplied together"
      end
      return unless cert_file

      require "openssl"
      certificate = OpenSSL::X509::Certificate.new(File.read(cert_file))
      private_key = OpenSSL::PKey.read(File.read(key_file))
      unless certificate.check_private_key(private_key)
        raise ArgumentError, "tls_cert_file and tls_key_file do not match"
      end

      context = OpenSSL::SSL::SSLContext.new
      context.cert = certificate
      context.key = private_key
      context
    end

    def build_listeners
      addresses =
        if @bind.nil?
          [[Socket::AF_INET6, "::"], [Socket::AF_INET, "0.0.0.0"]]
        else
          Addrinfo
            .getaddrinfo(@bind, @port, nil, :STREAM, nil, Socket::AI_PASSIVE)
            .filter_map do |address|
              case address.afamily
              when Socket::AF_INET, Socket::AF_INET6
                # Supported below.
              else
                next
              end

              [address.afamily, address.ip_address]
            end
            .uniq
        end

      listeners = []
      port = @port
      errors = []

      addresses.each do |family, address|
        begin
          listener = Socket.new(family, Socket::SOCK_STREAM, 0)
          listener.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEADDR, 1)
          if family == Socket::AF_INET6 && Socket.const_defined?(:IPV6_V6ONLY)
            begin
              listener.setsockopt(Socket::IPPROTO_IPV6, Socket::IPV6_V6ONLY, 1)
            rescue SystemCallError
              # Some platforms expose IPV6_V6ONLY but do not allow changing it.
            end
          end
          listener.bind(Socket.sockaddr_in(port, address))
          listener.listen(Socket::SOMAXCONN)
          port = listener.local_address.ip_port if port.to_i.zero?
          listeners << listener
        rescue SystemCallError, SocketError => e
          close_quietly(listener)
          errors << e
        end
      end

      raise(errors.first || SocketError.new("No usable bind addresses")) if listeners.empty?

      listeners
    rescue StandardError
      listeners&.each { |listener| close_quietly(listener) }
      raise
    end

    def accept_loop
      retry_delay = ACCEPT_RETRY_INITIAL_DELAY

      loop do
        state =
          @state_mutex.synchronize do
            unless @stopping || !@listeners || @listeners.empty?
              [@listeners.dup, @wakeup_reader, @workers.length >= @max_connections]
            end
          end
        break unless state

        listeners, wakeup_reader, at_capacity = state
        readers = at_capacity ? [wakeup_reader] : listeners + [wakeup_reader]
        ready = IO.select(readers)
        next unless ready

        if ready.first.include?(wakeup_reader)
          drain_wakeup
          next
        end

        ready.first.each do |listener|
          break unless worker_capacity_available?

          begin
            accepted = accept_socket(listener)
          rescue Errno::ECONNABORTED
            next
          rescue Errno::EMFILE, Errno::ENFILE => e
            @logger.warn "Temporarily unable to accept prometheus exporter connection: #{e}"
            wait_for_accept_retry(retry_delay)
            retry_delay = [retry_delay * 2, ACCEPT_RETRY_MAX_DELAY].min
            break
          end
          next if accepted == :wait_readable

          retry_delay = ACCEPT_RETRY_INITIAL_DELAY
          socket = accepted.is_a?(Array) ? accepted.first : accepted
          spawn_worker(socket)
        rescue IOError, Errno::EBADF, Errno::EINVAL
          raise unless stopping?
        end
      end
    rescue => e
      unless stopping?
        @state_mutex.synchronize { @accept_error = e }
        @logger.error "Failed to run prometheus collector web on port #{@port}: #{e}"
      end
    ensure
      @state_mutex.synchronize { @runner = nil if @runner == Thread.current }
    end

    def accept_socket(listener)
      listener.accept_nonblock(exception: false)
    end

    def worker_capacity_available?
      @state_mutex.synchronize { !@stopping && @workers.length < @max_connections }
    end

    def wait_for_accept_retry(delay)
      ready = IO.select([@wakeup_reader], nil, nil, delay)
      drain_wakeup if ready
    rescue IOError, Errno::EBADF
      raise unless stopping?
    end

    def drain_wakeup
      loop do
        result = @wakeup_reader.read_nonblock(1024, exception: false)
        break if result == :wait_readable || result.nil?
      end
    rescue IOError, Errno::EBADF
      raise unless stopping?
    end

    def wake_accept_loop
      @wakeup_writer&.write_nonblock(".", exception: false)
    rescue IOError, SystemCallError
      nil
    end

    def spawn_worker(socket)
      reject = false

      @state_mutex.synchronize do
        if @stopping || @workers.length >= @max_connections
          reject = true
        else
          worker =
            Thread.new do
              begin
                serve_client(socket)
              ensure
                close_quietly(socket)
                @state_mutex.synchronize do
                  @clients.delete(socket)
                  @workers.delete(Thread.current)
                end
                wake_accept_loop
              end
            end
          @clients[socket] = true
          @workers[worker] = true
        end
      end

      # Capacity is checked before accept; rejection here is only possible when
      # shutdown races with an accepted connection.
      close_quietly(socket) if reject
    rescue ThreadError => e
      close_quietly(socket)
      @logger.error "Failed to allocate prometheus exporter worker: #{e}"
    end

    def serve_client(socket)
      header_deadline = monotonic_deadline(@header_timeout)
      io = wrap_tls(socket, header_deadline)
      reader = SocketReader.new(io)
      request = parse_request(reader.read_headers(@max_header_bytes, header_deadline))
      status, body, headers = route(request, reader)
      write_response(io, status, body, headers)
    rescue HTTPError => e
      write_response(io || (@ssl_context ? nil : socket), e.status, e.message, e.headers)
    rescue IOError, SystemCallError
      # Clients commonly disconnect without reading the response.
    rescue => e
      # A TLS peer may disconnect or send plaintext before an HTTP request exists.
      return if ssl_error?(e)

      if @verbose
        @logger.error "Prometheus exporter request failed: #{e.inspect}\n#{Array(e.backtrace).join("\n")}"
      end
      write_response(io || (@ssl_context ? nil : socket), 500, "Internal Server Error", {})
    ensure
      close_quietly(io) if io && io != socket
    end

    def wrap_tls(socket, deadline)
      return socket unless @ssl_context

      ssl_socket = OpenSSL::SSL::SSLSocket.new(socket, @ssl_context)
      ssl_socket.sync_close = false

      loop do
        result = ssl_socket.accept_nonblock(exception: false)
        case result
        when :wait_readable
          wait_for(socket, :read, deadline)
        when :wait_writable
          wait_for(socket, :write, deadline)
        else
          return ssl_socket
        end
      end
    end

    def wait_for(socket, direction, deadline)
      remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
      raise HTTPError.new(408, "Request timed out") if remaining <= 0

      readers = direction == :read ? [socket] : nil
      writers = direction == :write ? [socket] : nil
      unless IO.select(readers, writers, nil, remaining)
        raise HTTPError.new(408, "Request timed out")
      end
    end

    def parse_request(raw_headers)
      lines = raw_headers.byteslice(0, raw_headers.bytesize - 4).split("\r\n", -1)
      request_line = lines.shift
      if request_line.bytesize > MAX_REQUEST_LINE_BYTES
        raise HTTPError.new(400, "Malformed request line")
      end

      match = %r{\A([A-Z]+) ([^ ]+) HTTP/(\d\.\d)\z}n.match(request_line)
      raise HTTPError.new(400, "Malformed request line") unless match

      method, target, version = match.captures
      case version
      when "1.0", "1.1"
        # Supported below.
      else
        raise HTTPError.new(505, "HTTP version is not supported")
      end
      unless target.start_with?("/") && target.match?(/\A[\x21-\x7e]+\z/n) && !target.include?("#")
        raise HTTPError.new(400, "Malformed request target")
      end
      raise HTTPError.new(431, "Too many request headers") if lines.length > MAX_HEADER_COUNT

      headers = {}
      lines.each do |line|
        if line.bytesize > MAX_HEADER_LINE_BYTES
          raise HTTPError.new(431, "Request header line is too large")
        end
        if line.start_with?(" ", "\t")
          raise HTTPError.new(400, "Obsolete folded headers are not supported")
        end

        header =
          /\A([!#$%&'*+\-.^_`|~0-9A-Za-z]+):[ \t]*([^\x00-\x08\x0a-\x1f\x7f]*)\z/n.match(line)
        raise HTTPError.new(400, "Malformed request header") unless header

        name = header[1].downcase
        raise HTTPError.new(400, "Duplicate request header") if headers.key?(name)

        headers[name] = header[2].strip
      end

      host = headers["host"]
      if version == "1.1" && (!host || host.empty?)
        raise HTTPError.new(400, "HTTP/1.1 requires a Host header")
      end
      raise HTTPError.new(400, "Malformed Host header") if host&.match?(%r{[\x00-\x20,\x7f\\/]}n)
      raise HTTPError.new(417, "Expect is not supported") if headers.key?("expect")
      if version == "1.0" && headers.key?("transfer-encoding")
        raise HTTPError.new(400, "HTTP/1.0 does not support Transfer-Encoding")
      end
      if headers.key?("content-length") && headers.key?("transfer-encoding")
        raise HTTPError.new(400, "Content-Length and Transfer-Encoding cannot be combined")
      end

      Request.new(
        method: method,
        path: target.split("?", 2).first,
        version: version,
        headers: headers,
      )
    end

    def route(request, reader)
      case request.path
      when "/metrics"
        require_method(request, "GET")
        reject_get_body(request)
        return unauthorized_response unless valid_authorization?(request.headers["authorization"])

        body = metrics
        headers = { "Vary" => "Accept-Encoding" }
        if accepts_gzip?(request.headers["accept-encoding"])
          body = gzip(body)
          headers["Content-Encoding"] = "gzip"
        end
        [200, body, headers]
      when "/ping"
        require_method(request, "GET")
        reject_get_body(request)
        [200, "PONG", {}]
      when "/send-metrics"
        require_method(request, "POST")
        receive_metrics(request, reader)
      else
        [
          404,
          "Not Found! The Prometheus Ruby Exporter only listens on /ping, /metrics and /send-metrics",
          {},
        ]
      end
    end

    def require_method(request, expected)
      return if request.method == expected

      raise HTTPError.new(405, "Method Not Allowed", { "Allow" => expected })
    end

    def reject_get_body(request)
      if request.headers.key?("transfer-encoding")
        raise HTTPError.new(400, "GET requests cannot use Transfer-Encoding")
      end

      content_length = parse_content_length(request.headers["content-length"])
      raise HTTPError.new(400, "GET requests cannot contain a body") if content_length&.positive?
    end

    def receive_metrics(request, reader)
      transfer_encoding = request.headers["transfer-encoding"]
      content_length_header = request.headers["content-length"]
      @sessions_total.observe

      if transfer_encoding
        unless transfer_encoding.downcase == "chunked"
          raise HTTPError.new(400, "Only chunked Transfer-Encoding is supported")
        end
        receive_chunked_metrics(reader)
      elsif content_length_header
        length = parse_content_length(content_length_header)
        raise HTTPError.new(413, "Metrics payload is too large") if length > @max_body_chunk_bytes

        if length.positive?
          body_deadline = monotonic_deadline(@body_read_timeout)
          process_metrics_chunk(reader.read_exact(length, body_deadline))
        end
      else
        raise HTTPError.new(411, "A request body length is required")
      end

      [200, "OK", {}]
    rescue HTTPError
      raise
    rescue => e
      log_collector_error(e)
      raise HTTPError.new(collector_error_status(e), "Bad Metrics #{e}")
    end

    def receive_chunked_metrics(reader)
      loop do
        body_deadline = monotonic_deadline(@body_read_timeout)
        size_line = reader.read_line(MAX_HEADER_LINE_BYTES, body_deadline)
        unless size_line.match?(/\A[0-9A-Fa-f]+\z/n)
          raise HTTPError.new(400, "Malformed chunk size")
        end
        raise HTTPError.new(413, "Metrics chunk is too large") if size_line.bytesize > 16

        size = size_line.to_i(16)
        raise HTTPError.new(413, "Metrics chunk is too large") if size > @max_body_chunk_bytes

        if size.zero?
          trailer = reader.read_line(MAX_HEADER_LINE_BYTES, body_deadline)
          raise HTTPError.new(400, "Chunk trailers are not supported") unless trailer.empty?
          break
        end

        chunk = reader.read_exact(size, body_deadline)
        unless reader.read_exact(2, body_deadline) == "\r\n"
          raise HTTPError.new(400, "Malformed chunk framing")
        end
        process_metrics_chunk(chunk)
      end
    end

    def parse_content_length(value)
      return unless value
      unless value.match?(/\A(?:0|[1-9][0-9]*)\z/n)
        raise HTTPError.new(400, "Malformed Content-Length")
      end

      value.to_i
    end

    def process_metrics_chunk(chunk)
      @metrics_total.observe
      @collector.process(chunk)
    rescue => e
      @bad_metrics_total.observe
      raise e
    end

    def log_collector_error(error)
      @logger.error "\n\n#{error.inspect}\n#{Array(error.backtrace).join("\n")}\n\n" if @verbose
    end

    def collector_error_status(error)
      status = error.status_code if error.respond_to?(:status_code)
      status.is_a?(Integer) && status.between?(400, 599) ? status : 500
    rescue StandardError
      500
    end

    def unauthorized_response
      return 200, nil, {} unless @auth

      [401, "Unauthorized", { "WWW-Authenticate" => basic_auth_challenge }]
    end

    def valid_authorization?(authorization)
      return true unless @auth

      scheme, encoded = authorization.to_s.split(" ", 2)
      return false unless scheme&.casecmp?("Basic") && encoded && !encoded.include?(" ")

      decoded = encoded.unpack1("m0")
      username, password = decoded.split(":", 2)
      return false unless username && password

      File.foreach(@auth) do |line|
        entry_user, password_hash = line.chomp.split(":", 2)
        next unless entry_user == username && password_hash && !password_hash.empty?

        password_hash = password_hash.delete_prefix("{CRYPT}")
        candidate = password.crypt(password_hash)
        return secure_compare(candidate, password_hash)
      end
      false
    rescue ArgumentError, Errno::EINVAL, SystemCallError
      false
    end

    def secure_compare(left, right)
      return false unless left.bytesize == right.bytesize

      result = 0
      left.bytes.zip(right.bytes) { |a, b| result |= a ^ b }
      result.zero?
    end

    def basic_auth_challenge
      realm = @realm.to_s.gsub(/[\x00-\x1f\x7f]/, "").gsub(/["\\]/) { |character| "\\#{character}" }
      %(Basic realm="#{realm}")
    end

    def accepts_gzip?(header)
      return false unless header

      qualities = Hash.new { |hash, coding| hash[coding] = [] }
      header
        .split(",", -1)
        .each do |entry|
          coding, *parameters = entry.strip.split(";", -1)
          coding = coding&.strip
          next unless coding&.match?(/\A(?:[!#$%&'*+\-.^_`|~0-9A-Za-z]+|\*)\z/n)

          quality = parse_encoding_quality(parameters)
          qualities[coding.downcase] << quality if quality
        end

      explicit = qualities["gzip"]
      quality = explicit.empty? ? qualities["*"].max : explicit.max
      !!(quality && quality.positive?)
    end

    def parse_encoding_quality(parameters)
      return 1.0 if parameters.empty?
      return unless parameters.length == 1

      match = /\Aq[ \t]*=[ \t]*(0(?:\.[0-9]{0,3})?|1(?:\.0{0,3})?)\z/i.match(parameters.first.strip)
      match && match[1].to_f
    end

    def gzip(body)
      output = StringIO.new
      writer = Zlib::GzipWriter.new(output)
      writer.write(body)
      writer.close
      output.string
    end

    def write_response(socket, status, body, headers)
      return unless socket && !socket.closed?

      status = 500 unless status.is_a?(Integer) && status.between?(100, 599)
      body = body.to_s.b
      reason = STATUS_TEXT.fetch(status, "Error")
      response_headers = {
        "Content-Type" => "text/plain; charset=utf-8",
        "Content-Length" => body.bytesize.to_s,
        "Connection" => "close",
      }.merge(headers)
      response = +"HTTP/1.1 #{status} #{reason}\r\n"
      response_headers.each do |name, value|
        name = name.to_s
        value = value.to_s
        unless name.match?(/\A[!#$%&'*+\-.^_`|~0-9A-Za-z]+\z/n) &&
                 value.match?(/\A[^\x00-\x1f\x7f]*\z/n)
          raise ArgumentError, "Invalid response header"
        end
        response << "#{name}: #{value}\r\n"
      end
      response << "\r\n"
      response << body
      write_all_nonblock(socket, response, monotonic_deadline(@write_timeout))
    rescue ArgumentError, HTTPError, IOError, SystemCallError
      nil
    rescue => e
      raise unless ssl_error?(e)
    end

    def write_all_nonblock(socket, string, deadline)
      offset = 0
      while offset < string.bytesize
        result =
          socket.write_nonblock(
            string.byteslice(offset, string.bytesize - offset),
            exception: false,
          )
        case result
        when :wait_readable
          wait_for(socket, :read, deadline)
        when :wait_writable
          wait_for(socket, :write, deadline)
        else
          raise IOError, "Response socket made no write progress" unless result.positive?

          offset += result
        end
      end
    end

    def ssl_error?(error)
      return false unless Object.const_defined?(:OpenSSL, false)

      openssl = Object.const_get(:OpenSSL, false)
      return false unless openssl.const_defined?(:SSL, false)

      ssl = openssl.const_get(:SSL, false)
      ssl.const_defined?(:SSLError, false) && error.is_a?(ssl.const_get(:SSLError, false))
    end

    def join_threads_until(threads, deadline)
      threads.each do |thread|
        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        thread.join(remaining) if remaining.positive?
      end
    end

    def monotonic_deadline(timeout)
      Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    end

    def stopping?
      @state_mutex.synchronize { @stopping }
    end

    def close_quietly(socket)
      return unless socket

      begin
        socket.shutdown(Socket::SHUT_RDWR) if socket.respond_to?(:shutdown) && !socket.closed?
      rescue StandardError
        nil
      ensure
        begin
          socket.close unless socket.closed?
        rescue StandardError
          nil
        end
      end
    end
  end
end
