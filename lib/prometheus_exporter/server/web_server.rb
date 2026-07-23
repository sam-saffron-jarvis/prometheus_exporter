# frozen_string_literal: true

require "logger"
require "puma"
require "puma/server"
require "socket"
require "stringio"
require "timeout"
require "zlib"

module PrometheusExporter::Server
  class WebServer
    class PumaAdapter
      MAX_EPHEMERAL_BIND_ATTEMPTS = 5
      UNAVAILABLE_FAMILY_ERRORS =
        %i[EAFNOSUPPORT EADDRNOTAVAIL EPROTONOSUPPORT]
          .filter_map { |name| Errno.const_get(name) if Errno.const_defined?(name) }
          .freeze

      def initialize(app, log_writer:, max_record_size:, logger:, verbose:)
        @app = app
        @log_writer = log_writer
        @max_record_size = max_record_size
        @logger = logger
        @verbose = verbose
        @listeners = []
      end

      def start(hosts:, port:, ssl_context: nil)
        attempts = 0
        begin
          attempts += 1
          build_server
          bound_port = bind_listeners(hosts, port, ssl_context)
          @runner = @server.run(true, thread_name: "prometheus-exporter")
          [@runner, bound_port]
        rescue Errno::EADDRINUSE
          close_listeners
          retry if port.zero? && attempts < MAX_EPHEMERAL_BIND_ATTEMPTS
          raise
        rescue StandardError
          close_listeners
          raise
        end
      end

      def stop
        @server.stop(true) if @runner&.alive?
      ensure
        close_listeners
        @runner = nil
      end

      private

      def build_server
        @server =
          Puma::Server.new(
            @app,
            nil,
            log_writer: @log_writer,
            environment: "production",
            http_content_length_limit: @max_record_size,
          )
      end

      def bind_listeners(hosts, requested_port, ssl_context)
        bound_port = requested_port
        unavailable_errors = []

        hosts.each do |host|
          next if listener_covers_host?(host)

          begin
            listener = add_listener(host, bound_port, ssl_context)
            @listeners << listener
            bound_port = listener.local_address.ip_port if bound_port.zero?
          rescue *UNAVAILABLE_FAMILY_ERRORS => e
            raise if hosts.one?

            unavailable_errors << e
            if @verbose
              @logger.warn "Could not bind unavailable address family #{host}:#{bound_port}: #{e.message}"
            end
          end
        end

        raise unavailable_errors.last if @listeners.empty?

        bound_port
      end

      def listener_covers_host?(host)
        return false unless host == "0.0.0.0"

        @listeners.any? { |listener| dual_stack_ipv6_wildcard?(listener) }
      end

      def dual_stack_ipv6_wildcard?(listener)
        address = listener.local_address
        return false unless address.ipv6? && address.ip_address == "::"

        listener.getsockopt(Socket::IPPROTO_IPV6, Socket::IPV6_V6ONLY).int.zero?
      rescue IOError, SystemCallError, SocketError
        false
      end

      def add_listener(host, port, ssl_context)
        if ssl_context
          @server.add_ssl_listener(host, port, ssl_context)
        else
          @server.add_tcp_listener(host, port)
        end
      end

      def close_listeners
        @listeners.each do |listener|
          listener.close if listener && !listener.closed?
        rescue IOError, SystemCallError
          nil
        end
        @listeners.clear
      end
    end
    private_constant :PumaAdapter

    PAGESIZE =
      begin
        `getconf PAGESIZE`.to_i
      rescue StandardError
        4096
      end
    private_constant :PAGESIZE

    DEFAULT_MAX_RECORD_SIZE = 1024 * 1024

    NOT_FOUND =
      "Not Found! The Prometheus Ruby Exporter only listens on /ping, /metrics and /send-metrics"
    private_constant :NOT_FOUND

    attr_reader :collector, :port

    def initialize(opts)
      @port = opts[:port] || PrometheusExporter::DEFAULT_PORT
      @bind = opts[:bind] || PrometheusExporter::DEFAULT_BIND_ADDRESS
      @timeout = opts[:timeout] || PrometheusExporter::DEFAULT_TIMEOUT
      @verbose = opts[:verbose] || false
      @auth = opts[:auth]
      @realm = opts[:realm] || PrometheusExporter::DEFAULT_REALM
      @max_record_size = positive_integer(opts.fetch(:max_record_size, DEFAULT_MAX_RECORD_SIZE))
      @tls_cert_file = opts[:tls_cert_file]
      @tls_key_file = opts[:tls_key_file]
      @pid = Process.pid
      @stopped = false

      validate_options!
      build_self_metrics
      build_loggers(opts[:log_target])
      @logger.info "Using Basic Authentication via #{@auth}" if @verbose && @auth
      @collector = opts[:collector] || Collector.new(logger: @logger)
    end

    def call(env)
      log_request(env)

      case env["PATH_INFO"]
      when "/metrics"
        metrics_response(env)
      when "/send-metrics"
        handle_metrics(env)
      when "/ping"
        response(200, "PONG")
      else
        response(404, NOT_FOUND)
      end
    end

    def start
      return @runner if @runner&.alive?
      raise "prometheus collector web server has been stopped" if @stopped

      begin
        hosts = listener_hosts
        if %w[ALL ANY].include?(@bind)
          @logger.info "Listening on both 0.0.0.0/:: network interfaces"
        end
        ssl_context = build_ssl_context if @tls_cert_file
        @adapter =
          PumaAdapter.new(
            self,
            log_writer: @puma_log_writer,
            max_record_size: @max_record_size,
            logger: @logger,
            verbose: @verbose,
          )
        @runner, @port = @adapter.start(hosts: hosts, port: @port, ssl_context: ssl_context)
        @runner
      rescue => e
        @logger&.error "Failed to start prometheus collector web on port #{@port}: #{e}"
        @adapter&.stop
        @adapter = nil
        @runner = nil
        raise
      end
    end

    def stop
      return if @stopped

      @adapter&.stop
    ensure
      @runner = nil
      @stopped = true
      close_owned_log
    end

    def metrics
      metric_text = nil
      begin
        Timeout.timeout(@timeout) { metric_text = @collector.prometheus_metrics_text }
      rescue Timeout::Error
        @logger.error "Generating Prometheus metrics text timed out"
      end

      self_metrics = [
        add_gauge(
          "collector_working",
          "Is the master process collector able to collect metrics",
          metric_text && metric_text.length > 0 ? 1 : 0,
        ),
        add_gauge("collector_rss", "total memory used by collector process", get_rss),
        @metrics_total,
        @sessions_total,
        @bad_metrics_total,
      ]

      <<~TEXT
        #{self_metrics.map(&:to_prometheus_text).join("\n\n")}
        #{metric_text}
      TEXT
    end

    def get_rss
      File.read("/proc/#{@pid}/statm").split(" ")[1].to_i * PAGESIZE
    rescue StandardError
      0
    end

    def add_gauge(name, help, value)
      gauge = PrometheusExporter::Metric::Gauge.new(name, help)
      gauge.observe(value)
      gauge
    end

    private

    def positive_integer(value)
      value = Integer(value)
      raise ArgumentError if value <= 0

      value
    rescue TypeError, ArgumentError
      raise ArgumentError, "max_record_size must be larger than 0"
    end

    def validate_options!
      if @tls_cert_file.nil? != @tls_key_file.nil?
        raise ArgumentError, "tls_cert_file and tls_key_file must be configured together"
      end
      if @realm.to_s.match?(/[\x00-\x1f\x7f]/)
        raise ArgumentError, "realm must not contain control characters"
      end
    end

    def listener_hosts
      return %w[:: 0.0.0.0] if %w[ALL ANY].include?(@bind)
      return [@bind] unless @bind == "localhost"

      hosts =
        Socket
          .ip_address_list
          .filter_map do |address|
            address.ip_address if address.ipv4_loopback? || address.ipv6_loopback?
          end
          .uniq
      hosts.empty? ? ["127.0.0.1"] : hosts
    end

    def build_self_metrics
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

      [@metrics_total, @sessions_total, @bad_metrics_total].each { |metric| metric.observe(0) }
    end

    def build_loggers(log_target)
      @log_io =
        if log_target.respond_to?(:write)
          log_target
        elsif log_target
          @owns_log_io = true
          File.open(log_target, "a")
        elsif @verbose
          $stderr
        else
          @owns_log_io = true
          File.open(File::NULL, "w")
        end
      @puma_log_writer = Puma::LogWriter.new(@log_io, @log_io)
      @logger = Logger.new(@log_io)
      @log_enabled = @verbose || !log_target.nil?
    end

    def close_owned_log
      return unless @owns_log_io && @log_io && !@log_io.closed?

      @logger&.close
    rescue IOError, SystemCallError
      nil
    end

    def build_ssl_context
      require "puma/minissl"

      context = Puma::MiniSSL::Context.new
      context.cert = @tls_cert_file
      context.key = @tls_key_file
      context.verify_mode = Puma::MiniSSL::VERIFY_NONE
      context
    end

    def metrics_response(env)
      return unauthorized_response unless authenticated?(env)

      body = metrics
      headers = { "Vary" => "Accept-Encoding" }
      if accepts_gzip?(env["HTTP_ACCEPT_ENCODING"])
        output = StringIO.new
        writer = Zlib::GzipWriter.new(output)
        begin
          writer.write(body)
        ensure
          writer.close
        end
        body = output.string
        headers["Content-Encoding"] = "gzip"
      end

      response(200, body, headers)
    end

    def accepts_gzip?(header)
      qualities = {}
      header
        .to_s
        .split(",")
        .each do |entry|
          coding, *parameters = entry.split(";")
          coding = coding.to_s.strip.downcase
          next if coding.empty?

          quality = 1.0
          parameters.each do |parameter|
            name, value = parameter.split("=", 2).map { |part| part&.strip }
            next unless name&.casecmp?("q")

            quality = valid_quality(value) || 0.0
          end
          qualities[coding] = quality
        end

      quality = qualities.key?("gzip") ? qualities["gzip"] : qualities.fetch("*", 0.0)
      quality.positive?
    end

    def valid_quality(value)
      return unless value&.match?(/\A(?:0(?:\.\d{0,3})?|1(?:\.0{0,3})?)\z/)

      value.to_f
    end

    def handle_metrics(env)
      unless env["REQUEST_METHOD"] == "POST"
        return response(405, "Method Not Allowed", "Allow" => "POST")
      end

      content_length = parse_content_length(env["CONTENT_LENGTH"]) if env["CONTENT_LENGTH"]
      if env["CONTENT_LENGTH"] && !content_length
        return response(400, "Invalid Content-Length", "Connection" => "close")
      end
      if content_length && content_length > @max_record_size
        return response(413, "Metric payload is too large", "Connection" => "close")
      end

      body = env.fetch("rack.input").read(@max_record_size + 1).to_s
      if body.bytesize > @max_record_size
        return response(413, "Metric payload is too large", "Connection" => "close")
      end
      if content_length && body.bytesize != content_length
        return response(400, "Incomplete metric payload", "Connection" => "close")
      end

      @sessions_total.observe
      @metrics_total.observe
      @collector.process(body)
      response(200, "OK")
    rescue => e
      @logger.error "\n\n#{e.inspect}\n#{e.backtrace}\n\n" if @log_enabled
      @bad_metrics_total.observe
      response(collector_error_status(e), "Bad Metrics #{e}")
    end

    def parse_content_length(value)
      return unless value.to_s.match?(/\A\d+\z/)

      Integer(value, 10)
    rescue ArgumentError
      nil
    end

    def collector_error_status(error)
      return 500 unless error.respond_to?(:status_code)

      status = error.status_code
      status = Integer(status, 10) if status.is_a?(String) && status.match?(/\A\d+\z/)
      status.is_a?(Integer) && (400..599).cover?(status) ? status : 500
    end

    def response(status, body, headers = {})
      headers = {
        "Content-Type" => "text/plain; charset=utf-8",
        "Content-Length" => body.bytesize.to_s,
      }.merge(headers)
      [status, headers, [body]]
    end

    def authenticated?(env)
      return true unless @auth

      scheme, encoded = env["HTTP_AUTHORIZATION"].to_s.split(" ", 2)
      return false unless scheme&.casecmp?("Basic") && encoded

      user, password = encoded.unpack1("m0").split(":", 2)
      return false unless user && password

      File.foreach(@auth) do |line|
        stored_user, password_hash = line.chomp.split(":", 2)
        next unless stored_user == user && password_hash

        return secure_compare(password.crypt(password_hash), password_hash)
      end
      false
    rescue ArgumentError, Errno::ENOENT
      false
    end

    def secure_compare(left, right)
      return false unless left.bytesize == right.bytesize

      result = 0
      left.bytes.zip(right.bytes) { |left_byte, right_byte| result |= left_byte ^ right_byte }
      result == 0
    end

    def unauthorized_response
      realm = @realm.to_s.gsub(/["\\]/) { |character| "\\#{character}" }
      response(401, "Unauthorized", "WWW-Authenticate" => %(Basic realm="#{realm}"))
    end

    def log_request(env)
      return unless @verbose

      @logger.info %(#{env["REMOTE_ADDR"]} "#{env["REQUEST_METHOD"]} #{env["PATH_INFO"]} #{env["SERVER_PROTOCOL"]}")
    end
  end
end
