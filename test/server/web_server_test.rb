# frozen_string_literal: true

require_relative "../test_helper"
require "prometheus_exporter/server"
require "prometheus_exporter/client"
require "base64"
require "English"
require "net/http"
require "openssl"
require "socket"
require "tempfile"

class DemoCollector
  attr_reader :processed

  def initialize
    @gauge = PrometheusExporter::Metric::Gauge.new "memory", "amount of memory"
    @processed = Queue.new
  end

  def process(str)
    @processed << str
    obj = JSON.parse(str)
    @gauge.observe(obj["value"]) if obj["type"] == "mem metric"
  end

  def prometheus_metrics_text
    @gauge.to_prometheus_text
  end
end

class SlowCollector < DemoCollector
  def prometheus_metrics_text
    sleep 0.2
    super
  end
end

class SlowProcessCollector < DemoCollector
  attr_reader :started, :release

  def initialize
    super
    @started = Queue.new
    @release = Queue.new
  end

  def process(str)
    @started << true
    @release.pop
    super
  end
end

class BlockingCollector < DemoCollector
  attr_reader :started

  def initialize
    super
    @started = Queue.new
  end

  def process(_str)
    @started << true
    sleep 30
  end
end

class CollectorStatusError < StandardError
  attr_reader :status_code

  def initialize(status_code, message)
    @status_code = status_code
    super(message)
  end
end

class ErrorCollector < DemoCollector
  def initialize(status_code, message = "invalid metrics")
    super()
    @status_code = status_code
    @message = message
  end

  def process(_str)
    raise CollectorStatusError.new(@status_code, @message)
  end
end

class PrometheusExporterWebServerTest < Minitest::Test
  def setup
    PrometheusExporter::Metric::Base.default_prefix = ""
    @servers = []
    @clients = []
    @files = []
  end

  def teardown
    @clients.each do |client|
      client.stop
    rescue StandardError
      nil
    end
    @servers.reverse_each do |server|
      server.stop
    rescue StandardError
      nil
    end
    @files.each(&:close!)
  end

  def test_client_chunked_uploads_are_processed
    assert PrometheusExporter.has_oj?
    assert_equal 100, PrometheusExporter::Server::WebServer::DEFAULT_MAX_CONNECTIONS
    assert_operator(
      PrometheusExporter::Server::WebServer::DEFAULT_BODY_READ_TIMEOUT,
      :>,
      PrometheusExporter::Client::MAX_SOCKET_AGE,
    )

    collector = DemoCollector.new
    server, port = start_server(collector: collector)
    clients =
      %i[oj json].map do |serializer|
        PrometheusExporter::Client.new(
          host: "127.0.0.1",
          port: port,
          thread_sleep: 0.001,
          json_serializer: serializer,
        )
      end
    clients << PrometheusExporter::Client.new(host: "127.0.0.1", port: port, thread_sleep: 0.001)
    @clients.concat(clients)

    clients.each_with_index do |client, index|
      client.send_json "type" => "mem metric", "value" => 150 + index
    end

    assert TestHelper.wait_for(2) { collector.processed.size == 3 }
    assert_equal 3, collector.processed.size
    assert_match(/memory 15[0-2]/, collector.prometheus_metrics_text)
    assert_instance_of PrometheusExporter::Server::WebServer, server
  end

  def test_bundled_client_reuses_request_after_short_header_deadline
    collector = DemoCollector.new
    _server, port = start_server(collector: collector, header_timeout: 0.1, body_read_timeout: 1)
    client = PrometheusExporter::Client.new(host: "127.0.0.1", port: port, thread_sleep: 0.001)
    @clients << client

    client.send_json "type" => "mem metric", "value" => 1
    assert_equal 1, JSON.parse(queue_pop(collector.processed))["value"]
    first_socket = client.instance_variable_get(:@socket)

    sleep 0.2
    assert_operator 0.2, :>, 0.1
    assert_operator 0.2, :<, PrometheusExporter::Client::MAX_SOCKET_AGE
    client.send_json "type" => "mem metric", "value" => 2

    assert_equal 2, JSON.parse(queue_pop(collector.processed))["value"]
    assert_same first_socket, client.instance_variable_get(:@socket)
  end

  def test_standard_client_metrics_keep_their_aggregation_behavior
    server, port = start_server
    client = PrometheusExporter::Client.new(host: "127.0.0.1", port: port, thread_sleep: 0.001)
    @clients << client
    gauge = client.register(:gauge, "my_gauge", "some gauge")
    counter = client.register(:counter, "my_counter", "some counter")

    gauge.observe(2, abcd: 1)
    counter.observe(1)
    counter.observe(3)
    gauge.observe(92, abcd: 1)

    expected = <<~TEXT
      # HELP my_gauge some gauge
      # TYPE my_gauge gauge
      my_gauge{abcd="1"} 92

      # HELP my_counter some counter
      # TYPE my_counter counter
      my_counter 4
    TEXT
    assert TestHelper.wait_for(2) { server.collector.prometheus_metrics_text == expected }
    assert_equal expected, server.collector.prometheus_metrics_text
  end

  def test_processes_each_http_chunk_before_the_upload_finishes
    collector = DemoCollector.new
    _server, port = start_server(collector: collector)
    socket = TCPSocket.new("127.0.0.1", port)
    socket.write(
      "POST /send-metrics HTTP/1.1\r\n" \
        "Host: localhost\r\n" \
        "Transfer-Encoding: chunked\r\n\r\n",
    )

    first = JSON.dump("type" => "mem metric", "value" => 1)
    socket.write("#{first.bytesize.to_s(16)}\r\n#{first}\r\n")
    assert_equal first, queue_pop(collector.processed)

    second = JSON.dump("type" => "mem metric", "value" => 2)
    socket.write("#{second.bytesize.to_s(16)}\r\n#{second}\r\n")
    assert_equal second, queue_pop(collector.processed)

    socket.write("0\r\n\r\n")
    status, headers, body = read_response(socket)
    assert_equal 200, status
    assert_equal "OK", body
    assert_fixed_response(headers, body)
  ensure
    socket&.close
  end

  def test_ping_and_metrics_responses_have_fixed_lengths_and_close
    collector = DemoCollector.new
    collector.process(JSON.dump("type" => "mem metric", "value" => 42))
    _server, port = start_server(collector: collector)

    status, headers, body = raw_request(port, "GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n")
    assert_equal 200, status
    assert_equal "PONG", body
    assert_fixed_response(headers, body)

    status, headers, body =
      raw_request(
        port,
        "GET /metrics HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: identity\r\n\r\n",
      )
    assert_equal 200, status
    assert_includes body, "memory 42"
    assert_fixed_response(headers, body)
  end

  def test_net_http_interoperability_and_exact_not_found_response
    collector = DemoCollector.new
    collector.process(JSON.dump("type" => "mem metric", "value" => 43))
    _server, port = start_server(collector: collector)

    Net::HTTP.start("127.0.0.1", port) do |http|
      ping = http.get("/ping")
      assert_equal "200", ping.code
      assert_equal "PONG", ping.body

      metrics = http.get("/metrics", "Accept-Encoding" => "identity")
      assert_equal "200", metrics.code
      assert_includes metrics.body, "memory 43"

      missing = http.get("/missing")
      assert_equal "404", missing.code
      assert_equal(
        "Not Found! The Prometheus Ruby Exporter only listens on /ping, /metrics and /send-metrics",
        missing.body,
      )
    end
  end

  def test_metrics_gzip_negotiation_and_vary_header
    collector = DemoCollector.new
    collector.process(JSON.dump("type" => "mem metric", "value" => 99))
    _server, port = start_server(collector: collector)

    {
      "br, gzip" => true,
      "*" => true,
      "gzip;q=0, *;q=1" => false,
      "gzip;q=0.5, *;q=0" => true,
      "GZip ; Q = 1.000" => true,
      "gzip;q=0.000" => false,
      "gzip;q=1.001" => false,
      "gzip;q=.5" => false,
      "gzip;q=0.0000" => false,
      "br, *;q=0" => false,
    }.each do |accept_encoding, compressed|
      status, headers, body =
        raw_request(
          port,
          "GET /metrics?source=test HTTP/1.1\r\nHost: localhost\r\n" \
            "Accept-Encoding: #{accept_encoding}\r\n\r\n",
        )

      assert_equal 200, status
      assert_equal "Accept-Encoding", headers["vary"]
      if compressed
        assert_equal "gzip", headers["content-encoding"], accept_encoding
        assert_includes Zlib::GzipReader.new(StringIO.new(body)).read, "memory 99"
      else
        refute headers.key?("content-encoding"), accept_encoding
        assert_includes body, "memory 99"
      end
      assert_fixed_response(headers, body)
    end
  end

  def test_metrics_collection_timeout_does_not_hang_the_endpoint
    _server, port = start_server(collector: SlowCollector.new, timeout: 0.02)

    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    status, _headers, body = raw_request(port, "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

    assert_equal 200, status
    assert_operator elapsed, :<, 0.15
    assert_includes body, "collector_working 0"
  end

  def test_basic_auth_uses_htpasswd_crypt_format
    auth_file = tempfile
    password_hash = "test_password".crypt("xy")
    auth_file.write("test_user:#{password_hash}\n")
    auth_file.flush
    _server, port = start_server(auth: auth_file.path, realm: "Metrics Realm")

    status, headers, body = raw_request(port, "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
    assert_equal 401, status
    assert_equal "Unauthorized", body
    assert_equal 'Basic realm="Metrics Realm"', headers["www-authenticate"]

    credentials = Base64.strict_encode64("test_user:test_password")
    status, _headers, body =
      raw_request(
        port,
        "GET /metrics HTTP/1.1\r\nHost: localhost\r\nAuthorization: Basic #{credentials}\r\n\r\n",
      )
    assert_equal 200, status
    assert_includes body, "collector_working"

    bad_credentials = Base64.strict_encode64("test_user:wrong")
    status, =
      raw_request(
        port,
        "GET /metrics HTTP/1.1\r\nHost: localhost\r\nAuthorization: Basic #{bad_credentials}\r\n\r\n",
      )
    assert_equal 401, status

    status, = raw_request(port, "GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n")
    assert_equal 200, status
  end

  def test_auth_realm_cannot_inject_response_headers
    auth_file = tempfile
    auth_file.write("test_user:#{"password".crypt("xy")}\n")
    auth_file.flush
    realm = "safe\"\r\nX-Injected: yes\\tail"
    _server, port = start_server(auth: auth_file.path, realm: realm)

    status, headers, = raw_request(port, "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")

    assert_equal 401, status
    assert_equal 'Basic realm="safe\\"X-Injected: yes\\\\tail"', headers["www-authenticate"]
    refute headers.key?("x-injected")
  end

  def test_serves_clients_concurrently
    _server, port = start_server(header_timeout: 1)
    stalled = TCPSocket.new("127.0.0.1", port)
    stalled.write("GET /ping HTTP/1.1\r\nHost: stalled")

    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    status, _headers, body = raw_request(port, "GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n")
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

    assert_equal 200, status
    assert_equal "PONG", body
    assert_operator elapsed, :<, 0.5
  ensure
    stalled&.close
  end

  def test_stop_unblocks_accept_and_socket_workers
    server, port = start_server(header_timeout: 30)
    stalled = TCPSocket.new("127.0.0.1", port)
    stalled.write("GET /ping HTTP/1.1\r\n")
    sleep 0.02

    stopper = Thread.new { server.stop }
    assert stopper.join(1), "server.stop did not terminate its accept and worker threads"
    assert_equal "", stalled.read
    assert_raises(Errno::ECONNREFUSED, Errno::ECONNRESET) { TCPSocket.new("127.0.0.1", port) }
  ensure
    stalled&.close
  end

  def test_stop_remains_prompt_when_collector_code_is_blocked
    collector = BlockingCollector.new
    server, port = start_server(collector: collector, stop_timeout: 0.03)
    socket = TCPSocket.new("127.0.0.1", port)
    socket.write("POST /send-metrics HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\n{}")
    queue_pop(collector.started)

    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    server.stop
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

    assert_operator elapsed, :<, 0.5
    assert_equal "", socket.read
  ensure
    socket&.close
  end

  def test_worker_cap_leaves_connections_in_the_kernel_backlog_until_capacity_returns
    server, port = start_server(max_connections: 3, header_timeout: 2, body_read_timeout: 2)
    stalled = 3.times.map { TCPSocket.new("127.0.0.1", port) }
    stalled.each { |socket| socket.write("GET /ping HTTP/1.1\r\nHost: stalled") }
    assert TestHelper.wait_for(1) { worker_count(server) == 3 }

    queued = TCPSocket.new("127.0.0.1", port)
    queued.write("GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n")
    refute IO.select([queued], nil, nil, 0.2), "queued connection was accepted or reset at capacity"
    assert_equal 3, worker_count(server)

    stalled.pop.close
    status, _headers, body = read_response(queued)
    assert_equal 200, status
    assert_equal "PONG", body
    assert_operator worker_count(server), :<=, 3
  ensure
    queued&.close
    stalled&.each { |socket| socket.close unless socket.closed? }
  end

  def test_accept_loop_continues_after_an_aborted_connection
    port = free_port
    server = PrometheusExporter::Server::WebServer.new(port: port, bind: "127.0.0.1")
    @servers << server
    original_accept = server.method(:accept_socket)
    attempts = 0
    server.define_singleton_method(:accept_socket) do |listener|
      attempts += 1
      raise Errno::ECONNABORTED if attempts == 1

      original_accept.call(listener)
    end
    server.start

    status, _headers, body = raw_request(port, "GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n")
    assert_equal 200, status
    assert_equal "PONG", body
    assert_operator attempts, :>=, 2
    assert_nil server.accept_error
  end

  def test_accept_loop_retries_temporary_file_descriptor_exhaustion
    port = free_port
    server = PrometheusExporter::Server::WebServer.new(port: port, bind: "127.0.0.1")
    @servers << server
    original_accept = server.method(:accept_socket)
    attempts = 0
    server.define_singleton_method(:accept_socket) do |listener|
      attempts += 1
      raise Errno::EMFILE if attempts <= 2

      original_accept.call(listener)
    end
    server.start

    status, _headers, body = raw_request(port, "GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n")
    assert_equal 200, status
    assert_equal "PONG", body
    assert_operator attempts, :>=, 3
    assert_nil server.accept_error
  end

  def test_fatal_accept_failure_is_observable_and_leaves_a_stoppable_server
    log = StringIO.new
    port = free_port
    server =
      PrometheusExporter::Server::WebServer.new(port: port, bind: "127.0.0.1", log_target: log)
    @servers << server
    server.define_singleton_method(:accept_socket) { |_listener| raise Errno::EIO }
    server.start
    probe = TCPSocket.new("127.0.0.1", port)

    assert TestHelper.wait_for(1) { server.accept_error }
    assert_instance_of Errno::EIO, server.accept_error
    assert_match(/Failed to run prometheus collector web/, log.string)
    runner =
      server
        .instance_variable_get(:@state_mutex)
        .synchronize { server.instance_variable_get(:@runner) }
    assert_nil runner
    server.stop
  ensure
    probe&.close
  end

  def test_header_and_single_chunk_read_deadlines
    _server, port = start_server(header_timeout: 0.2, body_read_timeout: 0.2)
    socket = TCPSocket.new("127.0.0.1", port)
    socket.write("GET /ping HTTP/1.1\r\nHost:")
    status, headers, body = read_response(socket)

    assert_equal 408, status
    assert_fixed_response(headers, body)

    socket.close
    socket = TCPSocket.new("127.0.0.1", port)
    socket.write(
      "POST /send-metrics HTTP/1.1\r\nHost: localhost\r\n" \
        "Transfer-Encoding: chunked\r\n\r\n10\r\n{}",
    )
    status, = read_response(socket)
    assert_equal 408, status
  ensure
    socket&.close
  end

  def test_each_chunk_gets_a_fresh_body_read_deadline
    collector = DemoCollector.new
    _server, port = start_server(collector: collector, body_read_timeout: 0.5)
    socket = TCPSocket.new("127.0.0.1", port)
    socket.write(
      "POST /send-metrics HTTP/1.1\r\nHost: localhost\r\n" \
        "Transfer-Encoding: chunked\r\n\r\n",
    )
    payload = JSON.dump("type" => "mem metric", "value" => 1)
    socket.write("#{payload.bytesize.to_s(16)}\r\n#{payload}\r\n")
    assert_equal payload, queue_pop(collector.processed)

    sleep 0.3
    socket.write("#{payload.bytesize.to_s(16)}\r\n#{payload}\r\n")
    assert_equal payload, queue_pop(collector.processed)

    sleep 0.3
    socket.write("0\r\n\r\n")
    status, = read_response(socket)
    assert_equal 200, status
  ensure
    socket&.close
  end

  def test_collector_latency_does_not_expire_buffered_complete_chunks
    collector = SlowProcessCollector.new
    _server, port = start_server(collector: collector, body_read_timeout: 0.2)
    payload = JSON.dump("type" => "mem metric", "value" => 1)
    socket = TCPSocket.new("127.0.0.1", port)
    socket.write(
      "POST /send-metrics HTTP/1.1\r\nHost: localhost\r\n" \
        "Transfer-Encoding: chunked\r\n\r\n" \
        "#{payload.bytesize.to_s(16)}\r\n#{payload}\r\n" \
        "#{payload.bytesize.to_s(16)}\r\n#{payload}\r\n0\r\n\r\n",
    )
    queue_pop(collector.started)
    sleep 0.4
    collector.release << true
    queue_pop(collector.started)
    collector.release << true

    status, = read_response(socket)
    assert_equal 200, status
    assert_equal 2, collector.processed.size
  ensure
    collector&.release&.push(true)
    socket&.close
  end

  def test_response_writes_are_nonblocking_and_bounded
    server, = start_server(write_timeout: 0.03)
    writer, reader = UNIXSocket.pair
    writer.setsockopt(Socket::SOL_SOCKET, Socket::SO_SNDBUF, 1024)

    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    server.send(:write_response, writer, 200, "x" * (2 * 1024 * 1024), {})
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

    assert_operator elapsed, :<, 1
  ensure
    writer&.close
    reader&.close
  end

  def test_rejects_oversized_headers_and_body_chunks
    _server, port = start_server(max_header_bytes: 96, max_body_chunk_bytes: 8)

    status, =
      raw_request(port, "GET /ping HTTP/1.1\r\nHost: localhost\r\nX-Large: #{"x" * 100}\r\n\r\n")
    assert_equal 431, status

    status, =
      raw_request(
        port,
        "POST /send-metrics HTTP/1.1\r\nHost: localhost\r\n" \
          "Transfer-Encoding: chunked\r\n\r\n9\r\n",
      )
    assert_equal 413, status
  end

  def test_validates_byte_limits_as_positive_integers
    %i[max_header_bytes max_body_chunk_bytes].each do |option|
      [0, -1, 1.5, "10", false, nil].each do |value|
        options = { :port => free_port, :bind => "127.0.0.1", option => value }
        error = assert_raises(ArgumentError) { PrometheusExporter::Server::WebServer.new(options) }
        assert_match(/#{option} must be a positive integer/, error.message)
      end
    end
  end

  def test_rejects_malformed_or_ambiguous_requests
    _server, port = start_server
    requests = {
      400 => [
        "GET /ping HTTP/1.1\nHost: localhost\n\n",
        "GET http://localhost/ping HTTP/1.1\r\nHost: localhost\r\n\r\n",
        "GET /ping HTTP/1.1\r\n\r\n",
        "GET /ping HTTP/1.1\r\nHost: one\r\nHost: two\r\n\r\n",
        "GET /ping HTTP/1.1\r\nHost: invalid host\r\n\r\n",
        "POST /send-metrics HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n",
        "POST /send-metrics HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nTransfer-Encoding: chunked\r\n\r\n",
        "POST /send-metrics HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: gzip, chunked\r\n\r\n",
        "POST /send-metrics HTTP/1.0\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
        "POST /send-metrics HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n1;x=y\r\na\r\n0\r\n\r\n",
        "POST /send-metrics HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nX-Trailer: no\r\n\r\n",
      ],
      405 => ["POST /ping HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n"],
      411 => ["POST /send-metrics HTTP/1.1\r\nHost: localhost\r\n\r\n"],
      417 => ["GET /ping HTTP/1.1\r\nHost: localhost\r\nExpect: 100-continue\r\n\r\n"],
      505 => ["GET /ping HTTP/2.0\r\nHost: localhost\r\n\r\n"],
    }

    requests.each do |expected_status, raw_requests|
      raw_requests.each do |request|
        status, headers, body = raw_request(port, request)
        assert_equal expected_status, status, request.inspect
        assert_fixed_response(headers, body)
      end
    end

    status, headers, =
      raw_request(port, "POST /ping HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n")
    assert_equal 405, status
    assert_equal "GET", headers["allow"]
  end

  def test_supports_a_bounded_content_length_upload
    collector = DemoCollector.new
    _server, port = start_server(collector: collector)
    json = JSON.dump("type" => "mem metric", "value" => 7)

    status, _headers, body =
      raw_request(
        port,
        "POST /send-metrics HTTP/1.1\r\nHost: localhost\r\nContent-Length: #{json.bytesize}\r\n\r\n#{json}",
      )

    assert_equal 200, status
    assert_equal "OK", body
    assert_equal json, queue_pop(collector.processed)
  end

  def test_collector_errors_return_an_error_and_are_counted
    _server, port = start_server(collector: DemoCollector.new)
    invalid_json = "not-json"
    request =
      "POST /send-metrics HTTP/1.1\r\nHost: localhost\r\n" \
        "Transfer-Encoding: chunked\r\n\r\n" \
        "#{invalid_json.bytesize.to_s(16)}\r\n#{invalid_json}\r\n0\r\n\r\n"

    status, _headers, body = raw_request(port, request)
    assert_equal 500, status
    assert_includes body, "Bad Metrics"

    _status, _headers, metrics =
      raw_request(port, "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
    assert_includes metrics, "collector_metrics_total 1"
    assert_includes metrics, "collector_sessions_total 1"
    assert_includes metrics, "collector_bad_metrics_total 1"
  end

  def test_collector_exception_status_is_validated_and_cannot_inject_response
    cases = [
      [422, 422, "Unprocessable Entity"],
      [499, 499, "Error"],
      [599, 599, "Error"],
      [399, 500, "Internal Server Error"],
      [600, 500, "Internal Server Error"],
      [422.0, 500, "Internal Server Error"],
      ["422\r\nX-Injected: yes", 500, "Internal Server Error"],
    ]

    cases.each do |status_code, expected, expected_reason|
      collector = ErrorCollector.new(status_code, "bad\r\nX-Body: body-only")
      _server, port = start_server(collector: collector)
      request =
        "POST /send-metrics HTTP/1.1\r\nHost: localhost\r\n" \
          "Content-Length: 2\r\n\r\n{}"

      status, headers, body, reason = raw_request(port, request)
      assert_equal expected, status
      assert_equal expected_reason, reason
      refute headers.key?("x-injected")
      refute headers.key?("x-body")
      assert_includes body, "X-Body: body-only"
    end
  end

  def test_tls
    cert_file, key_file = tls_files
    _server, port = start_server(tls_cert_file: cert_file.path, tls_key_file: key_file.path)
    tcp_socket = TCPSocket.new("127.0.0.1", port)
    context = OpenSSL::SSL::SSLContext.new
    context.verify_mode = OpenSSL::SSL::VERIFY_NONE
    socket = OpenSSL::SSL::SSLSocket.new(tcp_socket, context)
    socket.sync_close = true
    socket.connect
    socket.write("GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n")

    status, headers, body = read_response(socket)
    assert_equal 200, status
    assert_equal "PONG", body
    assert_fixed_response(headers, body)
  ensure
    socket&.close
    tcp_socket&.close
  end

  def test_tls_configuration_requires_certificate_and_key_together
    cert_file, key_file = tls_files

    error =
      assert_raises(ArgumentError) do
        PrometheusExporter::Server::WebServer.new(
          port: free_port,
          bind: "127.0.0.1",
          tls_cert_file: cert_file.path,
        )
      end
    assert_match(/supplied together/, error.message)

    assert_raises(ArgumentError) do
      PrometheusExporter::Server::WebServer.new(
        port: free_port,
        bind: "127.0.0.1",
        tls_key_file: key_file.path,
      )
    end
  end

  def test_tls_configuration_rejects_a_mismatched_private_key
    cert_file, = tls_files
    different_key = OpenSSL::PKey::RSA.new(2048)
    key_file = tempfile
    key_file.write(different_key.to_pem)
    key_file.flush

    error =
      assert_raises(ArgumentError) do
        PrometheusExporter::Server::WebServer.new(
          port: free_port,
          bind: "127.0.0.1",
          tls_cert_file: cert_file.path,
          tls_key_file: key_file.path,
        )
      end
    assert_match(/do not match/, error.message)
  end

  def test_plain_http_does_not_load_openssl_even_on_disconnect_rescue_paths
    script = <<~'RUBY'
      require "prometheus_exporter"
      require "prometheus_exporter/server"
      abort "OpenSSL loaded during require" if defined?(OpenSSL)

      server = PrometheusExporter::Server::WebServer.new(port: 0, bind: "127.0.0.1")
      port = server.instance_variable_get(:@listeners).first.local_address.ip_port
      server.start
      disconnected = TCPSocket.new("127.0.0.1", port)
      disconnected.write("GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n")
      disconnected.close
      socket = TCPSocket.new("127.0.0.1", port)
      socket.write("GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n")
      abort "plain HTTP request failed" unless socket.read.include?("200 OK")
      socket.close
      failing_socket = Object.new
      failing_socket.define_singleton_method(:closed?) { false }
      failing_socket.define_singleton_method(:write_nonblock) { |*| raise "write failure" }
      begin
        server.send(:write_response, failing_socket, 200, "body", {})
      rescue RuntimeError => error
        raise unless error.message == "write failure"
      end
      abort "OpenSSL loaded by rescue handling" if defined?(OpenSSL)
      server.stop
      abort "OpenSSL loaded by plain HTTP" if defined?(OpenSSL)
    RUBY
    output = IO.popen([RbConfig.ruby, "-Ilib", "-e", script], err: %i[child out], &:read)

    assert_predicate $CHILD_STATUS, :success?, output
  end

  def test_bundled_tls_client_uploads_payload_larger_than_tls_record
    collector = DemoCollector.new
    cert_file, key_file = tls_files
    _server, port =
      start_server(collector: collector, tls_cert_file: cert_file.path, tls_key_file: key_file.path)
    client =
      PrometheusExporter::Client.new(
        host: "localhost",
        port: port,
        thread_sleep: 0.001,
        tls_ca_file: cert_file.path,
        tls_cert_file: cert_file.path,
        tls_key_file: key_file.path,
      )
    @clients << client
    padding = "x" * (20 * 1024)

    client.send_json "type" => "mem metric", "value" => 77, "padding" => padding

    payload = JSON.parse(queue_pop(collector.processed, timeout: 2))
    assert_equal 77, payload["value"]
    assert_equal padding, payload["padding"]
  end

  def test_ipv6
    skip "IPv6 loopback is unavailable" unless ipv6_available?

    port = free_port("::1")
    server = PrometheusExporter::Server::WebServer.new(port: port, bind: "::1")
    @servers << server
    server.start

    status, _headers, body =
      raw_request(port, "GET /ping HTTP/1.1\r\nHost: [::1]\r\n\r\n", host: "::1")
    assert_equal 200, status
    assert_equal "PONG", body
  end

  def test_bundled_client_uploads_over_ipv6
    skip "IPv6 loopback is unavailable" unless ipv6_available?

    collector = DemoCollector.new
    _server, port = start_server(bind: "::1", collector: collector)
    client = PrometheusExporter::Client.new(host: "::1", port: port, thread_sleep: 0.001)
    @clients << client

    client.send_json "type" => "mem metric", "value" => 99

    assert_equal 99, JSON.parse(queue_pop(collector.processed))["value"]
    assert_includes collector.prometheus_metrics_text, "memory 99"
  end

  def test_any_and_all_bind_ipv4_and_ipv6_as_available
    %w[ANY ALL].each do |bind|
      port = free_port
      server = PrometheusExporter::Server::WebServer.new(port: port, bind: bind)
      @servers << server
      server.start

      status, = raw_request(port, "GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n")
      assert_equal 200, status

      if ipv6_available?
        status, = raw_request(port, "GET /ping HTTP/1.1\r\nHost: [::1]\r\n\r\n", host: "::1")
        assert_equal 200, status
      end
    end
  end

  private

  def start_server(**options)
    port = options.delete(:port) || free_port(options[:bind] || "127.0.0.1")
    options[:bind] ||= "127.0.0.1"
    server = PrometheusExporter::Server::WebServer.new(options.merge(port: port))
    @servers << server
    server.start
    [server, port]
  end

  def free_port(host = "127.0.0.1")
    server = TCPServer.new(host, 0)
    server.local_address.ip_port
  ensure
    server&.close
  end

  def raw_request(port, request, host: "127.0.0.1")
    socket = TCPSocket.new(host, port)
    socket.write(request)
    read_response(socket)
  ensure
    socket&.close
  end

  def read_response(socket)
    raw = socket.read
    header_text, body = raw.split("\r\n\r\n", 2)
    refute_nil body, "response had no header terminator: #{raw.inspect}"
    lines = header_text.split("\r\n")
    status_line = lines.shift
    status_match = /\AHTTP\/1\.1 (\d{3}) (.*)\z/.match(status_line)
    refute_nil status_match, "malformed response status line: #{status_line.inspect}"
    status = status_match[1].to_i
    reason = status_match[2]
    headers =
      lines.to_h do |line|
        name, value = line.split(":", 2)
        [name.downcase, value.strip]
      end
    [status, headers, body, reason]
  end

  def assert_fixed_response(headers, body)
    assert_equal body.bytesize.to_s, headers["content-length"]
    assert_equal "close", headers["connection"]
    refute headers.key?("transfer-encoding")
  end

  def worker_count(server)
    server
      .instance_variable_get(:@state_mutex)
      .synchronize { server.instance_variable_get(:@workers).length }
  end

  def queue_pop(queue, timeout: 1)
    Timeout.timeout(timeout) { queue.pop }
  end

  def tempfile
    file = Tempfile.new("prometheus-exporter-test")
    @files << file
    file
  end

  def tls_files
    key = OpenSSL::PKey::RSA.new(2048)
    certificate = OpenSSL::X509::Certificate.new
    certificate.version = 2
    certificate.serial = 1
    certificate.subject = OpenSSL::X509::Name.parse("/CN=localhost")
    certificate.issuer = certificate.subject
    certificate.public_key = key.public_key
    certificate.not_before = Time.now - 60
    certificate.not_after = Time.now + 3600
    certificate.sign(key, OpenSSL::Digest.new("SHA256"))

    cert_file = tempfile
    cert_file.write(certificate.to_pem)
    cert_file.flush
    key_file = tempfile
    key_file.write(key.to_pem)
    key_file.flush
    [cert_file, key_file]
  end

  def ipv6_available?
    server = TCPServer.new("::1", 0)
    true
  rescue SocketError, SystemCallError
    false
  ensure
    server&.close
  end
end
