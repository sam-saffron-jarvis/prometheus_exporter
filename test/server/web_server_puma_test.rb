# frozen_string_literal: true

require_relative "../test_helper"
require "fileutils"
require "net/http"
require "openssl"
require "prometheus_exporter/client"
require "prometheus_exporter/server"
require "socket"
require "tmpdir"

class PrometheusExporterPumaWebServerTest < Minitest::Test
  class RecordingCollector
    attr_reader :payloads

    def initialize(metrics_text: "recorded_metric 1\n", error: nil, delay: nil)
      @metrics_text = metrics_text
      @error = error
      @delay = delay
      @payloads = []
      @mutex = Mutex.new
    end

    def process(payload)
      raise @error if @error

      @mutex.synchronize { @payloads << payload }
    end

    def prometheus_metrics_text
      sleep(@delay) if @delay
      @metrics_text
    end
  end

  def setup
    PrometheusExporter::Metric::Base.default_prefix = ""
  end

  def test_puma_buffers_a_finite_body_and_delivers_one_opaque_record
    collector = RecordingCollector.new
    with_server(collector: collector) do |_server, port|
      socket = TCPSocket.new("127.0.0.1", port)
      socket.write(
        "POST /send-metrics HTTP/1.1\r\nHost: localhost\r\nContent-Length: 11\r\n\r\nopaque",
      )

      sleep(0.05)
      assert_empty(collector.payloads, "Puma must not call the app with a partial body")

      socket.write("\nbody")
      status, = read_response(socket)
      assert_equal(200, status)
      assert_equal(["opaque\nbody"], collector.payloads)
    ensure
      socket&.close
    end
  end

  def test_legacy_chunked_stream_is_processed_after_it_finishes
    collector = RecordingCollector.new
    with_server(collector: collector) do |_server, port|
      socket = TCPSocket.new("127.0.0.1", port)
      socket.write(
        "POST /send-metrics HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n6\r\nlegacy\r\n",
      )

      assert_nil(
        IO.select([socket], nil, nil, 0.1),
        "Puma buffers until the streaming request ends",
      )
      assert_empty(collector.payloads)

      socket.write("0\r\n\r\n")
      status, _headers, body = read_response(socket)
      assert_equal(200, status)
      assert_equal("OK", body)
      assert_equal(["legacy"], collector.payloads)
    ensure
      socket&.close
    end
  end

  def test_legacy_chunked_stream_recovers_multiple_json_metrics
    collector = RecordingCollector.new
    first = JSON.generate(name: "one", value: "a}b")
    second = JSON.generate(name: "two", keys: { quote: '"' })

    with_server(collector: collector) do |server, port|
      socket = TCPSocket.new("127.0.0.1", port)
      socket.write(
        "POST /send-metrics HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n" \
          "#{first.bytesize.to_s(16)}\r\n#{first}\r\n" \
          "#{second.bytesize.to_s(16)}\r\n#{second}\r\n0\r\n\r\n",
      )

      status, = read_response(socket)
      assert_equal(200, status)
      assert_equal([first, second], collector.payloads)
      assert_match(/collector_metrics_total 2/, server.metrics)
      assert_match(/collector_sessions_total 1/, server.metrics)
    ensure
      socket&.close
    end
  end

  def test_finite_request_accepts_multiple_adjacent_json_metrics
    collector = RecordingCollector.new
    first = JSON.generate(name: "one")
    second = JSON.generate(name: "two")

    with_server(collector: collector) do |server, port|
      response = post(port, first + second)

      assert_equal("200", response.code)
      assert_equal([first, second], collector.payloads)
      assert_match(/collector_metrics_total 2/, server.metrics)
      assert_match(/collector_sessions_total 1/, server.metrics)
    end
  end

  def test_non_json_object_stream_remains_one_opaque_payload
    collector = RecordingCollector.new
    payload = "{]{}"

    with_server(collector: collector) do |_server, port|
      response = post(port, payload)

      assert_equal("200", response.code)
      assert_equal([payload], collector.payloads)
    end
  end

  def test_send_metrics_requires_post
    collector = RecordingCollector.new
    with_server(collector: collector) do |_server, port|
      response = Net::HTTP.get_response("127.0.0.1", "/send-metrics", port)
      assert_equal("405", response.code)
      assert_equal("POST", response["Allow"])
      assert_empty(collector.payloads)
    end
  end

  def test_collector_errors_return_their_status_and_increment_bad_metrics
    error = Class.new(StandardError) { def status_code = 422 }.new("invalid opaque record")
    collector = RecordingCollector.new(error: error)
    with_server(collector: collector) do |server, port|
      response = post(port, "bad")
      assert_equal("422", response.code)
      assert_match(/Bad Metrics invalid opaque record/, response.body)
      metrics = server.metrics
      assert_match(/collector_metrics_total 1/, metrics)
      assert_match(/collector_sessions_total 1/, metrics)
      assert_match(/collector_bad_metrics_total 1/, metrics)
    end
  end

  def test_metrics_timeout_and_self_metrics_are_preserved
    collector = RecordingCollector.new(delay: 0.05)
    server =
      PrometheusExporter::Server::WebServer.new(
        port: free_port,
        bind: "127.0.0.1",
        timeout: 0.001,
        collector: collector,
      )

    output = server.metrics
    assert_match(/collector_working 0/, output)
    assert_match(/collector_metrics_total 0/, output)
    assert_match(/collector_sessions_total 0/, output)
    assert_match(/collector_bad_metrics_total 0/, output)
  ensure
    server&.stop
  end

  def test_start_is_idempotent_and_stop_joins_puma
    server =
      PrometheusExporter::Server::WebServer.new(
        port: free_port,
        bind: "127.0.0.1",
        collector: RecordingCollector.new,
      )
    runner = server.start
    assert_same(runner, server.start)
    assert_predicate(runner, :alive?)

    server.stop
    refute_predicate(runner, :alive?)
  ensure
    server&.stop
  end

  def test_verbose_logging_uses_the_requested_target
    log = StringIO.new
    with_server(
      collector: RecordingCollector.new,
      verbose: true,
      log_target: log,
    ) do |_server, port|
      assert_equal("PONG", Net::HTTP.get("127.0.0.1", "/ping", port))
      assert_match(%r{GET /ping}, log.string)
    end
  end

  def test_any_bind_accepts_ipv4_and_ipv6_when_the_host_supports_it
    collector = RecordingCollector.new
    with_server(collector: collector, bind: "ANY") do |_server, port|
      assert_equal("PONG", Net::HTTP.get("127.0.0.1", "/ping", port))

      if Socket.ip_address_list.any?(&:ipv6_loopback?)
        begin
          assert_equal("PONG", Net::HTTP.get("::1", "/ping", port))
        rescue Errno::EADDRNOTAVAIL, Errno::ECONNREFUSED
          skip "this host exposes IPv6 loopback but cannot bind it"
        end
      end
    end
  end

  def test_server_and_client_tls
    Dir.mktmpdir("prometheus-exporter-tls") do |directory|
      paths = write_tls_chain(directory)
      collector = RecordingCollector.new
      with_server(
        collector: collector,
        tls_cert_file: paths[:server_cert],
        tls_key_file: paths[:server_key],
      ) do |_server, port|
        client =
          PrometheusExporter::Client.new(
            host: "localhost",
            port: port,
            process_queue_once_and_stop: true,
            tls_ca_file: paths[:ca_cert],
            tls_cert_file: paths[:client_cert],
            tls_key_file: paths[:client_key],
          )
        client.send("tls opaque payload")
        assert_equal(["tls opaque payload"], collector.payloads)
      ensure
        client&.stop
      end
    end
  end

  def test_tls_hostname_mismatch_is_rejected
    Dir.mktmpdir("prometheus-exporter-tls") do |directory|
      paths = write_tls_chain(directory)
      collector = RecordingCollector.new
      with_server(
        collector: collector,
        tls_cert_file: paths[:server_cert],
        tls_key_file: paths[:server_key],
      ) do |_server, port|
        logs = StringIO.new
        client =
          PrometheusExporter::Client.new(
            host: "127.0.0.1",
            port: port,
            process_queue_once_and_stop: true,
            logger: Logger.new(logs),
            tls_ca_file: paths[:ca_cert],
            tls_cert_file: paths[:client_cert],
            tls_key_file: paths[:client_key],
          )
        client.send("must not arrive")

        assert_empty(collector.payloads)
        assert_match(/does not match the server certificate/, logs.string)
      ensure
        client&.stop
      end
    end
  end

  def test_partial_server_tls_configuration_fails_fast
    error =
      assert_raises(ArgumentError) do
        PrometheusExporter::Server::WebServer.new(
          port: 0,
          bind: "127.0.0.1",
          collector: RecordingCollector.new,
          tls_cert_file: "cert.pem",
        )
      end

    assert_match(/must be configured together/, error.message)
  end

  def test_oversized_fixed_body_is_rejected_by_puma_parser
    with_server(collector: RecordingCollector.new, max_record_size: 16) do |_server, port|
      socket = TCPSocket.new("127.0.0.1", port)
      socket.write(
        "POST /send-metrics HTTP/1.1\r\nHost: localhost\r\n" \
          "Content-Length: 17\r\n\r\n0123456789abcdefg",
      )

      status, = read_response(socket)
      assert_equal(413, status)
    ensure
      socket&.close
    end
  end

  def test_oversized_chunked_body_is_rejected_by_puma_parser
    with_server(collector: RecordingCollector.new, max_record_size: 16) do |_server, port|
      socket = TCPSocket.new("127.0.0.1", port)
      socket.write(
        "POST /send-metrics HTTP/1.1\r\nHost: localhost\r\n" \
          "Transfer-Encoding: chunked\r\n\r\n" \
          "11\r\n0123456789abcdefg\r\n0\r\n\r\n",
      )

      status, = read_response(socket)
      assert_equal(413, status)
    ensure
      socket&.close
    end
  end

  def test_oversized_unterminated_legacy_stream_gets_413_when_parser_limit_is_crossed
    if Gem::Version.new(Puma::Const::PUMA_VERSION) < Gem::Version.new("8.0.0")
      skip "Puma before 8 enforces the chunked body limit only after the request finishes"
    end

    with_server(collector: RecordingCollector.new, max_record_size: 16) do |_server, port|
      socket = TCPSocket.new("127.0.0.1", port)
      socket.write(
        "POST /send-metrics HTTP/1.1\r\nHost: localhost\r\n" \
          "Transfer-Encoding: chunked\r\n\r\n" \
          "10\r\n0123456789abcdef\r\n1\r\ng\r\n",
      )

      assert(IO.select([socket], nil, nil, 1), "Puma did not reject the open stream promptly")
      status, = read_response(socket)
      assert_equal(413, status)
    ensure
      socket&.close
    end
  end

  def test_application_validates_content_length_and_reads_boundedly
    collector = RecordingCollector.new
    server =
      PrometheusExporter::Server::WebServer.new(
        port: 0,
        bind: "127.0.0.1",
        collector: collector,
        max_record_size: 4,
      )
    input =
      Class
        .new do
          attr_reader :requested

          def read(length)
            @requested = length
            "12345"
          end
        end
        .new
    status, =
      server.call(
        "PATH_INFO" => "/send-metrics",
        "REQUEST_METHOD" => "POST",
        "CONTENT_LENGTH" => "4",
        "rack.input" => input,
      )

    assert_equal(413, status)
    assert_equal(5, input.requested)
    assert_empty(collector.payloads)
  ensure
    server&.stop
  end

  def test_port_zero_uses_one_ephemeral_port_for_any_bind
    server =
      PrometheusExporter::Server::WebServer.new(
        port: 0,
        bind: "ANY",
        collector: RecordingCollector.new,
      )
    server.start

    assert_operator(server.port, :>, 0)
    assert_equal("PONG", Net::HTTP.get("127.0.0.1", "/ping", server.port))
    if Socket.ip_address_list.any?(&:ipv6_loopback?)
      begin
        assert_equal("PONG", Net::HTTP.get("::1", "/ping", server.port))
      rescue Errno::EADDRNOTAVAIL, Errno::ECONNREFUSED
        skip "this host cannot bind both wildcard address families"
      end
    end
  ensure
    server&.stop
  end

  def test_default_localhost_port_zero_is_shared_and_all_listeners_close
    server = PrometheusExporter::Server::WebServer.new(port: 0, collector: RecordingCollector.new)
    server.start
    port = server.port

    assert_equal("PONG", Net::HTTP.get("127.0.0.1", "/ping", port))
    if Socket.ip_address_list.any?(&:ipv6_loopback?)
      assert_equal("PONG", Net::HTTP.get("::1", "/ping", port))
    end
    server.stop

    replacement = TCPServer.new("127.0.0.1", port)
    assert_equal(port, replacement.local_address.ip_port)
  ensure
    server&.stop
    replacement&.close
  end

  def test_port_zero_retries_a_second_family_address_collision
    skip "IPv6 loopback is unavailable" if Socket.ip_address_list.none?(&:ipv6_loopback?)

    adapter_class = PrometheusExporter::Server::WebServer.const_get(:PumaAdapter, false)
    log = StringIO.new
    adapter =
      adapter_class.new(
        ->(_env) { [200, { "Content-Length" => "4" }, ["PONG"]] },
        log_writer: Puma::LogWriter.new(log, log),
        max_record_size: 1024,
        logger: Logger.new(log),
        verbose: false,
      )
    original_add_listener = adapter.method(:add_listener)
    collision_injected = false
    adapter.define_singleton_method(:add_listener) do |host, port, ssl_context|
      if host == "127.0.0.1" && !collision_injected
        collision_injected = true
        raise Errno::EADDRINUSE, "simulated second-family ephemeral collision"
      end

      original_add_listener.call(host, port, ssl_context)
    end

    _runner, port = adapter.start(hosts: %w[::1 127.0.0.1], port: 0)
    assert(collision_injected)
    assert_equal("PONG", Net::HTTP.get("127.0.0.1", "/ping", port))
    assert_equal("PONG", Net::HTTP.get("::1", "/ping", port))
  ensure
    adapter&.stop
  end

  def test_any_and_localhost_binds_fail_on_material_family_conflicts
    %w[ANY localhost].each do |bind|
      blocker = TCPServer.new("127.0.0.1", 0)
      port = blocker.local_address.ip_port
      server =
        PrometheusExporter::Server::WebServer.new(
          port: port,
          bind: bind,
          collector: RecordingCollector.new,
        )

      assert_raises(Errno::EADDRINUSE, "#{bind} silently accepted a partial bind") { server.start }
    ensure
      server&.stop
      blocker&.close
    end
  end

  def test_stop_before_start_closes_owned_log_file
    skip "file-descriptor inspection requires /proc" unless Dir.exist?("/proc/self/fd")

    Dir.mktmpdir("prometheus-exporter-log") do |directory|
      path = File.join(directory, "server.log")
      server =
        PrometheusExporter::Server::WebServer.new(
          port: 0,
          bind: "127.0.0.1",
          collector: RecordingCollector.new,
          log_target: path,
        )
      assert_operator(open_descriptors_for(path), :>, 0)

      server.stop
      assert_equal(0, open_descriptors_for(path))
    end
  end

  def test_start_failure_cleans_listeners_and_allows_retry_until_terminal_stop
    blocker = TCPServer.new("127.0.0.1", 0)
    port = blocker.local_address.ip_port
    Dir.mktmpdir("prometheus-exporter-log") do |directory|
      path = File.join(directory, "server.log")
      server =
        PrometheusExporter::Server::WebServer.new(
          port: port,
          bind: "127.0.0.1",
          collector: RecordingCollector.new,
          log_target: path,
        )

      assert_raises(Errno::EADDRINUSE) { server.start }
      assert_operator(open_descriptors_for(path), :>, 0)

      blocker.close
      blocker = nil
      runner = server.start
      assert_predicate(runner, :alive?)
      assert_equal("PONG", Net::HTTP.get("127.0.0.1", "/ping", port))

      server.stop
      assert_equal(0, open_descriptors_for(path))
      error = assert_raises(RuntimeError) { server.start }
      assert_match(/has been stopped/, error.message)
    ensure
      server&.stop
    end

    replacement = TCPServer.new("127.0.0.1", port)
    assert_equal(port, replacement.local_address.ip_port)
  ensure
    blocker&.close
    replacement&.close
  end

  def test_quiet_server_honors_explicit_log_target
    logs = StringIO.new
    collector = RecordingCollector.new(error: StandardError.new("quiet failure"))
    with_server(collector: collector, verbose: false, log_target: logs) do |_server, port|
      response = post(port, "bad")
      assert_equal("500", response.code)
      assert_match(/quiet failure/, logs.string)
    end
  end

  def test_invalid_collector_status_falls_back_to_500
    [399, 422.5, 600, "not a status"].each do |invalid_status|
      error =
        Class
          .new(StandardError) { define_method(:status_code) { invalid_status } }
          .new("invalid status")
      with_server(collector: RecordingCollector.new(error: error)) do |_server, port|
        assert_equal("500", post(port, "bad").code)
      end
    end
  end

  def test_realm_rejects_header_control_characters
    error =
      assert_raises(ArgumentError) do
        PrometheusExporter::Server::WebServer.new(
          port: 0,
          collector: RecordingCollector.new,
          realm: "unsafe\r\nX-Injected: yes",
        )
      end

    assert_match(/control characters/, error.message)
  end

  def test_gzip_quality_negotiation_and_vary
    with_server(collector: RecordingCollector.new) do |_server, port|
      disabled = get_metrics(port, "gzip;q=0, *;q=1")
      assert_nil(disabled["Content-Encoding"])
      assert_equal("Accept-Encoding", disabled["Vary"])

      wildcard = get_metrics(port, "br;q=0.5, *;q=0.2")
      assert_equal("gzip", wildcard["Content-Encoding"])
      assert_equal("Accept-Encoding", wildcard["Vary"])
    end
  end

  private

  def with_server(collector:, bind: "127.0.0.1", **options)
    port = free_port
    server =
      PrometheusExporter::Server::WebServer.new(
        { port: port, bind: bind, collector: collector }.merge(options),
      )
    server.start
    yield server, port
  ensure
    server&.stop
  end

  def free_port
    socket = TCPServer.new("127.0.0.1", 0)
    socket.local_address.ip_port
  ensure
    socket&.close
  end

  def post(port, body)
    Net::HTTP.start("127.0.0.1", port) do |http|
      request = Net::HTTP::Post.new("/send-metrics")
      request.body = body
      http.request(request)
    end
  end

  def read_response(socket)
    status_line = socket.gets("\n")
    status = status_line.split(" ", 3)[1].to_i
    headers = {}
    while (line = socket.gets("\n")) != "\r\n"
      name, value = line.split(":", 2)
      headers[name.downcase] = value.strip
    end
    body = socket.read(Integer(headers.fetch("content-length"), 10))
    [status, headers, body]
  end

  def get_metrics(port, accept_encoding)
    Net::HTTP.start("127.0.0.1", port) do |http|
      request = Net::HTTP::Get.new("/metrics")
      request["Accept-Encoding"] = accept_encoding
      http.request(request)
    end
  end

  def open_descriptors_for(path)
    Dir
      .glob("/proc/self/fd/*")
      .count do |descriptor|
        File.realpath(descriptor) == File.realpath(path)
      rescue Errno::ENOENT
        false
      end
  end

  def write_tls_chain(directory)
    ca_key = OpenSSL::PKey::RSA.new(2048)
    ca_cert = certificate("Prometheus Exporter Test CA", ca_key, serial: 1, ca: true)
    server_key = OpenSSL::PKey::RSA.new(2048)
    server_cert =
      certificate("localhost", server_key, serial: 2, issuer_cert: ca_cert, issuer_key: ca_key)
    client_key = OpenSSL::PKey::RSA.new(2048)
    client_cert =
      certificate("client", client_key, serial: 3, issuer_cert: ca_cert, issuer_key: ca_key)

    files = {
      ca_cert: ["ca.crt", ca_cert.to_pem],
      server_cert: ["server.crt", server_cert.to_pem],
      server_key: ["server.key", server_key.to_pem],
      client_cert: ["client.crt", client_cert.to_pem],
      client_key: ["client.key", client_key.to_pem],
    }
    files.transform_values do |filename, contents|
      path = File.join(directory, filename)
      File.write(path, contents)
      path
    end
  end

  def certificate(common_name, key, serial:, ca: false, issuer_cert: nil, issuer_key: nil)
    cert = OpenSSL::X509::Certificate.new
    cert.version = 2
    cert.serial = serial
    cert.subject = OpenSSL::X509::Name.parse("/CN=#{common_name}")
    cert.issuer = issuer_cert ? issuer_cert.subject : cert.subject
    cert.public_key = key.public_key
    cert.not_before = Time.now - 60
    cert.not_after = Time.now + 3600

    extensions = OpenSSL::X509::ExtensionFactory.new
    extensions.subject_certificate = cert
    extensions.issuer_certificate = issuer_cert || cert
    cert.add_extension(
      extensions.create_extension("basicConstraints", ca ? "CA:TRUE" : "CA:FALSE", true),
    )
    cert.add_extension(
      extensions.create_extension(
        "keyUsage",
        ca ? "keyCertSign,cRLSign" : "digitalSignature,keyEncipherment",
        true,
      ),
    )
    if !ca && common_name == "localhost"
      cert.add_extension(extensions.create_extension("subjectAltName", "DNS:localhost"))
    end
    cert.sign(issuer_key || key, OpenSSL::Digest.new("SHA256"))
    cert
  end
end
