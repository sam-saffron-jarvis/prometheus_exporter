# frozen_string_literal: true

require_relative "test_helper"
require "openssl"
require "prometheus_exporter/client"
require "socket"
require "tmpdir"

class PrometheusExporterClientWireTest < Minitest::Test
  def test_sends_one_content_length_request_per_payload_and_reuses_connection
    requests = []
    listener, port = build_listener
    server_thread =
      Thread.new do
        socket = listener.accept
        2.times do |index|
          request_line, headers, body = read_request(socket)
          requests << [request_line, headers, body]
          connection = index == 0 ? "Keep-Alive" : "close"
          socket.write(
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: #{connection}\r\n\r\nOK",
          )
        end
        socket.close
      end

    client = synchronous_client(port)
    client.send("first\nopaque\0payload".b)
    client.send('{"second":true}')

    server_thread.join(2)
    assert_equal(2, requests.length)
    assert_equal(["first\nopaque\0payload".b, '{"second":true}'], requests.map(&:last))
    requests.each do |request_line, headers, body|
      assert_equal("POST /send-metrics HTTP/1.1", request_line)
      assert_equal(body.bytesize.to_s, headers["content-length"])
      refute(headers.key?("x-prometheus-exporter-protocol"))
      refute(headers.key?("transfer-encoding"))
    end
  ensure
    cleanup(client, listener, server_thread)
  end

  def test_net_http_handles_informational_and_chunked_responses
    requests = []
    listener, port = build_listener
    server_thread =
      Thread.new do
        socket = listener.accept
        2.times do |index|
          requests << read_request(socket).last
          if index == 0
            socket.write(
              "HTTP/1.1 100 Continue\r\n\r\n" \
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nO\r\n1\r\nK\r\n0\r\n\r\n",
            )
          else
            socket.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")
          end
        end
        socket.close
      end

    client = synchronous_client(port)
    client.send("one")
    client.send("two")

    server_thread.join(2)
    assert_equal(%w[one two], requests)
  ensure
    cleanup(client, listener, server_thread)
  end

  def test_net_http_reconnects_after_connection_close
    requests = []
    listener, port = build_listener
    server_thread =
      Thread.new do
        2.times do
          socket = listener.accept
          requests << read_request(socket).last
          socket.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")
          socket.close
        end
      end

    client = synchronous_client(port)
    client.send("one")
    client.send("two")

    server_thread.join(2)
    assert_equal(%w[one two], requests)
  ensure
    cleanup(client, listener, server_thread)
  end

  def test_stalled_response_is_bounded_by_read_timeout_and_not_retried
    requests = Queue.new
    listener, port = build_listener
    server_thread =
      Thread.new do
        socket = listener.accept
        requests << read_request(socket).last
        sleep 1
      ensure
        socket&.close
      end
    logs = StringIO.new
    started = monotonic_now
    client = synchronous_client(port, read_timeout: 0.05, logger: Logger.new(logs))
    client.send("at-most-once")
    elapsed = monotonic_now - started

    assert_operator(elapsed, :<, 0.5)
    assert_equal("at-most-once", requests.pop)
    assert_match(/Net::ReadTimeout/, logs.string)
    assert_raises(ThreadError) { requests.pop(true) }
  ensure
    cleanup(client, listener, server_thread)
  end

  def test_stop_waits_for_dequeued_unacknowledged_delivery
    accepted = Queue.new
    release = Queue.new
    listener, port = build_listener
    server_thread =
      Thread.new do
        socket = listener.accept
        read_request(socket)
        accepted << true
        release.pop
        socket.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")
      ensure
        socket&.close
      end
    client = PrometheusExporter::Client.new(host: "127.0.0.1", port: port, thread_sleep: 0.001)
    client.send("in flight")
    accepted.pop

    stopper = Thread.new { client.stop(wait_timeout_seconds: 1) }
    sleep 0.05
    assert_predicate(
      stopper,
      :alive?,
      "stop returned while a dequeued delivery awaited its response",
    )
    release << true
    stopper.join(1)
    refute_predicate(stopper, :alive?)
  ensure
    cleanup(client, listener, server_thread)
    stopper&.kill
  end

  def test_stop_abandons_in_flight_delivery_at_deadline
    accepted = Queue.new
    listener, port = build_listener
    server_thread =
      Thread.new do
        socket = listener.accept
        read_request(socket)
        accepted << true
        sleep 1
      ensure
        socket&.close
      end
    client =
      PrometheusExporter::Client.new(
        host: "127.0.0.1",
        port: port,
        thread_sleep: 0.001,
        read_timeout: 2,
      )
    client.send("stalled in flight")
    accepted.pop

    started = monotonic_now
    client.stop(wait_timeout_seconds: 0.05)
    elapsed = monotonic_now - started
    assert_operator(elapsed, :>=, 0.04)
    assert_operator(elapsed, :<, 0.5)
  ensure
    cleanup(client, listener, server_thread)
  end

  def test_public_process_queue_and_synchronous_send_serialize_one_http_session
    first_request = Queue.new
    release = Queue.new
    requests = []
    listener, port = build_listener
    server_thread =
      Thread.new do
        socket = listener.accept
        requests << read_request(socket).last
        first_request << socket
        release.pop
        socket.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: Keep-Alive\r\n\r\nOK")
        requests << read_request(socket).last
        socket.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")
      ensure
        socket&.close
      end
    client = synchronous_client(port)
    client.stub(:ensure_worker_thread!, nil) { client.send("public process_queue") }

    public_delivery = Thread.new { client.process_queue }
    socket = first_request.pop
    synchronous_delivery = Thread.new { client.send("synchronous send") }

    assert_nil(
      IO.select([listener, socket], nil, nil, 0.1),
      "a concurrent delivery mutated the active Net::HTTP session",
    )
    release << true
    public_delivery.join(1)
    synchronous_delivery.join(1)

    refute_predicate(public_delivery, :alive?)
    refute_predicate(synchronous_delivery, :alive?)
    public_delivery.value
    synchronous_delivery.value
    assert_equal(["public process_queue", "synchronous send"], requests)
  ensure
    release << true if release && release.empty?
    cleanup(client, listener, server_thread)
    public_delivery&.kill
    synchronous_delivery&.kill
  end

  def test_stop_does_not_close_a_session_while_synchronous_send_uses_it
    accepted = Queue.new
    release = Queue.new
    listener, port = build_listener
    server_thread =
      Thread.new do
        socket = listener.accept
        read_request(socket)
        accepted << socket
        release.pop
        socket.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: Keep-Alive\r\n\r\nOK")
        socket.read
      ensure
        socket&.close
      end
    client = synchronous_client(port, read_timeout: 1)
    sender = Thread.new { client.send("send while stopping") }
    socket = accepted.pop

    stopper = Thread.new { client.stop(wait_timeout_seconds: 0.05) }
    sleep 0.1
    assert_predicate(stopper, :alive?, "stop closed Net::HTTP concurrently with send")
    assert_nil(IO.select([socket], nil, nil, 0), "stop wrote or closed the active session")

    release << true
    sender.join(1)
    stopper.join(1)
    refute_predicate(sender, :alive?)
    refute_predicate(stopper, :alive?)
    sender.value
    stopper.value
  ensure
    release << true if release && release.empty?
    cleanup(client, listener, server_thread)
    sender&.kill
    stopper&.kill
  end

  def test_tls_connection_survives_parent_child_parent_delivery
    skip "raw fork is unavailable" unless Process.respond_to?(:_fork)

    Dir.mktmpdir("prometheus-exporter-client-fork-tls") do |directory|
      paths = write_tls_chain(directory)
      listener, port = build_listener
      events = Queue.new
      server_thread = start_tls_recording_server(listener, paths, events)
      client =
        synchronous_client(
          port,
          host: "localhost",
          open_timeout: 1,
          read_timeout: 1,
          tls_ca_file: paths[:ca_cert],
          tls_cert_file: paths[:client_cert],
          tls_key_file: paths[:client_key],
        )

      client.send("parent before fork")
      child_read, child_write = IO.pipe
      child_pid = fork_without_unrelated_test_callbacks
      if child_pid.zero?
        child_read.close
        begin
          client.send("child")
          client.stop
          child_write.write("OK")
        rescue => e
          child_write.write("#{e.class}: #{e.message}")
        ensure
          child_write.close
        end
        exit! 0
      end
      child_write.close
      child_result = child_read.read
      child_read.close
      _, child_status = Process.wait2(child_pid)
      child_pid = nil
      assert_predicate(child_status, :success?)
      assert_equal("OK", child_result)

      client.send("parent after fork")
      observed = 3.times.map { events.pop }
      connection_for = observed.to_h

      assert_equal(connection_for["parent before fork"], connection_for["parent after fork"])
      refute_equal(connection_for["parent before fork"], connection_for["child"])
    ensure
      cleanup(client, listener, server_thread)
      child_read&.close unless child_read&.closed?
      child_write&.close unless child_write&.closed?
      Process.wait(child_pid) if child_pid && Process.waitpid(child_pid, Process::WNOHANG).nil?
    end
  end

  def test_oversized_record_is_dropped_before_enqueue
    logs = StringIO.new
    client = PrometheusExporter::Client.new(max_record_size: 3, logger: Logger.new(logs))

    assert_nil(client.send("four"))
    assert_empty(client.instance_variable_get(:@queue))
    assert_match(/dropping message.*4 bytes; maximum is 3 bytes/, logs.string)
  ensure
    client&.stop
  end

  def test_oversized_json_record_is_dropped_without_raising
    logs = StringIO.new
    client = PrometheusExporter::Client.new(max_record_size: 3, logger: Logger.new(logs))

    assert_nil(client.send_json(value: "too large"))
    assert_empty(client.instance_variable_get(:@queue))
    assert_match(/dropping message.*maximum is 3 bytes/, logs.string)
  ensure
    client&.stop
  end

  def test_partial_tls_configuration_fails_fast
    error = assert_raises(ArgumentError) { PrometheusExporter::Client.new(tls_ca_file: "ca.pem") }

    assert_match(/must be configured together/, error.message)
  end

  private

  def build_listener
    listener = TCPServer.new("127.0.0.1", 0)
    [listener, listener.local_address.ip_port]
  end

  def fork_without_unrelated_test_callbacks
    if defined?(ConnectionPool) && ConnectionPool.respond_to?(:after_fork)
      ConnectionPool.stub(:after_fork, nil) { Process._fork }
    else
      Process._fork
    end
  end

  def start_tls_recording_server(listener, paths, events)
    context = OpenSSL::SSL::SSLContext.new
    context.cert = OpenSSL::X509::Certificate.new(File.read(paths[:server_cert]))
    context.key = OpenSSL::PKey.read(File.read(paths[:server_key]))
    ssl_server = OpenSSL::SSL::SSLServer.new(listener, context)

    Thread.new do
      handlers =
        2.times.map do |connection_id|
          socket = ssl_server.accept
          Thread.new do
            loop do
              _request_line, _headers, body = read_request(socket)
              events << [body, connection_id]
              connection = body == "parent after fork" ? "close" : "Keep-Alive"
              socket.write(
                "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: #{connection}\r\n\r\nOK",
              )
              break if connection == "close"
            end
          rescue EOFError, IOError, OpenSSL::SSL::SSLError, SystemCallError
            nil
          ensure
            socket&.close
          end
        end
      handlers.each(&:join)
    rescue IOError, OpenSSL::SSL::SSLError, SystemCallError
      handlers&.each { |handler| handler.kill.join }
    end
  end

  def write_tls_chain(directory)
    ca_key = OpenSSL::PKey::RSA.new(2048)
    ca_cert = certificate("Prometheus Exporter Client Test CA", ca_key, serial: 1, ca: true)
    server_key = OpenSSL::PKey::RSA.new(2048)
    server_cert =
      certificate("localhost", server_key, serial: 2, issuer_cert: ca_cert, issuer_key: ca_key)
    client_key = OpenSSL::PKey::RSA.new(2048)
    client_cert =
      certificate("client", client_key, serial: 3, issuer_cert: ca_cert, issuer_key: ca_key)

    {
      ca_cert: ["ca.crt", ca_cert.to_pem],
      server_cert: ["server.crt", server_cert.to_pem],
      server_key: ["server.key", server_key.to_pem],
      client_cert: ["client.crt", client_cert.to_pem],
      client_key: ["client.key", client_key.to_pem],
    }.transform_values do |filename, contents|
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

  def synchronous_client(port, **options)
    PrometheusExporter::Client.new(
      **{ host: "127.0.0.1", port: port, process_queue_once_and_stop: true }.merge(options),
    )
  end

  def cleanup(client, listener, server_thread)
    client&.stop
    listener&.close
    server_thread&.kill
    server_thread&.join(1)
  rescue IOError, SystemCallError
    nil
  end

  def read_request(socket)
    request_line = socket.gets("\n")
    raise EOFError unless request_line

    request_line = request_line.chomp
    headers = {}
    while (line = socket.gets("\n")) != "\r\n"
      raise EOFError unless line

      name, value = line.split(":", 2)
      headers[name.downcase] = value.strip
    end
    body = socket.read(Integer(headers.fetch("content-length"), 10))
    [request_line, headers, body]
  end

  def monotonic_now
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end
end
