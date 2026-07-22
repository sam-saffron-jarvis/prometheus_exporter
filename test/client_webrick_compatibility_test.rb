# frozen_string_literal: true

require "minitest/autorun"
require "prometheus_exporter"
require "prometheus_exporter/client"
require "webrick"

class PrometheusExporterClientWEBrickCompatibilityTest < Minitest::Test
  LEGACY_INPUT_BUFFER_SIZE = 65_536

  def test_default_max_record_fits_one_real_legacy_webrick_body_callback
    received = Queue.new
    with_old_webrick_block_handler(received) do |port|
      client = synchronous_client(port)
      payload = "x".b * LEGACY_INPUT_BUFFER_SIZE

      client.send(payload)

      chunks = received.pop
      assert_equal(1, chunks.length)
      assert_equal(payload, chunks.first)
    ensure
      client&.stop
    end
  end

  def test_default_client_drops_above_the_legacy_callback_boundary
    received = Queue.new
    with_old_webrick_block_handler(received) do |port|
      logs = StringIO.new
      client = synchronous_client(port, logger: Logger.new(logs))

      assert_nil(client.send("x".b * (LEGACY_INPUT_BUFFER_SIZE + 1)))
      assert_match(/maximum is 65536 bytes/, logs.string)
      assert_raises(ThreadError) { received.pop(true) }
    ensure
      client&.stop
    end
  end

  def test_old_webrick_really_splits_a_record_above_the_boundary
    received = Queue.new
    with_old_webrick_block_handler(received) do |port|
      client = synchronous_client(port, max_record_size: LEGACY_INPUT_BUFFER_SIZE + 1)

      client.send("x".b * (LEGACY_INPUT_BUFFER_SIZE + 1))

      assert_equal([LEGACY_INPUT_BUFFER_SIZE, 1], received.pop.map(&:bytesize))
    ensure
      client&.stop
    end
  end

  private

  def synchronous_client(port, **options)
    PrometheusExporter::Client.new(
      **{ host: "127.0.0.1", port: port, process_queue_once_and_stop: true }.merge(options),
    )
  end

  def with_old_webrick_block_handler(received)
    server =
      WEBrick::HTTPServer.new(
        Port: 0,
        BindAddress: "127.0.0.1",
        InputBufferSize: LEGACY_INPUT_BUFFER_SIZE,
        Logger: WEBrick::Log.new(File::NULL),
        AccessLog: [],
      )
    server.mount_proc("/send-metrics") do |request, response|
      chunks = []
      request.body { |chunk| chunks << chunk }
      received << chunks
      response.status = 200
      response.body = "OK"
    end
    server_thread = Thread.new { server.start }

    yield server.listeners.first.local_address.ip_port
  ensure
    server&.shutdown
    server_thread&.join(2)
  end
end
