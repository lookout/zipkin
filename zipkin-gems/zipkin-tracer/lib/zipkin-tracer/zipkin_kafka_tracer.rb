require 'finagle-thrift'
require 'finagle-thrift/tracer'
require 'hermann'
require 'hermann/producer'
require 'hermann/discovery/zookeeper'

module Trace
  class ZipkinKafkaTracer < Tracer
    TRACER_CATEGORY = "zipkin"
    KAFKA_TOPIC = "zipkin_kafka"

    def initialize(zookeepers)
      broker_ids = Hermann::Discovery::Zookeeper.new(zookeepers).get_brokers
      @producer = Hermann::Producer.new(nil, broker_ids)
      reset
    end

    def record(id, annotation)
      return unless id.sampled?
      span = get_span_for_id(id)

      case annotation
      when BinaryAnnotation
        span.binary_annotations << annotation
      when Annotation
        span.annotations << annotation
      end

      @count += 1
      # if @count >= @max_buffer || (annotation.is_a?(Annotation) && annotation.value == Annotation::SERVER_SEND)
        flush!
      # end
    end

    def set_rpc_name(id, name)
      return unless id.sampled?
      span = get_span_for_id(id)
      span.name = name.to_s
    end

    private
    def get_span_for_id(id)
      key = id.span_id.to_s
      @spans[key] ||= begin
        Span.new("", id)
      end
    end

    def reset
      @count = 0
      @spans = {}
    end

    def flush!
      # @scribe.batch do
      begin
        messages = @spans.values.map do |span|
          buf = ''
          trans = Thrift::MemoryBufferTransport.new(buf)
          oprot = Thrift::BinaryProtocol.new(trans)
          span.to_thrift.write(oprot)
          # binary = Base64.encode64(buf).gsub("\n", "")
          # @scribe.log(binary, TRACER_CATEGORY)
          @producer.push(buf, :topic => KAFKA_TOPIC).value!
        end
      rescue => e
        puts "....Exception: #{e.message}"
        puts e.backtrace.join("\n")
      end

      reset
    end
  end
end