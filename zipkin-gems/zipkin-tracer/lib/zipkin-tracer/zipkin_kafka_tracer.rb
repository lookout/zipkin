require 'finagle-thrift'
require 'finagle-thrift/tracer'
require 'hermann'
require 'hermann/producer'
require 'hermann/discovery/zookeeper'

module Trace
  class ZipkinKafkaTracer < Tracer
    TRACER_CATEGORY = "zipkin"
    DEFAULT_KAFKA_TOPIC = "zipkin_kafka"

    def initialize(zookeepers, opts={})
      broker_ids = Hermann::Discovery::Zookeeper.new(zookeepers).get_brokers
      @producer  = Hermann::Producer.new(nil, broker_ids)
      @logger    = opts[:logger]
      @topic     = opts[:topic] || DEFAULT_KAFKA_TOPIC
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

      flush!
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

      def flush!
        begin
          messages = @spans.values.map do |span|
            buf = ''
            trans = Thrift::MemoryBufferTransport.new(buf)
            oprot = Thrift::BinaryProtocol.new(trans)
            span.to_thrift.write(oprot)
            @producer.push(buf, :topic => @topic).value!
          end
        rescue => e
          if @logger
            @logger.error("Exception: #{e.message}")
            @logger.error(e.backtrace.join("\n"))
          end
        end
      end
  end
end