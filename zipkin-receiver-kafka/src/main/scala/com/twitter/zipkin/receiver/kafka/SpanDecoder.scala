package com.twitter.zipkin.receiver.kafka

import com.twitter.zipkin.thriftscala.{Span => ThriftSpan}
import com.twitter.scrooge.BinaryThriftStructSerializer
import com.twitter.zipkin.conversions.thrift.{thriftSpanToSpan, spanToThriftSpan}
import kafka.message.Message

class SpanDecoder extends KafkaProcessor.KafkaDecoder {
  val deserializer = new BinaryThriftStructSerializer[ThriftSpan] {
    def codec = ThriftSpan
  }

  def toEvent(message: Message): Option[List[ThriftSpan]] = {

    val buffer = message.payload
    val payload = new Array[Byte](buffer.remaining)
    buffer.get(payload)
    val span = deserializer.fromBytes(payload)
    Some(List(span))
  }
  def fromBytes(bytes: Array[Byte]): Option[List[ThriftSpan]] = Some(List(deserializer.fromBytes(bytes)))
}
