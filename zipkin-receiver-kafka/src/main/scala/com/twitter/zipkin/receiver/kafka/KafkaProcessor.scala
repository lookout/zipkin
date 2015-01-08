package com.twitter.zipkin.receiver.kafka

import com.twitter.zipkin.thriftscala.{Span => ThriftSpan}
import com.twitter.util.{Closable, CloseAwaitably, FuturePool, Future, Time}
import java.util.concurrent.{TimeUnit, Executors}
import java.util.Properties
import kafka.consumer.{Consumer, ConsumerConnector, ConsumerConfig}
import kafka.serializer.{Decoder, StringDecoder}
import com.twitter.scrooge.BinaryThriftStructSerializer
import com.twitter.zipkin.conversions.thrift.{thriftSpanToSpan, spanToThriftSpan}
import kafka.message.Message


object KafkaProcessor {

  type KafkaDecoder = Decoder[Option[List[ThriftSpan]]]

  val defaultKeyDecoder = new StringDecoder()

  def apply[T](
    topics:Map[String, Int],
    config: Properties,
    process: Seq[ThriftSpan] => Future[Unit],
    keyDecoder: Decoder[T],
    valueDecoder: KafkaDecoder
  ): KafkaProcessor[T] = new KafkaProcessor(topics, config, process, keyDecoder, valueDecoder)
}

class KafkaProcessor[T](
  topics: Map[String, Int],
  config: Properties,
  process: Seq[ThriftSpan] => Future[Unit],
  keyDecoder: Decoder[T],
  valueDecoder: KafkaProcessor.KafkaDecoder
) extends Closable with CloseAwaitably {
  private[this] val processorPool = {
    val consumerConnector: ConsumerConnector = Consumer.create(new ConsumerConfig(config))
    val threadCount = topics.foldLeft(0) { case (sum, (_, a)) => sum + a }
    val pool = Executors.newFixedThreadPool(threadCount)
    for {
      (topic, streams) <- consumerConnector.createMessageStreams(topics, keyDecoder, valueDecoder)
      stream <- streams
    } pool.submit(new KafkaStreamProcessor(stream, process))
    pool
  }

  def close(deadline: Time): Future[Unit] = closeAwaitably {
    FuturePool.unboundedPool {
      processorPool.shutdown()
      processorPool.awaitTermination(deadline.inMilliseconds, TimeUnit.MILLISECONDS)
    }
  }
}
