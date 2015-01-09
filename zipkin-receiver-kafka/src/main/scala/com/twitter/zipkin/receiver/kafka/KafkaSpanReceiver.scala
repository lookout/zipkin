package com.twitter.zipkin.receiver.kafka

import java.util.Properties
import com.twitter.app.App
import com.twitter.finagle.stats.{DefaultStatsReceiver, StatsReceiver}
import com.twitter.util.{Closable, Future, Time}
import com.twitter.zipkin.collector.SpanReceiver
import com.twitter.zipkin.thriftscala.{Span => ThriftSpan}
import kafka.serializer.Decoder

trait KafkaSpanReceiverFactory { self: App =>
  val defaultKafkaServer = "127.0.0.1:2181"
  val defaultKafkaGroupId = "zipkinId"
  val defaultKafkaZkConnectionTimeout = "1000000"
  val defaultKafkaSessionTimeout = "4000"
  val defaultKafkaSyncTime = "200"
  val defaultKafkaAutoOffset = "smallest"
  val defaultKafkaTopics = Map("zipkin_kafka" -> 1)
  val defaultAutoCommitInterval = "10"
  val defaultConsumerId = "zipkin-consumerid"
  val defaultConsumerTimeout = "-1"
  val defaultRebalanceMaxRetries = "4"
  val defaultNumConsumerFetchers = "2"

  val kafkaTopics = flag[Map[String, Int]]("zipkin.kafka.topics", defaultKafkaTopics, "kafka topics to collect from")
  val kafkaServer = flag("zipkin.kafka.server", defaultKafkaServer, "kafka server to connect")
  val kafkaGroupId = flag("zipkin.kafka.groupid", defaultKafkaGroupId, "kafka group id")
  val kafkaZkConnectionTimeout = flag("zipkin.kafka.zk.connectionTimeout", defaultKafkaZkConnectionTimeout, "kafka zk connection timeout in ms")
  val kafkaSessionTimeout = flag("zipkin.kafka.zk.sessionTimeout", defaultKafkaSessionTimeout, "kafka zk session timeout in ms")
  val kafkaSyncTime = flag("zipkin.kafka.zk.syncTime", defaultKafkaSyncTime, "kafka zk sync time in ms")
  val kafkaAutoOffset = flag("zipkin.kafka.zk.autooffset", defaultKafkaAutoOffset, "kafka zk auto offset [smallest|largest]")

  val kafkaAutoCommitInterval = flag("zipkin.kafka.autocommitInterval", defaultAutoCommitInterval, "auto commit interval")
  val kafkaConsumerId = flag("zipkin.kafka.consumerId", defaultConsumerId, "consumer ID")
  val kafkaConsumerTimeout = flag("zipkin.kafka.consumerTimeout", defaultConsumerTimeout, "consumer timeout")
  val kafkaRebalanceMaxRetries = flag("zipkin.kafka.rebalanceMaxRetries", defaultRebalanceMaxRetries, "rebalance max retries")
  val kafkaNumConsumerFetchers = flag("zipkin.kafka.consumerFetchers", defaultNumConsumerFetchers, "num consumer fetchers" )


  def newKafkaSpanReceiver[T](
    process: Seq[ThriftSpan] => Future[Unit],
    stats: StatsReceiver = DefaultStatsReceiver.scope("KafkaSpanReceiver"),
    keyDecoder: Option[Decoder[T]],
    valueDecoder: KafkaProcessor.KafkaDecoder
  ): SpanReceiver = new SpanReceiver {

    val receiverProps = new Properties() {
      put("group.id", kafkaGroupId())
      put("zookeeper.connect", kafkaServer() )
      put("zookeeper.connection.timeout.ms", kafkaZkConnectionTimeout())
      put("zookeeper.session.timeout.ms", kafkaSessionTimeout())
      put("zookeeper.sync.time.ms", kafkaSyncTime())
      put("auto.offset.reset", kafkaAutoOffset())
      put("auto.commit.interval.ms", kafkaAutoCommitInterval())
      put("consumer.id", kafkaConsumerId())
      put("consumer.timeout.ms", kafkaConsumerTimeout())
      put("rebalance.max.retries", kafkaRebalanceMaxRetries())
      put("num.consumer.fetchers", kafkaNumConsumerFetchers())
    }

    val service = KafkaProcessor(kafkaTopics(), receiverProps, process, keyDecoder getOrElse KafkaProcessor.defaultKeyDecoder, valueDecoder)

    def close(deadline: Time): Future[Unit] = closeAwaitably {
      Closable.sequence(service).close(deadline)
    }
  }
}
