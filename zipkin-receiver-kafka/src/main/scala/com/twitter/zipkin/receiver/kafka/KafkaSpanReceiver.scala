package com.twitter.zipkin.receiver.kafka

import com.twitter.app.{App, Flaggable}
import java.util.Properties
import com.twitter.zipkin.thriftscala.{Span => ThriftSpan}
import com.twitter.zipkin.collector.SpanReceiver
import com.twitter.zipkin.conversions.thrift._
import com.twitter.util.{Closable, Future, Time}
import com.twitter.finagle.stats.{DefaultStatsReceiver, StatsReceiver}
import com.twitter.zipkin.zookeeper.ZooKeeperClientFactory

trait KafkaSpanReceiverFactory { self: App =>
  val defaultZookeeperServer = "localhost:2181"
  val defaultKafkaGroupId = "zipkinId"
  val defaultKafkaZkConnectionTimeout = "1000000"
  val defaultKafkaSessionTimeout = "4000"
  val defaultKafkaSyncTime = "200"
  val defaultKafkaAutoOffset = "largest"
  val defaultKafkaTopics = Map("zipkin_kafka" -> 1)

  val kafkaTopics = flag[Map[String, Int]]("zipkin.kafka.topics", defaultKafkaTopics, "kafka topics to collect from")
  val kafkaServer = flag("zipkin.kafka.server", defaultZookeeperServer, "kafka server to connect")
  val kafkaGroupId = flag("zipkin.kafka.groupid", defaultKafkaGroupId, "kafka group id")
  val kafkaZkConnectionTimeout = flag("zipkin.kafka.zk.connectionTimeout", defaultKafkaZkConnectionTimeout, "kafka zk connection timeout in ms")
  val kafkaSessionTimeout = flag("zipkin.kafka.zk.sessionTimeout", defaultKafkaSessionTimeout, "kafka zk session timeout in ms")
  val kafkaSyncTime = flag("zipkin.kafka.zk.syncTime", defaultKafkaSyncTime, "kafka zk sync time in ms")
  val kafkaAutoOffset = flag("zipkin.kafka.zk.autooffset", defaultKafkaAutoOffset, "kafka zk auto offset [smallest|largest]")

  def newKafkaSpanReceiver(
    process: Seq[ThriftSpan] => Future[Unit],
    stats: StatsReceiver = DefaultStatsReceiver.scope("KafkaSpanReceiver"),
    decoder: KafkaProcessor.KafkaDecoder
  ): SpanReceiver = new SpanReceiver {

    val props = new Properties() {
      put("group.id", kafkaGroupId())
      put("zookeeper.connect", kafkaServer() )
      put("zookeeper.connection.timeout.ms", kafkaZkConnectionTimeout())
      put("zookeeper.session.timeout.ms", kafkaSessionTimeout())
      put("zookeeper.sync.time.ms", kafkaSyncTime())
      put("auto.offset.reset", kafkaAutoOffset())
    }

    val service = KafkaProcessor(kafkaTopics(), props, process, decoder)

    def close(deadline: Time): Future[Unit] = closeAwaitably {
      Closable.sequence(service).close(deadline)
    }
  }

}
