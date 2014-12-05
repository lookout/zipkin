/*
 * Copyright 2012 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
// package com.twitter.zipkin.collector

// import com.twitter.logging.Logger
// import com.twitter.ostrich.admin.{ServiceTracker, RuntimeEnvironment}
// import com.twitter.util.Eval
// import com.twitter.zipkin.collector.builder.CollectorServiceBuilder
// import com.twitter.zipkin.BuildProperties


// object Main {
//   val log = Logger.get(getClass.getName)

//   def main(args: Array[String]) {
//     log.info("Loading configuration")
//     val runtime = RuntimeEnvironment(BuildProperties, args)
//     val builder = (new Eval).apply[CollectorServiceBuilder[Seq[String]]](runtime.configFile)
//     try {
//       val server = builder.apply().apply(runtime)
//       server.start()
//       ServiceTracker.register(server)
//     } catch {
//       case e: Exception =>
//         e.printStackTrace()
//         log.error(e, "Unexpected exception: %s", e.getMessage)
//         System.exit(0)
//     }
//   }
// }




import com.twitter.zipkin.gen.{Span => ThriftSpan}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.server.TwitterServer
import com.twitter.util.{Await, Future}
import com.twitter.zipkin.cassandra.CassieSpanStoreFactory
import com.twitter.zipkin.collector.{SpanReceiver, ZipkinQueuedCollectorFactory}
import com.twitter.zipkin.common._
import com.twitter.zipkin.receiver.kafka.KafkaSpanReceiverFactory
import com.twitter.zipkin.storage.WriteSpanStore
import com.twitter.zipkin.zookeeper.ZooKeeperClientFactory
import kafka.serializer.Decoder

// JW: added this
import com.twitter.zipkin.receiver.kafka.SpanDecoder

object ZipkinKafkaCollectorServer extends TwitterServer
  with ZipkinQueuedCollectorFactory
  with CassieSpanStoreFactory
  with ZooKeeperClientFactory
  with KafkaSpanReceiverFactory
{
  def newReceiver(receive: Seq[ThriftSpan] => Future[Unit], stats: StatsReceiver): SpanReceiver =
    newKafkaSpanReceiver(receive, stats.scope("kafkaSpanReceiver"), new SpanDecoder())

  def newSpanStore(stats: StatsReceiver): WriteSpanStore =
    newCassandraStore(stats.scope("cassie"))

  def main() {
    val collector = newCollector(statsReceiver)
    onExit { collector.close() }
    Await.ready(collector)
  }
}
