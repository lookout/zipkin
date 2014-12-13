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
package com.twitter.zipkin.collector

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.Service
import com.twitter.logging.Logger
import com.twitter.util.{Time, Future}
import com.twitter.scrooge.BinaryThriftStructSerializer
import com.twitter.zipkin.common.Span
import com.twitter.zipkin.conversions.thrift._
import com.twitter.zipkin.gen
import kafka.message.Message
import kafka.producer._
import kafka.serializer.Encoder
import java.nio.charset.StandardCharsets

class Kafka(
  kafka: Producer[String, gen.Span],
  topic: String,
  statsReceiver: StatsReceiver
) extends Service[Span, Unit] {

  private[this] val log = Logger.get()

  def send(message: String, partition: String = null): Unit = {
    send(new String(message.getBytes(StandardCharsets.UTF_8)),
      if (partition == null) null else new String(partition.getBytes(StandardCharsets.UTF_8)))
  }

  def apply(req: Span): Future[Unit] = {
    statsReceiver.counter("try").incr()

    Future {
      kafka.send(new KeyedMessage(topic, req.toThrift))
    } onSuccess { (_) =>
      statsReceiver.counter("success").incr()
    }
  }

  override def close(deadline: Time) = {
    kafka.close()
    super.close(deadline)
  }
}

class SpanEncoder extends Encoder[gen.Span] {
  val serializer = new BinaryThriftStructSerializer[gen.Span] {
    def codec = gen.Span
  }
  def toBytes(span: gen.Span): Array[Byte] = serializer.toBytes(span)

  def toMessage(span: gen.Span): Message = {
    new Message(serializer.toBytes(span))
  }
}
