package com.twitter.zipkin.receiver.kafka

import com.twitter.logging.Logger
import com.twitter.util._
import com.twitter.zipkin.thriftscala.{Span => ThriftSpan}
import kafka.consumer.KafkaStream
import com.twitter.zipkin.collector.QueueFullException
import kafka.message.MessageAndMetadata
import scala.util.{Try, Success, Failure}
import com.twitter.zipkin.storage.util.Retry

case class KafkaStreamProcessor[T](
  stream: KafkaStream[T, List[ThriftSpan]],
  process: Seq[ThriftSpan] => Future[Unit]
  ) extends Runnable {

  private[this] val log = Logger.get(getClass.getName)
  private val retrycount = 5

  def run() {
    log.debug(s"${KafkaStreamProcessor.getClass.getName} run")
    try {
      stream foreach { msg =>
        log.debug(s"processing event ${msg.message()}")
        Retry(retrycount) { Await.ready(process(msg.message)) }
      }
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        log.error(s"${e.getCause}")
      }
    }
  }
}
