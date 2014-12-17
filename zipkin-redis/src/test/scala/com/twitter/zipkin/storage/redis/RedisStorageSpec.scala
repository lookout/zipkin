/*
 * Copyright 2012 Tumblr Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.zipkin.storage.redis

import com.twitter.conversions.time.intToTimeableNumber
import com.twitter.zipkin.common.{Annotation, AnnotationType, BinaryAnnotation, Endpoint, Span}
import com.twitter.zipkin.conversions.thrift.thriftAnnotationTypeToAnnotationType
import com.twitter.zipkin.thriftscala
import java.nio.ByteBuffer

class RedisStorageSpec extends RedisSpecification {

  var redisStorage: RedisStorage = null

  def binaryAnnotation(key: String, value: String) =
    BinaryAnnotation(
      key,
      ByteBuffer.wrap(value.getBytes),
      AnnotationType.String,
      Some(ep)
    )

  val ep = Endpoint(123, 123, "service")

  val spanId = 456
  val ann1 = Annotation(1, "cs", Some(ep))
  val ann2 = Annotation(2, "sr", None)
  val ann3 = Annotation(2, "custom", Some(ep))
  val ann4 = Annotation(2, "custom", Some(ep))

  val span1 = Span(123, "methodcall", spanId, None, List(ann1, ann3),
    List(binaryAnnotation("BAH", "BEH")))

  "RedisStorage" should {

    doBefore {
      _client.flushDB()
      redisStorage = new RedisStorage {
        val database = _client
        val ttl = Some(7.days)
      }
    }

    doAfter {
      redisStorage.close()
    }

    "getTraceById" in {
      redisStorage.storeSpan(span1)()
      val trace = redisStorage.getSpansByTraceId(span1.traceId)()
      trace.isEmpty mustEqual false
      trace(0) mustEqual span1
    }

    "getTracesByIds" in {
      redisStorage.storeSpan(span1)()
      val actual1 = redisStorage.getSpansByTraceIds(List(span1.traceId))()
      actual1.isEmpty mustEqual false
      actual1(0).isEmpty mustEqual false
      actual1(0)(0) mustEqual span1

      val span2 = Span(666, "methodcall2", spanId, None, List(ann2),
        List(binaryAnnotation("BAH2", "BEH2")))
      redisStorage.storeSpan(span2)()
      val actual2 = redisStorage.getSpansByTraceIds(List(span1.traceId, span2.traceId))()
      actual2.isEmpty mustEqual false
      actual2(0).isEmpty mustEqual false
      actual2(0)(0) mustEqual span1
      actual2(1).isEmpty mustEqual false
      actual2(1)(0) mustEqual span2
    }

    "getTracesByIds should return empty list if no trace exists" in {
      val actual1 = redisStorage.getSpansByTraceIds(List(span1.traceId))()
      actual1.isEmpty mustEqual true
    }

    "set time to live on a trace and then get it" in {
      redisStorage.storeSpan(span1)()
      redisStorage.setTimeToLive(span1.traceId, 1234.seconds)()
      redisStorage.getTimeToLive(span1.traceId)() mustEqual 1234.seconds
    }
  }
}
