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
package com.twitter.zipkin.common

import collection.mutable
import com.twitter.zipkin.Constants
import com.twitter.zipkin.query.{Timespan, Trace, TraceSummary, SpanTreeEntry}
import java.nio.ByteBuffer
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TraceTest extends FunSuite {

  // TODO these don't actually make any sense
  val annotations1 = List(Annotation(100, Constants.ClientSend, Some(Endpoint(123, 123, "service1"))),
    Annotation(150, Constants.ClientRecv, Some(Endpoint(456, 456, "service1"))))
  val annotations2 = List(Annotation(200, Constants.ClientSend, Some(Endpoint(456, 456, "service2"))),
    Annotation(250, Constants.ClientRecv, Some(Endpoint(123, 123, "service2"))))
  val annotations3 = List(Annotation(300, Constants.ClientSend, Some(Endpoint(456, 456, "service2"))),
    Annotation(350, Constants.ClientRecv, Some(Endpoint(666, 666, "service2"))))
  val annotations4 = List(Annotation(400, Constants.ClientSend, Some(Endpoint(777, 777, "service3"))),
    Annotation(500, Constants.ClientRecv, Some(Endpoint(888, 888, "service3"))))

  val span1Id = 666L
  val span2Id = 777L
  val span3Id = 888L
  val span4Id = 999L

  val span1 = Span(12345, "methodcall1", span1Id, None, annotations1, Nil)
  val span2 = Span(12345, "methodcall2", span2Id, Some(span1Id), annotations2, Nil)
  val span3 = Span(12345, "methodcall2", span3Id, Some(span2Id), annotations3, Nil)
  val span4 = Span(12345, "methodcall2", span4Id, Some(span3Id), annotations4, Nil)
  val span5 = Span(12345, "methodcall4", 1111L, Some(span4Id), List(), Nil) // no annotations

  val trace = Trace(List[Span](span1, span2, span3, span4))

  test("get duration of trace") {
    val annotations = List(Annotation(100, Constants.ClientSend, Some(Endpoint(123, 123, "service1"))),
      Annotation(200, Constants.ClientRecv, Some(Endpoint(123, 123, "service1"))))
    val span = Span(12345, "methodcall", 666, None,
      annotations, Nil)
    assert(Trace(List(span)).duration === 100)
  }

  test("get duration of trace without root span") {
    val annotations = List(Annotation(100, Constants.ClientSend, Some(Endpoint(123, 123, "service1"))),
      Annotation(200, Constants.ClientRecv, Some(Endpoint(123, 123, "service1"))))
    val span = Span(12345, "methodcall", 666, Some(123),
      annotations, Nil)
    val annotations2 = List(Annotation(150, Constants.ClientSend, Some(Endpoint(123, 123, "service1"))),
      Annotation(160, Constants.ClientRecv, Some(Endpoint(123, 123, "service1"))))
    val span2 = Span(12345, "methodcall", 666, Some(123),
      annotations2, Nil)
    assert(Trace(List(span, span2)).duration === 100)
  }

  test("get correct duration for imbalanced spans") {
    val ann1 = List(
      Annotation(0, "Client send", None)
    )
    val ann2 = List(
      Annotation(1, "Server receive", None),
      Annotation(12, "Server send", None)
    )

    val span1 = Span(123, "method_1", 100, None, ann1, Nil)
    val span2 = Span(123, "method_2", 200, Some(100), ann2, Nil)

    val trace = new Trace(Seq(span1, span2))
    val duration = TraceSummary(trace).get.durationMicro
    assert(duration === 12)
  }

  test("get services involved in trace") {
    val expectedServices = Set("service1", "service2", "service3")
    assert(expectedServices === Trace(List(span1, span2, span3, span4)).services)
  }

  test("get endpooints involved in trace") {
    val expectedEndpoints = Set(Endpoint(123, 123, "service1"), Endpoint(123, 123, "service2"),
      Endpoint(456, 456, "service1"), Endpoint(456, 456, "service2"), Endpoint(666, 666, "service2"),
      Endpoint(777, 777, "service3"), Endpoint(888, 888, "service3"))
    assert(expectedEndpoints === Trace(List(span1, span2, span3, span4)).endpoints)
  }

  test("return root span") {
    assert(trace.getRootSpan === Some(span1))
  }

  test("getIdToChildrenMap") {
    val map = new mutable.HashMap[Long, mutable.Set[Span]] with mutable.MultiMap[Long, Span]
    map.addBinding(span1Id, span2)
    map.addBinding(span2Id, span3)
    map.addBinding(span3Id, span4)
    assert(trace.getIdToChildrenMap === map)
  }

  test("getBinaryAnnotations") {
    val ba1 = BinaryAnnotation("key1", ByteBuffer.wrap("value1".getBytes), AnnotationType.String, None)
    val span1 = Span(1L, "", 1L, None, List(), List(ba1))
    val ba2 = BinaryAnnotation("key2", ByteBuffer.wrap("value2".getBytes), AnnotationType.String, None)
    val span2 = Span(1L, "", 2L, None, List(), List(ba2))

    val trace = Trace(List[Span](span1, span2))
    def counts(e: Traversable[_]) = e groupBy identity mapValues (_.size)
    assert(counts(trace.getBinaryAnnotations) === counts(Seq(ba1, ba2)))
  }

  test("getSpanTree") {
    val spanTreeEntry4 = SpanTreeEntry(span4, List[SpanTreeEntry]())
    val spanTreeEntry3 = SpanTreeEntry(span3, List[SpanTreeEntry](spanTreeEntry4))
    val spanTreeEntry2 = SpanTreeEntry(span2, List[SpanTreeEntry](spanTreeEntry3))
    val spanTreeEntry1 = SpanTreeEntry(span1, List[SpanTreeEntry](spanTreeEntry2))
    assert(trace.getSpanTree(trace.getRootSpan.get, trace.getIdToChildrenMap) === spanTreeEntry1)
  }

  test("instantiate with SpanTreeEntry") {
    val spanTree = trace.getSpanTree(trace.getRootSpan.get, trace.getIdToChildrenMap)
    val actualTrace = Trace(spanTree)
    assert(trace === actualTrace)
  }

  test("return none due to missing annotations") {
    // no annotation at all
    val spanNoAnn = Span(1, "method", 123L, None, List(), Nil)
    val noAnnTrace = Trace(List(spanNoAnn))
    assert(noAnnTrace.getStartAndEndTimestamp === None)
  }

  test("sort spans by first annotation timestamp") {
    val inputSpans = List[Span](span4, span3, span5, span1, span2)
    val expectedTrace = Trace(List[Span](span1, span2, span3, span4, span5))
    val actualTrace = Trace(inputSpans)

    assert(expectedTrace.spans === actualTrace.spans)
  }

  test("merge spans") {
    val ann1 = List(Annotation(100, Constants.ClientSend, Some(Endpoint(123, 123, "service1"))),
      Annotation(300, Constants.ClientRecv, Some(Endpoint(123, 123, "service1"))))
    val ann2 = List(Annotation(150, Constants.ServerRecv, Some(Endpoint(456, 456, "service2"))),
      Annotation(200, Constants.ServerSend, Some(Endpoint(456, 456, "service2"))))

    val annMerged = List(
      Annotation(100, Constants.ClientSend, Some(Endpoint(123, 123, "service1"))),
      Annotation(300, Constants.ClientRecv, Some(Endpoint(123, 123, "service1"))),
      Annotation(150, Constants.ServerRecv, Some(Endpoint(456, 456, "service2"))),
      Annotation(200, Constants.ServerSend, Some(Endpoint(456, 456, "service2")))
    )

    val spanToMerge1 = Span(12345, "methodcall2", span2Id, Some(span1Id), ann1, Nil)
    val spanToMerge2 = Span(12345, "methodcall2", span2Id, Some(span1Id), ann2, Nil)
    val spanMerged = Span(12345, "methodcall2", span2Id, Some(span1Id), annMerged, Nil)

    assert(Trace(List(spanMerged)).spans === Trace(List(spanToMerge1, spanToMerge2)).spans)
  }

  test("get rootmost span from full trace") {
    val spanNoneParent = Span(1, "", 100, None, List(), Nil)
    val spanParent = Span(1, "", 200, Some(100), List(), Nil)
    assert(Trace(List(spanParent, spanNoneParent)).getRootMostSpan === Some(spanNoneParent))
  }

  test("get rootmost span from trace without real root") {
    val spanNoParent = Span(1, "", 100, Some(0), List(), Nil)
    val spanParent = Span(1, "", 200, Some(100), List(), Nil)
    assert(Trace(List(spanParent, spanNoParent)).getRootMostSpan === Some(spanNoParent))
  }

  test("get span depths for trace") {
    assert(trace.toSpanDepths === Some(Map(666 -> 1, 777 -> 2, 888 -> 3, 999 -> 4)))
  }

  test("get no span depths for empty trace") {
    assert(Trace(List()).toSpanDepths === None)
  }

  test("get start and end timestamp") {
    val ann1 = Annotation(1, "hello", None)
    val ann2 = Annotation(43, "goodbye", None)

    val span1 = Span(12345, "methodcall", 6789, None, List(), Nil)
    val span2 = Span(12345, "methodcall_2", 345, None, List(ann1, ann2), Nil)

    val span3 = Span(23456, "methodcall_3", 12, None, List(ann1), Nil)
    val span4 = Span(23456, "methodcall_4", 34, None, List(ann2), Nil)

    // no spans
    val trace1 = new Trace(List())
    assert(trace1.getStartAndEndTimestamp === None)

    // 1 span, 0 annotations
    val trace2 = new Trace(List(span1))
    assert(trace2.getStartAndEndTimestamp === None)

    val trace3 = new Trace(List(span1, span2))
    assert(trace3.getStartAndEndTimestamp === Some(Timespan(1, 43)))

    val trace4 = new Trace(List(span3, span4))
    assert(trace4.getStartAndEndTimestamp === Some(Timespan(1, 43)))
  }

  test("get service counts") {
    val ep1 = Some(Endpoint(1, 1, "ep1"))
    val ep2 = Some(Endpoint(2, 2, "ep2"))
    val ep3 = Some(Endpoint(3, 3, "ep3"))

    val ann1 = Annotation(1, "ann1", ep1)
    val ann2 = Annotation(2, "ann2", ep2)
    val ann3 = Annotation(3, "ann3", ep3)
    val ann4 = Annotation(4, "ann4", ep2)

    val span1 = Span(1234, "method1", 5678, None, List(ann1, ann2, ann3, ann4), Nil)
    val span2 = Span(1234, "method2", 345, None, List(ann4), Nil)
    val trace1 = new Trace(Seq(span1, span2))

    val expected = Map("ep1" -> 1, "ep2" -> 2, "ep3" -> 1)
    assert(trace1.serviceCounts === expected)
  }
}
