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
package com.twitter.zipkin.query

import scala.collection.Map
import com.twitter.zipkin.common.Endpoint

case class SpanTimestamp(name: String, startTimestamp: Long, endTimestamp: Long) {
  def duration = endTimestamp - startTimestamp
}

object TraceSummary {

  /**
   * Return a summary of this trace or none if we
   * cannot construct a trace summary. Could be that we have no spans.
   */
  def apply(trace: Trace): Option[TraceSummary] = {
    for (traceId <- trace.id; startEnd <- trace.getStartAndEndTimestamp)
    yield TraceSummary(
      traceId,
      startEnd.start,
      startEnd.end,
      (startEnd.end - startEnd.start).toInt,
      trace.spanTimestamps.toList,
      trace.endpoints.toList)
  }
}

/**
 * @param traceId id of this trace
 * @param startTimestamp when did the trace start?
 * @param endTimestamp when did the trace end?
 * @param durationMicro how long did the traced operation take?
 * @param serviceCounts name of the services involved in the traced operation
 *                      mapped to the number of spans of that service
 * @param endpoints endpoints involved in the traced operation
 */
case class TraceSummary(
  traceId: Long,
  startTimestamp: Long,
  endTimestamp: Long,
  durationMicro: Int,
  spanTimestamps: List[SpanTimestamp],
  endpoints: List[Endpoint])
