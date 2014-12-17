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

import com.twitter.conversions.time._
import com.twitter.util.Duration
import com.twitter.zipkin.thriftscala.Adjust
import com.twitter.zipkin.query.adjusters._

package object constants {
  /* Amount of time padding to use when resolving complex query timestamps */
  val TraceTimestampPadding: Duration = 1.minute

  val DefaultAdjusters = Map[Adjust, Adjuster](
    Adjust.Nothing -> NullAdjuster,
    Adjust.TimeSkew -> new TimeSkewAdjuster()
  )
}
