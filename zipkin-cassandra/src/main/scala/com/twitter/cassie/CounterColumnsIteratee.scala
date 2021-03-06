// Copyright 2012 Twitter, Inc.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.cassie

import com.twitter.util.{ Future, Promise }
import java.util.{ Map => JMap, List => JList, ArrayList => JArrayList }
import org.apache.cassandra.finagle.thrift
import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer

/**
 * Async iteration across the columns for a given key.
 *
 * EXAMPLE
 * val cf = new Cluster("127.0.0.1").keyspace("foo")
 *   .connect().columnFamily("bar", Utf8Codec, Utf8Codec, Utf8Codec)
 *
 * val done = cf.CounterColumnsIteratee.foreach("bam").foreach {col =>
 *   println(col) // this function is executed asynchronously for each column
 * }
 * done() // this is a Future[Unit] that will be satisfied when the iteration
 *        //   is done
 */

trait CounterColumnsIteratee[Key, Name] {

  def hasNext(): Boolean
  def next(): Future[CounterColumnsIteratee[Key, Name]]

  def foreach(f: CounterColumn[Name] => Unit): Future[Unit] = {
    val p = new Promise[Unit]
    next map (_.visit(p, f)) handle { case e => p.setException(e) }
    p
  }

  def map[A](f: CounterColumn[Name] => A): Future[Iterable[A]] = {
    val buffer = Buffer.empty[A]
    foreach { column =>
      buffer.append(f(column))
    }.map { _ => buffer }
  }
  def visit(p: Promise[Unit], f: CounterColumn[Name] => Unit): Unit
}

object CounterColumnsIteratee {
  def apply[Key, Name](cf: CounterColumnFamily[Key, Name], key: Key,
    start: Option[Name], end: Option[Name], batchSize: Int,
    limit: Int, order: Order = Order.Normal) = {
    new InitialCounterColumnsIteratee(cf, key, start, end, batchSize, limit, order)
  }
}

private[cassie] class InitialCounterColumnsIteratee[Key, Name](
  val cf: CounterColumnFamily[Key, Name],
  val key: Key,
  val start: Option[Name],
  val end: Option[Name],
  val batchSize: Int,
  val remaining: Int,
  val order: Order)
  extends CounterColumnsIteratee[Key, Name] {

  def hasNext() = true

  def next() = {
    val fetchSize = math.min(batchSize, remaining)

    cf.getRowSlice(key, start, end, fetchSize, order).map { buf =>
      if (buf.size < batchSize || batchSize == remaining) {
        new FinalCounterColumnsIteratee(buf)
      } else {
        new SubsequentCounterColumnsIteratee(cf, key, batchSize, buf.last.name, end, remaining - buf.size, order, buf)
      }
    }
  }

  def visit(p: Promise[Unit], f: CounterColumn[Name] => Unit) {
    throw new UnsupportedOperationException("no need to visit the initial Iteratee")
  }
}

private[cassie] class SubsequentCounterColumnsIteratee[Key, Name](
  val cf: CounterColumnFamily[Key, Name],
  val key: Key,
  val batchSize: Int,
  val start: Name,
  val end: Option[Name],
  val remaining: Int,
  val order: Order,
  val buffer: JList[CounterColumn[Name]])
  extends CounterColumnsIteratee[Key, Name] {

  def hasNext = true

  def next() = {
    val fetchSize = math.min(batchSize + 1, remaining + 1)
    cf.getRowSlice(key, Some(start), end, fetchSize, order).map { buf =>
      val skipped = buf.subList(1, buf.length)
      if (skipped.size() < batchSize || batchSize == remaining) {
        new FinalCounterColumnsIteratee(skipped)
      } else {
        new SubsequentCounterColumnsIteratee(cf, key, batchSize, skipped.last.name, end, remaining - skipped.size, order, skipped)
      }
    }
  }

  def visit(p: Promise[Unit], f: CounterColumn[Name] => Unit) {
    for (c <- buffer) {
      f(c)
    }
    if (hasNext) {
      next map (_.visit(p, f)) handle { case e => p.setException(e) }
    } else {
      p.setValue(Unit)
    }
  }
}

private[cassie] class FinalCounterColumnsIteratee[Key, Name](
  val buffer: JList[CounterColumn[Name]])
  extends CounterColumnsIteratee[Key, Name] {
  def hasNext = false
  def next = Future.exception(new UnsupportedOperationException("no next for the final iteratee"))

  def visit(p: Promise[Unit], f: CounterColumn[Name] => Unit) {
    for (c <- buffer) {
      f(c)
    }
    p.setValue(Unit)
  }
}
