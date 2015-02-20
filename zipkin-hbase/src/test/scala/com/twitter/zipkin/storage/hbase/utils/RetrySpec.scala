package com.twitter.zipkin.storage.hbase.utils

import org.specs.SpecificationWithJUnit
import java.util.concurrent.atomic.AtomicLong
import com.twitter.zipkin.storage.util.Retry

class RetrySpec extends SpecificationWithJUnit {
  "Retry" should {
    "return if success" in {
      val counter = new AtomicLong(0)
      val result = Retry(10) {
        val innerResult: Long = counter.incrementAndGet()
        LongWrapper(innerResult)
      }
      result must_== LongWrapper(1L)
    }
    "throw an error if retries are exhausted" in {
      {
        val result = Retry(5) {
          throw new Exception("No! No! No!")
          LongWrapper(1)
        }
      } must throwA[Retry.RetriesExhaustedException]

    }
    "return if fewer than max retries are needed" in {
      val counter = new AtomicLong(0)
      val result = Retry(10) {
        val innerResult: Long = counter.incrementAndGet()
        if (innerResult < 10) {
           throw new Exception("No No No")
        }
        LongWrapper(innerResult)
      }
      result must_== LongWrapper(10L)
    }
    "sleeps when exception thrown and flag set" in {
      val t1 = System.currentTimeMillis
      try {
        val result = Retry(2, true) {
          throw new Exception
        }
      }
      catch {
        case e: Throwable => null
      }
      val t2 = System.currentTimeMillis
      val delta = t2 - t1
      delta >= 2
    }
  }
  case class LongWrapper(value:Long)
}
