/*
 * Copyright 2019 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.spotify.scio.extra.filter

// scalastyle:off
object ProbFilterTest {
  private val targetFpp = 0.05

  def test[PF[T] <: ProbFilter[T]](b: ProbFilterBuilder[PF], targetCapacity: Int): Unit = {
    val data = (1 to targetCapacity).map(_.toString)
    var t = System.currentTimeMillis()
    val f = b.build(data)
    var duration = System.currentTimeMillis() - t
    println("=" * 50)
    println(s"Testing ${f.getClass.getSimpleName}")
    println(s"target capacity=$targetCapacity")
    println(s"target fpp=$targetFpp")
    println(s"bytes=${f.bytes}")
    println(s"capacity=${f.capacity}")
    println(s"size=${f.size}")
    println(s"fpp=${f.fpp}")
    println(s"fnp=${f.fnp}")
    println(s"Build: $duration ms")

    (1 to 10).foreach { _ =>
      t = System.currentTimeMillis()
      var fn = 0
      data.foreach { s =>
        if (!f.contains(s)) {
          fn += 1
        }
      }
      var fp = 0
      (targetCapacity + 1 to targetCapacity * 2).map(_.toString).foreach { s =>
        if (f.contains(s)) {
          fp += 1
        }
      }
      duration = System.currentTimeMillis() - t
      val fpp = fp.toDouble / targetCapacity
      val fnp = fn.toDouble / f.size
      println(s"actual fpp=$fpp")
      println(s"actual fnp=$fnp")
      println(s"Lookup: $duration ms avg: ${duration.toDouble * 1000000 / targetCapacity / 2} ns")
    }
  }

  def main(args: Array[String]): Unit =
    testFilters()

  def testFilters(): Unit = {
//    for (cap <- Seq(1000, 100000, 10000000)) {
    for (cap <- Seq(1000000)) {
      test(BloomFilter.builder(cap, targetFpp), cap)
      test(GBloomFilter.builder(cap, targetFpp), cap)
      test(CuckooFilter.builder(cap, targetFpp), cap)
      test(CuckooFilter4J.builder(cap, targetFpp), cap)
//      test(OneSidedCuckooFilter.builder(cap, targetFpp), cap)
//      test(TwoSidedCuckooFilter.builder(cap, targetFpp), cap)
      test(QuotientFilter.builder(cap, targetFpp), cap)
    }
  }

  def testCuckoo(): Unit = {
    import com.duprasville.guava.probably.{CuckooFilter => JCF}
    import com.google.common.base.Charsets
    import com.google.common.hash.Funnels
    for (cap <- Seq(100, 10000, 1000000)) {
      (1 to 10).foreach { _ =>
        val cf = JCF.create(Funnels.stringFunnel(Charsets.UTF_8), cap, targetFpp)
        var failures = 0
        (1 to cap).foreach { _ =>
          val item = java.util.UUID.randomUUID().toString
          if (!cf.add(item)) {
            failures += 1
          }
        }
        println(s"cap=$cap failures=$failures rate=${failures.toDouble / cap}")
      }
    }
  }
}
