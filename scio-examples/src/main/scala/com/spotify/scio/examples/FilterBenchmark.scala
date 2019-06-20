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
package com.spotify.scio.examples

import com.duprasville.guava.probably.{CuckooFilter, CuckooStrategies}
import com.google.common.base.Charsets
import com.google.common.hash.Funnels
import com.spotify.scio._
import com.spotify.scio.util.{BloomFilter, BloomFilterAggregator}
import com.spotify.scio.values.SCollection
import com.twitter.algebird.Hash128.stringHash
import org.apache.beam.sdk.io.GenerateSequence
import org.apache.beam.sdk.testing.PAssert

object FilterBenchmark {

  val million = 1000000
  val n = 10
  val numEntries = n * million
  val fpp = 0.05

  val dfArgs = Array[String](
    "--runner=DataflowRunner",
    "--project=scio-playground",
    "--autoscalingAlgorithm=NONE",
    "--numWorkers=4"
  )

  val check = false

  def main(args: Array[String]): Unit = {
    bf()
    cf()
  }

  def makeHits(sc: ScioContext): SCollection[String] =
    sc.customInput("hits", GenerateSequence.from(0).to(n * million))
      .map(i => "item-%012d".format(i))

  def makeMisses(sc: ScioContext): SCollection[String] =
    sc.customInput("misses", GenerateSequence.from(n * million).to(2 * n * million))
      .map(i => "item-%012d".format(i))

  def bf(): Unit = {
    val (sc, _) = ContextAndArgs(dfArgs)
    sc.setAppName("BloomFilter")
    val hits = makeHits(sc)

    val width = BloomFilter.optimalWidth(numEntries, fpp).get
    val numHashes = BloomFilter.optimalNumHashes(numEntries, width)
    val a = BloomFilterAggregator[String](numHashes, width)
    println(s"BloomFilter: numHashes=${numHashes} width=${width}")

    val si = hits.aggregate(a.monoid.zero)(_ += _, _ ++= _).asSingletonSideInput(a.monoid.zero)

    val pos = hits
      .withSideInputs(si)
      .map { (x, c) =>
        c(si).maybeContains(x)
      }
      .toSCollection
      .reduce(_ && _)

    if (check) {
      PAssert.thatSingleton(pos.internal).isEqualTo(true)
    }

    val neg = makeMisses(sc)
      .withSideInputs(si)
      .map { (x, c) =>
        val fp = if (c(si).maybeContains(x)) 1 else 0
        (fp, 1)
      }
      .toSCollection
      .sum
      .map(t => t._1.toDouble / t._2)

    if (check) {
      PAssert
        .thatSingleton(neg.internal)
        .satisfies((input: Double) => {
          require(input <= fpp)
          null
        })
    }

    sc.close()
  }

  def cf(): Unit = {
    val (sc, _) = ContextAndArgs(dfArgs)
    sc.setAppName("CuckooFilter")
    val hits = makeHits(sc)

    val strategy = CuckooStrategies.MURMUR128_BEALDUPRAS_32.strategy()
    val numEntriesPerBucket = CuckooFilter.optimalEntriesPerBucket(fpp)
    val numBitsPerEntry = CuckooFilter.optimalBitsPerEntry(fpp, numEntriesPerBucket)
    val numBuckets = CuckooFilter.optimalNumberOfBuckets(numEntries, numEntriesPerBucket)

    val si = hits
      .map(strategy.prehash(_, funnel, numBuckets, numBitsPerEntry))
      .groupBy(_ => ())
      .map {
        case (_, hashes) =>
          val cf = CuckooFilter.create(funnel, numEntries, fpp)
          hashes.foreach(cf.add)
          cf
      }
      .asSingletonSideInput

    val pos = hits
      .withSideInputs(si)
      .map { (x, c) =>
        c(si).contains(x)
      }
      .toSCollection
      .reduce(_ && _)

    if (check) {
      PAssert.thatSingleton(pos.internal).isEqualTo(true)
    }

    val neg = makeMisses(sc)
      .withSideInputs(si)
      .map { (x, c) =>
        val fp = if (c(si).contains(x)) 1 else 0
        (fp, 1)
      }
      .toSCollection
      .sum
      .map(t => t._1.toDouble / t._2)

    if (check) {
      PAssert
        .thatSingleton(neg.internal)
        .satisfies((input: Double) => {
          require(input <= fpp)
          null
        })
    }

    sc.close()
  }

  val funnel = Funnels.stringFunnel(Charsets.UTF_8)
}
