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

import java.nio.charset.Charset

import com.datastax.bdp.util.{QuotientFilter => JQF}
import com.duprasville.guava.probably.{CuckooFilter => JCF}
import com.google.common.base.Charsets
import com.google.common.hash.{Funnel, Funnels}
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.twitter.algebird.Hash128

/*
TODO:
- coders
- property-based testing
- optimized build methods for SCollections
- build from repetitive (list) or unique items (set)
 */

trait ProbFilter[T] {
  val capacity: Long // maximum number of items allowed
  val size: Long // number of items inserted
  val bytes: Long // size in bytes when serialized
  val fpp: Double // false positive prob
  val fnp: Double // false negative prob
  def hasFalsePos: Boolean = fpp > 0.0
  def hasFalseNeg: Boolean = fnp > 0.0

  // may return both false positive and negative depending on `fpp` and `fnp`
  def contains(item: T): Boolean
}

trait ProbFilterBuilder[PF[T] <: ProbFilter[T]] {

  /** Build a [[ProbFilter]] from an [[Iterable]]. */
  def build[T](data: Iterable[T])(implicit hash: Hash[T]): PF[T]

  /**
   * Build a [[ProbFilter]] from an [[SCollection]].
   *
   * Naive implementation that groups everything to a single iterable and build sequentially.
   */
  def build[T: Coder](
    data: SCollection[T]
  )(implicit hash: Hash[T], coder: Coder[PF[T]]): SCollection[PF[T]] =
    data
      .groupBy(_ => ())
      .map(kv => build(kv._2))
}

trait ProbFilterCompanion[PF[T] <: ProbFilter[T]] {
  def builder(capacity: Long, fpp: Double): ProbFilterBuilder[PF]
  def coder[T]: Coder[PF[T]]
}

case class Hash[T](algebird: Hash128[T], guava: Funnel[T], toBytes: T => Array[Byte])

object Hash {
  implicit val intHash: Hash[Int] =
    Hash(Hash128.intHash, Funnels.integerFunnel().asInstanceOf[Funnel[Int]], null)
  implicit val longHash: Hash[Long] =
    Hash(Hash128.longHash, Funnels.longFunnel().asInstanceOf[Funnel[Long]], null)
  implicit val bytesHash: Hash[Array[Byte]] =
    Hash(Hash128.arrayByteHash, Funnels.byteArrayFunnel(), null)
  implicit val stringHash: Hash[String] =
    Hash(
      Hash128.stringHash,
      Funnels.stringFunnel(Charsets.UTF_8).asInstanceOf[Funnel[String]],
      _.getBytes(Charset.defaultCharset())
    )
}

////////////////////////////////////////////////////////////////////////////////

import com.twitter.{algebird => t}

case class BloomFilter[T] private (bf: t.BF[T], capacity: Long) extends ProbFilter[T] {
  override val size: Long = bf.size.estimate
  override val bytes: Long = scala.math.ceil(bf.width / 8).toInt
  override val fpp: Double =
    if (bf.density > 0.95)
      1.0
    else
      scala.math
        .pow(1 - scala.math.exp(-bf.numHashes * bf.size.estimate * 1.1 / bf.width), bf.numHashes)
  override val fnp: Double = 0.0

  override def contains(item: T): Boolean = bf.maybeContains(item)
}

object BloomFilter extends ProbFilterCompanion[BloomFilter] {
  override def builder(capacity: Long, fpp: Double): ProbFilterBuilder[BloomFilter] =
    new ProbFilterBuilder[BloomFilter] {
      override def build[T](data: Iterable[T])(implicit hash: Hash[T]): BloomFilter[T] = {
        val monoid = t.BloomFilter[T](capacity.toInt, fpp)(hash.algebird)
        val bf = monoid.create(data.iterator)
        BloomFilter(bf, capacity)
      }
    }

  override def coder[T]: Coder[BloomFilter[T]] = ???
}

import com.google.common.{hash => g}

case class GBloomFilter[T] private (bf: g.BloomFilter[T], capacity: Long) extends ProbFilter[T] {
  override val size: Long = bf.approximateElementCount()
  override val bytes: Long = {
    var n = 0L
    val os = new java.io.OutputStream {
      override def write(b: Int): Unit = n += 1
    }
    bf.writeTo(os)
    n
  }
  override val fpp: Double = bf.expectedFpp()
  override val fnp: Double = 0.0

  override def contains(item: T): Boolean = bf.mightContain(item)
}

object GBloomFilter extends ProbFilterCompanion[GBloomFilter] {
  override def builder(capacity: Long, fpp: Double): ProbFilterBuilder[GBloomFilter] =
    new ProbFilterBuilder[GBloomFilter] {
      override def build[T](data: Iterable[T])(implicit hash: Hash[T]): GBloomFilter[T] = {
        val bf = g.BloomFilter.create(hash.guava, capacity, fpp)
        data.foreach(bf.put)
        GBloomFilter(bf, capacity)
      }
    }

  override def coder[T]: Coder[GBloomFilter[T]] = ???
}
////////////////////////////////////////////////////////////////////////////////

/**
 * A conventional Cuckoo Filter.
 *
 * Insertions may fail if the filter is saturated.
 */
case class CuckooFilter[T] private (cf: JCF[T]) extends ProbFilter[T] {
  override val capacity: Long = cf.capacity()
  override val size: Long = cf.sizeLong()
  override val fpp: Double = cf.currentFpp()
  override val fnp: Double = 0.0
  override val bytes: Long = cf.bytes()

  override def contains(item: T): Boolean = cf.contains(item)
}

object CuckooFilter extends ProbFilterCompanion[CuckooFilter] {
  override def builder(capacity: Long, fpp: Double): ProbFilterBuilder[CuckooFilter] =
    new ProbFilterBuilder[CuckooFilter] {
      override def build[T](data: Iterable[T])(implicit hash: Hash[T]): CuckooFilter[T] = {
        val cf = JCF.create(hash.guava, capacity, fpp)
        data.foreach { i =>
//          require(cf.add(i), s"Failed to add item at size ${cf.sizeLong()}")
          if (!cf.add(i)) println(s"Failed to add item at size ${cf.sizeLong()}")
        }
        CuckooFilter(cf)
      }
    }

  override def coder[T]: Coder[CuckooFilter[T]] = ???
}

import com.github.mgunlogson.{cuckoofilter4j => c4j}

case class CuckooFilter4J[T] private (cf: c4j.CuckooFilter[T]) extends ProbFilter[T] {
  override val capacity: Long = cf.getActualCapacity
  override val size: Long = cf.getCount
  override val fpp: Double = -1.0
  override val fnp: Double = 0.0
  override val bytes: Long = -1

  override def contains(item: T): Boolean = cf.mightContain(item)
}

object CuckooFilter4J extends ProbFilterCompanion[CuckooFilter4J] {
  override def builder(capacity: Long, fpp: Double): ProbFilterBuilder[CuckooFilter4J] =
    new ProbFilterBuilder[CuckooFilter4J] {
      override def build[T](data: Iterable[T])(implicit hash: Hash[T]): CuckooFilter4J[T] = {
        val cf = new c4j.CuckooFilter.Builder[T](hash.guava, capacity)
          .withFalsePositiveRate(fpp)
          .build()
        data.foreach { i =>
          //          require(cf.add(i), s"Failed to add item at size ${cf.sizeLong()}")
          if (!cf.put(i)) println(s"Failed to add item at size ${cf.getCount()}")
        }
        CuckooFilter4J(cf)
      }
    }

  override def coder[T]: Coder[CuckooFilter4J[T]] = ???
}

object TwoSidedCuckooFilter extends ProbFilterCompanion[TwoSidedCuckooFilter] {
  override def builder(capacity: Long, fpp: Double): ProbFilterBuilder[TwoSidedCuckooFilter] =
    new ProbFilterBuilder[TwoSidedCuckooFilter] {
      override def build[T](data: Iterable[T])(implicit hash: Hash[T]): TwoSidedCuckooFilter[T] = {
        val cf = JCF.create(hash.guava, capacity, fpp)
        var failures = 0
        data.foreach { i =>
          if (!cf.add(i)) {
            failures += 1
          }
        }
        val fnp = failures.toDouble / data.size
        TwoSidedCuckooFilter(cf, fnp)
      }
    }

  override def coder[T]: Coder[TwoSidedCuckooFilter[T]] = ???
}

/**
 * A two-sided Cuckoo Filter.
 *
 * A filter is two-sided if it can have both false positives and false negatives.
 * False negatives happen when there are insertion failures.
 */
case class TwoSidedCuckooFilter[T](cf: JCF[T], fnp: Double) extends ProbFilter[T] {
  override val capacity: Long = cf.capacity()
  override val size: Long = cf.sizeLong()
  override val fpp: Double = cf.currentFpp()
  override val bytes: Long = cf.bytes()

  override def contains(item: T): Boolean = cf.contains(item)
}

object OneSidedCuckooFilter extends ProbFilterCompanion[OneSidedCuckooFilter] {
  override def builder(capacity: Long, fpp: Double): ProbFilterBuilder[OneSidedCuckooFilter] =
    new ProbFilterBuilder[OneSidedCuckooFilter] {
      override def build[T](data: Iterable[T])(implicit hash: Hash[T]): OneSidedCuckooFilter[T] = {
        val cf = JCF.create(hash.guava, capacity, fpp)
        val b = Set.newBuilder[T]
        data.foreach { i =>
          if (!cf.add(i)) {
            b += i
          }
        }

        val failures = b.result()
        val overflow = if (failures.isEmpty) {
          None
        } else {
          val f = JCF.create(hash.guava, (failures.size * 1.2).toLong, fpp)
          failures.foreach(i => assert(f.add(i), "Failed to add to overflow CuckooFilter"))
          Some(f)
        }
        OneSidedCuckooFilter(cf, overflow)
      }
    }

  override def coder[T]: Coder[OneSidedCuckooFilter[T]] = ???
}

/**
 * A one-sided Cuckoo Filter.
 *
 * A filter is one-sided if it can only have both false positives and never false negatives.
 * Since Cuckoo Filter cannot guarantee successful insertion, we overflow failed items to a second
 * filter.
 */
case class OneSidedCuckooFilter[T](cf: JCF[T], overflow: Option[JCF[T]]) extends ProbFilter[T] {
  override val capacity: Long = cf.capacity() + overflow.map(_.capacity()).getOrElse(0L)
  override val size: Long = cf.sizeLong() + overflow.map(_.sizeLong()).getOrElse(0L)
  override val fpp: Double = cf.currentFpp() + overflow.map(_.currentFpp()).getOrElse(0.0)
  override val fnp: Double = 0.0
  override val bytes: Long = cf.bytes() + 1 + overflow.map(_.bytes()).getOrElse(0L)

  override def contains(item: T): Boolean = cf.contains(item) || overflow.exists(_.contains(item))
}

////////////////////////////////////////////////////////////////////////////////

object QuotientFilter extends ProbFilterCompanion[QuotientFilter] {
  override def builder(capacity: Long, fpp: Double): ProbFilterBuilder[QuotientFilter] =
    new ProbFilterBuilder[QuotientFilter] {
      override def build[T](data: Iterable[T])(implicit hash: Hash[T]): QuotientFilter[T] = {
        val qf = JQF.create(capacity, fpp)
        data.foreach(i => qf.add(hash.toBytes(i)))
        QuotientFilter(qf, hash.toBytes)
      }
    }

  override def coder[T]: Coder[QuotientFilter[T]] = ???
}

case class QuotientFilter[T](qf: JQF, toBytes: T => Array[Byte]) extends ProbFilter[T] {
  override val capacity: Long = qf.capacity()
  override val size: Long = qf.size()
  override val bytes: Long = qf.bytes()
  override val fpp: Double = qf.fpp()
  override val fnp: Double = 0.0

  override def contains(item: T): Boolean = qf.maybeContains(toBytes(item))
}
