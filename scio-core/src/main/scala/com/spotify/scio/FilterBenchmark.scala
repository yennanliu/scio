package com.spotify.scio

import com.duprasville.guava.probably.CuckooFilter
import com.google.common.base.Charsets
import com.google.common.hash.Funnels
import com.spotify.scio.util.{BloomFilter, BloomFilterMonoid, MutableBFInstance}

object FilterBenchmark {
  val million = 1000000
  val n = 1 * million
  val fpp = 0.05

  def main(args: Array[String]): Unit = {

    args(0) match {
      case "bf" => bf()
      case "cf" => cf()
    }
  }

  def bf(): Unit = {
    val w = BloomFilter.optimalWidth(n, fpp).get
    val h = BloomFilter.optimalNumHashes(n, w)
    import com.twitter.algebird.Hash128.stringHash
    var f = BloomFilterMonoid(h, w).zero
    println(f)

    val buildStart = System.currentTimeMillis()
    var i = 0
    while (i < n) {
      f = f += i.toString
      i += 1
    }
    println("BUILD: " + (System.currentTimeMillis() - buildStart))
    println("SIZE: " + f.asInstanceOf[MutableBFInstance[String]].bits.size())

    val lookupStart = System.currentTimeMillis()
    i = 0
    while (i < n) {
      require(f.contains(i.toString).isTrue)
      i += 1
    }
    println("LOOKUP: " + (System.currentTimeMillis() - lookupStart))
  }

  def cf(): Unit = {
    val f = CuckooFilter.create(Funnels.stringFunnel(Charsets.UTF_8), n, fpp)
    println(f)

    val buildStart = System.currentTimeMillis()
    var i = 0
    while (i < n) {
      require(f.add(i.toString))
      i += 1
    }
    println("BUILD: " + (System.currentTimeMillis() - buildStart))

    val lookupStart = System.currentTimeMillis()
    i = 0
    while (i < n) {
      require(f.contains(i.toString))
      i += 1
    }
    println("LOOKUP: " + (System.currentTimeMillis() - lookupStart))
  }
}
