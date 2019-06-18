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
package com.spotify.scio.util

import java.util.{BitSet => JBitSet}

import com.google.common.hash.Hashing

/**
 * f: fingerprint length in bits
 * b: number of entries per bucket - bucket size
 * m: number of buckets
 * n: number of items
 */
object CuckooFilter {
  def capacity(numBuckets: Long, bucketSize: Int): Long =
    numBuckets * bucketSize

  def size(numBuckets: Long, bucketSize: Int, fingerprintBits: Int): Long =
    numBuckets * bucketSize * fingerprintBits / 8

  def main(args: Array[String]): Unit = {
    val numBuckets = 100000000
    val bucketSize = 4
    val fingerprintBits = 8
    println(s"numBuckets = $numBuckets")
    println(s"bucketSize = $bucketSize")
    println(s"fingerprintBits = $fingerprintBits")
    println(s"capacity = ${capacity(numBuckets, bucketSize)}")
    println(s"size = ${size(numBuckets, bucketSize, fingerprintBits)} bytes")

    CuckooFilter(10, 4, 8)
  }

}

case class CuckooFilter[A](numBuckets: Int, bucketSize: Int, fingerprintBits: Int) {
  require(numBuckets > 0)
  require(bucketSize > 0)
  require(fingerprintBits > 0 && fingerprintBits < 32)

  private val bucketWidth = bucketSize * fingerprintBits
  private val numBits = numBuckets * bucketWidth
  private val bitSet = new JBitSet(numBits)

  val hash = CuckooHash(numBuckets, fingerprintBits)

  def insert(bytes: Array[Byte]): Boolean = {
    val h = hash.hashes(bytes)
    true
  }

  {
    println(getEmptySlot(0))
    (0 until (bucketSize - 1) * fingerprintBits).foreach(bitSet.set)
    println(getEmptySlot(0))
    println(getEmptySlot(1))
    (0 until (bucketSize - 1) * fingerprintBits).map(_ + bucketWidth).foreach(bitSet.set)
    println(getEmptySlot(1))
  }

  // bucket: [0, numBuckets)
  // return: [0, bucketSize), or -1 if not found
  private def getEmptySlot(bucket: Int): Int = {
    val bucketOff = bucket * bucketWidth
    var slot = 0
    var found = -1
    while (slot < bucketSize && found == -1) {
      val slotOff = bucketOff + slot * fingerprintBits
      if ((0 until fingerprintBits).forall(i => !bitSet.get(slotOff + i))) {
        found = slot
      }
      slot += 1
    }
    found
  }

//  @inline
//  private def getBit(bucket: Int, slot: Int): =
//    bitSet.get(bucket * bucketWidth + slot * )
}

case class CuckooHashes(fp: Int, i1: Int, i2: Int)

case class CuckooHash(numBuckets: Int, fingerprintBits: Int) {
  private val fpMask = (1 << fingerprintBits) - 1
  private val fpHash = Hashing.murmur3_32(1234)
  private val hash = Hashing.murmur3_32(5678)

  // truncate to fingerprintBits
  def fingerprint(bytes: Array[Byte]): Int =
    fpHash.hashBytes(bytes).asInt() & fpMask

  def hashes(bytes: Array[Byte]): CuckooHashes = {
    val fp = fingerprint(bytes)
    // toInt truncates to 32 bites, & 0x7fffffff makes it positive
    val i1 = hash.hashBytes(bytes).asInt() & 0x7fffffff % numBuckets
    val i2 = i1 ^ hashFingerprint(fp)
    CuckooHashes(fp, i1, i2)
  }

  def hashFingerprint(fp: Int): Int = hash.hashInt(fp).asInt() % numBuckets

}
