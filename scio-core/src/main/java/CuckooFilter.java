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

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.*;
import java.util.stream.IntStream;

public class CuckooFilter<T> {

  public CuckooFilter(int numBuckets, int bucketSize, int fingerprintBits, int maxNumKicks,
                      Hasher<T> hasher) {
    this.maxNumKicks = maxNumKicks;
    Preconditions.checkArgument(numBuckets > 0);
    Preconditions.checkArgument(bucketSize > 0);
    Preconditions.checkArgument(fingerprintBits > 0 && fingerprintBits < 32);
    Preconditions.checkState(numBuckets == hasher.numBuckets);
    Preconditions.checkState(fingerprintBits == hasher.fingerprintBits);

    this.numBuckets = numBuckets;
    this.bucketSize = bucketSize;
    this.fingerprintBits = fingerprintBits;
    this.hasher = hasher;

    this.bucketWidth = bucketSize * fingerprintBits;
    this.numBits = numBuckets * bucketWidth;
    this.bitSet = new BitSet();
    this.random = new Random();
    this.entryIds = IntStream.range(0, bucketSize).toArray();
  }

  public CuckooFilter(int numBuckets, int bucketSize, int fingerprintBits, int maxNumKicks,
                      Funnel<T> funnel) {
    this(numBuckets, bucketSize, fingerprintBits, maxNumKicks,
        new Hasher<>(numBuckets, fingerprintBits, funnel));
  }

  private final int numBuckets;
  private final int bucketSize;
  private final int fingerprintBits;
  private final int maxNumKicks;
  private final Hasher<T> hasher;

  private final int bucketWidth;
  private final int numBits;
  private final BitSet bitSet;
  private final Random random;
  private final int[] entryIds;

  private final boolean debug = false;

  public static void main(String[] args) {
    CuckooFilter<CharSequence> f =
            new CuckooFilter<>(256, 4, 8, 100,
                    Funnels.stringFunnel(Charsets.UTF_8));

    System.out.println("Size: " + f.getSize());
    System.out.println("Capacity: " + f.getCapacity());

    List<String> inserted = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      String item = String.format("item-%08d", i);
      if (!f.insert(item)) {
        System.out.println("Insert stopped at item:" + i);
        break;
      }
      inserted.add(item);
      Preconditions.checkState(f.contains(item));
    }

    System.out.println("====================");
    int i = 0;
    for (String item : inserted) {
      if (!f.contains(item)) {
        System.out.println("Check failed at item: " + i);
      }
      i++;
      Preconditions.checkState(f.contains(item));
    }
  }

  /** Size of the Cuckoo Filter in bytes. */
  public int getSize() {
    return numBits / 8;
  }

  /** Capacity of the Cuckoo Filter, i.e. number of unique items that can be inserted. */
  public int getCapacity() {
    return numBuckets * bucketSize;
  }

  public boolean insert(T item) {
    Hash hash = hasher.hashItem(item);
    if (debug)
      System.out.println(
              String.format("INSERT %s\ti1: %d\ti2: %d\tfp: %d",
                      item.toString(), hash.i1, hash.i2, hash.fp));
    if (tryPutFingerprint(hash.i1, hash.fp)) {
      return true;
    }
    if (tryPutFingerprint(hash.i2, hash.fp)) {
      return true;
    }

    // must relocate existing items
    int i = random.nextBoolean() ? hash.i1 : hash.i2;
    int fp = hash.fp;
    Set<Integer> path = new HashSet<>();
    for (int n = 0; n < maxNumKicks; n++) {
//      System.out.println(path);
      int e = -1;
      for (int j = 0; j < bucketSize; j++) {
        int id = i * bucketSize + entryIds[j];
        if (path.contains(id)) {
          continue;
        }
        path.add(id);
        e = entryIds[j];
      }
      if (e == -1) {
        System.out.println("PATH COLLISION");
//        throw new RuntimeException("XXX");
        return false;
      }
      int kicked = getFingerprint(i, e);
      if (kicked == fp) {
        System.out.println("collision");
        return true;
      }
      putFingerprint(i, e, fp);
      fp = kicked;
      i = hasher.otherBucketId(i, fp);
      if (tryPutFingerprint(i, fp)) {
        System.out.println("Insert succeeded at attempt: " + n);
        return true;
      }
    }
    // hashtable is considered full
    System.out.println("Insert failed");
    return false;
  }

  public boolean contains(T item) {
    Hash hash = hasher.hashItem(item);
    if (debug)
      System.out.println(String.format("CONTAINS %8d %8d %8d", hash.i1, hash.i2, hash.fp));
    return containsFingerprint(hash.i1, hash.fp) || containsFingerprint(hash.i2, hash.fp);
  }

  // bucketId: [0, numBuckets)
  // return: [0, bucketSize), or -1 if not found
  private int getEmptyEntry(int bucketId) {
    int bucketOffset = bucketId * bucketWidth;
    for (int i = 0; i < bucketSize; i++) {
      int entryOffset = bucketOffset + fingerprintBits * i;
      boolean empty = true;
      for (int j = 0; j < fingerprintBits; j++) {
        if (bitSet.get(entryOffset + j)) {
          empty = false;
          break;
        }
      }
      if (empty) {
        return i;
      }
    }
    return -1;
  }

  private boolean tryPutFingerprint(int bucketId, int fp) {
    int entryId = getEmptyEntry(bucketId);
    if (entryId >= 0) {
      putFingerprint(bucketId, entryId, fp);
      return true;
    } else {
      return false;
    }
  }

  private boolean containsFingerprint(int bucketId, int fp) {
    for (int i = 0; i < bucketSize; i++) {
      if (getFingerprint(bucketId, i) == fp) {
        return true;
      }
    }
    return false;
  }

  private void putFingerprint(int bucketId, int entryId, int fp) {
    if (debug)
      System.out.println(String.format("PUT %8d %8d %8d", bucketId, entryId, fp));
    final int offset = bucketId * bucketWidth + fingerprintBits * entryId;
    for (int i = 0, j = fingerprintBits - 1; i < fingerprintBits; i++, j--) {
      bitSet.set(offset + i, ((fp >>> j) & 1) == 1);
    }
  }

  private int getFingerprint(int bucketId, int entryId) {
    final int offset = bucketId * bucketWidth + fingerprintBits * entryId;
    int fp = 0;
    for (int i = 0; i < fingerprintBits; i++) {
      int bit = bitSet.get(offset + i) ? 1 : 0;
      fp = (fp << 1) | bit;
    }
    if (debug)
      System.out.println(String.format("GET %8d %8d %8d", bucketId, entryId, fp));
    return fp;
  }

  ////////////////////////////////////////////////////////////////////////////////

  public static class Hasher<T> {
    private final int numBuckets;
    private final int fingerprintBits;
    private final Funnel<T> funnel;
    private final HashFunction fpFn;
    private final HashFunction hashFn;

    private final int fpMask;

    public Hasher(int numBuckets, int fingerprintBits, Funnel<T> funnel) {
      Preconditions.checkArgument(numBuckets > 0);
      Preconditions.checkArgument(fingerprintBits > 0 && fingerprintBits < 32);

      this.numBuckets = numBuckets;
      this.fingerprintBits = fingerprintBits;
      this.funnel = funnel;
      this.fpFn = Hashing.murmur3_32(1234);
      this.hashFn = Hashing.murmur3_32(5678);

      fpMask = (1 << fingerprintBits) - 1;
    }

    private int fingerprint(T item) {
      // FIXME: this should not be zero, otherwise no bits in the entry are unset
      int fp = fpFn.hashObject(item, funnel).asInt() & fpMask;
      // FIXME: hack to prevent 0 fingerprints
//      Preconditions.checkState(fp != 0);
      Preconditions.checkState(fp < (1 << fingerprintBits));
      return fp != 0 ? fp : 1;
    }

    public int hashFingerprint(int fp) {
      return (hashFn.hashInt(fp).asInt() & 0x7fffff) % numBuckets;
    }

    public Hash hashItem(T item) {
      int fp = fingerprint(item);
      // toInt truncates to 32 bits, AND makes it positive
      int i1 = (hashFn.hashObject(item, funnel).asInt() & 0x7fffff) % numBuckets;
      int i2 = otherBucketId(i1, fp);
      Preconditions.checkState(i1 < numBuckets);
      Preconditions.checkState(i2 < numBuckets);
//      Preconditions.checkState(i1 != i2);
      return new Hash(fp, i1, i2);
    }

    public int otherBucketId(int i1, int fp) {
      return (i1 ^ hashFingerprint(fp)) % numBuckets;
    }
  }

  ////////////////////////////////////////////////////////////////////////////////

  public static class Hash {
    private final int fp;
    private final int i1;
    private final int i2;

    public Hash(int fp, int i1, int i2) {
      this.fp = fp;
      this.i1 = i1;
      this.i2 = i2;
    }

    public int getFp() {
      return fp;
    }

    public int getI1() {
      return i1;
    }

    public int getI2() {
      return i2;
    }
  }
}
