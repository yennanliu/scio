package com.duprasville.guava.probably;

import com.google.common.hash.Funnel;

import java.io.Serializable;

public class CuckooHasher<T> implements Serializable {

  private final CuckooStrategy strategy;
  private final Funnel<? super T> funnel;
  private final CuckooTable table;

  public CuckooHasher(CuckooStrategy strategy, Funnel<? super T> funnel,
                      long numBuckets, int numEntriesPerBucket, int numBitsPerEntry) {
    this.strategy = strategy;
    this.funnel = funnel;
    this.table = new CuckooTable(null, 0, 0, numBuckets, numEntriesPerBucket, numBitsPerEntry);
  }

  public Hash hash(T object) {
    return strategy.hash(object, funnel, table);
  }

  public static class Hash {
    private final int fingerprint;
    private final long index;

    public Hash(int fingerprint, long index) {
      this.fingerprint = fingerprint;
      this.index = index;
    }

    public int getFingerprint() {
      return fingerprint;
    }

    public long getIndex() {
      return index;
    }
  }

}
