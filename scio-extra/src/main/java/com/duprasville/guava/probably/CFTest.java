package com.duprasville.guava.probably;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CFTest {
  private static Funnel<CharSequence> funnel = Funnels.stringFunnel(Charsets.UTF_8);

  public static void main(String[] args) {
    long[] capacities = new long[]{1000, 10000, 100000, 1000000};
    double[] fpps = new double[]{0.05, 0.01, 0.032, 0.5};
    double[] margins = new double[]{1.0, 1.05, 1.1, 1.2};

    for (long capacity : capacities) {
      System.out.println("capacity=" + capacity);
      for (double margin : margins) {
        System.out.println("margin=" + margin);
        for (double fpp : fpps) {
          test(capacity, fpp, margin);
        }
      }
    }
  }

  private static void test() {
    int n = 1000;

    CuckooHasher<CharSequence> hasher = CuckooFilter.createHasher(funnel, n);

    List<String> strings = IntStream.range(0, n)
        .mapToObj(String::valueOf)
        .collect(Collectors.toList());

    List<CuckooHasher.Hash> hashes = strings.stream()
        .map(hasher::hash)
        .collect(Collectors.toList());

    CuckooFilter<CharSequence> cf1 = CuckooFilter.create(funnel, n);
    hashes.forEach(cf1::add);

    CuckooFilter<CharSequence> cf2 = CuckooFilter.create(funnel, n);
    strings.forEach(cf2::add);

    for (String s : strings) {
      System.out.println(cf1.contains(s) + " " + cf2.contains(s));
    }
  }

  private static void test(long capacity, double fpp, double margin) {
    CuckooFilter<CharSequence> cf = CuckooFilter.create(funnel, (long) (capacity * margin), fpp);
    int addErrs = 0;
    Set<Integer> failedAdds = new HashSet<>();
    for (int i = 0; i < capacity; i++) {
      boolean r = cf.add(String.valueOf(i));
      if (!r) {
        addErrs++;
        failedAdds.add(i);
      }
    }

    int lookupErrs = 0;
    int fps = 0;
    for (int i = 0; i < capacity; i++) {
      if (!failedAdds.contains(i) && !cf.contains(String.valueOf(i))) {
        lookupErrs++;
      }
      if (cf.contains(String.valueOf(i + capacity))) {
        fps++;
      }
    }

    System.out.println(String.format(
        "capacity=%d fpp=%f margin=%f fppActual=%f addErrs=%d lookupErrs=%d",
        capacity, fpp, margin, (double) fps / capacity, addErrs, lookupErrs));
  }
}
