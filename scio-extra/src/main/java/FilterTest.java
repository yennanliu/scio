import com.duprasville.guava.probably.CuckooFilter;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;

public class FilterTest {
  public static void main(String[] args) {
    long capacity = 10000000;
    double fpp = 0.05;
    Funnel<CharSequence> funnel = Funnels.stringFunnel(Charsets.UTF_8);
    long t;

    System.out.println("BloomFilter");
    BloomFilter<CharSequence> bf = BloomFilter.create(funnel, capacity, fpp);
    t = System.currentTimeMillis();
    for (int i = 0; i < capacity; i++) {
      bf.put(String.valueOf(i));
    }
    System.out.println(System.currentTimeMillis() - t);

    t = System.currentTimeMillis();
    for (int i = 0; i < capacity * 2; i++) {
      boolean r = bf.mightContain(String.valueOf(i));
      if (i < capacity) {
        Preconditions.checkState(r);
      }
    }
    System.out.println(System.currentTimeMillis() - t);

    System.out.println("CuckooFilter");
    CuckooFilter<CharSequence> cf = CuckooFilter.create(funnel, capacity, fpp);
    t = System.currentTimeMillis();
    for (int i = 0; i < capacity; i++) {
      cf.add(String.valueOf(i));
    }
    System.out.println(System.currentTimeMillis() - t);

    t = System.currentTimeMillis();
    for (int i = 0; i < capacity * 2; i++) {
      boolean r = cf.contains(String.valueOf(i));
      if (i < capacity) {
        Preconditions.checkState(r);
      }
    }
    System.out.println(System.currentTimeMillis() - t);
  }
}
