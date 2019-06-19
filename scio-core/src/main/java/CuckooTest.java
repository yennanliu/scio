import com.duprasville.guava.probably.CuckooFilter;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.hash.Funnels;

import java.util.ArrayList;
import java.util.List;

public class CuckooTest {
  public static void main(String[] args) {
    int capacity = 1000000;
    CuckooFilter<CharSequence> cf =
        CuckooFilter.create(Funnels.stringFunnel(Charsets.UTF_8), capacity);

    System.out.println(cf);
    System.out.printf("capacity=%dd fpp=%f\n", cf.capacity(), cf.fpp());

    List<String> inserted = new ArrayList<>();
    for (int i = 0; i < capacity; i++) {
      String item = String.format("item-%08d", i);
      if (cf.add(item)) {
        inserted.add(item);
        Preconditions.checkState(cf.contains(item));
        if ((i + 1) % 10000 == 0) {
          System.out.printf("PROGRESS %8d size=%d load=%f fpp=%f\n",
              i + 1, cf.size(), (double) cf.size() / cf.capacity(), cf.currentFpp());
        }
      } else {
        System.out.printf("ADD FAILED at %d size=%d load=%f fpp=%f\n",
            i + 1, cf.size(), (double) cf.size() / cf.capacity(), cf.currentFpp());
      }
    }

    for (String item : inserted) {
      Preconditions.checkState(cf.contains(item));
    }
  }
}
