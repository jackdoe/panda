package panda;
import java.util.BitSet;
public class NotSure {
    // works quite ok for 1m random keys, it gave false positive about 4% of the cases
    // TODO: can be greatly improved by adding more hash functions, and maybe bucketing the bitsets
    //  (went down from 5% with 2^24bits to 4% with 2^23bits, just by using 1 more hash function
    //   not sure if we should use more hash functions, considering http://hur.st/bloomfilter?n=1000000&p=0.04
    //   if we go to 5 hash functions it will use almost 6699669 bits anyway (almost 2^23))
    public static final int bitset_size = 1 << 23;
    public static final int mask = bitset_size - 1;
    public BitSet data = new BitSet(bitset_size);

    public static int l_32(long l) {
        return (int) (l ^ (l >>> 32)); // Long.hashCode()
    }
    public boolean maybe(long l) {
        return data.get(l_32(l) & mask) && data.get(Murmur3.hashLong(l) & mask);
    }
    public void add(long l) {
        data.set(Murmur3.hashLong(l) & mask);
        data.set(l_32(l) & mask);
    }
    public void clear() {
        data.clear();
    }
}
