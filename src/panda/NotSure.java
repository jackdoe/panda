package panda;
import java.util.BitSet;
public class NotSure {
    // works quite ok for 1m random keys, it gave false positive about 5% of the cases
    // since i intend to use this for about 500k keys, 2mb bitset is good enough
    public static final int bitset_size = 1 << 24;
    public static final int mask = bitset_size - 1;
    public BitSet data = new BitSet(bitset_size);
    public void clear() {
        data.clear();
    }
    public boolean maybe(long l) {
        return data.get(Murmur3.hashLong(l) & mask);
    }
    public void add(long l) {
        data.set(Murmur3.hashLong(l) & mask);
    }
    public String toString() {
        return data.toString();
    }
}
