package panda;
import java.util.BitSet;
public class NotSure {
    public static final int bitset_size = 1 << 20;
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
