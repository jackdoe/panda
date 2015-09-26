package panda;
import java.util.function.BiConsumer;
public class KV {
    // XXX: bloom
    public static final long NO_MORE = Long.MAX_VALUE;
    StoredLongArray keys;
    StoredLongArray offsets;
    StoredLongArray values;
    public KV(String path) throws Exception {
        keys = new StoredLongArray(path + ".keys");
        values = new StoredLongArray(path + ".values");
        offsets = new StoredLongArray(path + ".offsets");
    }
    public long get_single(long key) {
        int idx = keys.bsearch(key);
        if (idx >= 0) {
            long value_range = offsets.get(idx);
            int from = (int)(value_range >> 32);
            return values.get(from);
        }
        return NO_MORE;
    }

    public long[] get(long key) {
        int idx = keys.bsearch(key);
        if (idx >= 0) {
            long value_range = offsets.get(idx);
            int from = (int)(value_range >> 32);
            int len   = (int)(value_range & 0xFFFFFFFF);
            return values.slice(from,len);
        }
        return null;
    }
    public void validate_key(long key) {
        if (key == NO_MORE)
            throw new RuntimeException("cannot store(reserved) " + key);
    }
    public void append(long key, long v) {
        validate_key(key);
        keys.append(key);
        long from = (long) values.append(v);
        offsets.append(from << 32L | 1L);
    }

    public void append(long key, long[] v) {
        append(key,v,0,v.length);
    }

    public void append(long key, long[] v,int off, int len) {
        validate_key(key);
        if (len > 0) {
            keys.append(key);
            long from = (long) values.append(v,off,len);
            offsets.append(from << 32L | len);
        }
    }

    public synchronized void flush() throws Exception {
        values.flush();
        offsets.flush();
        keys.flush(); // must flush keys last, if anything else fails before that, we wont use the keys anyway
    }

    public KV merge(KV... kvs) throws Exception {
        return merge_into(this,kvs);
    }

    public static KV merge_into(KV into, KV... kvs) throws Exception {
        for(KV sub : kvs) {
            if (sub.keys.length == 0)
                throw new Exception(sub.keys.toString() + " has 0 keys");
            sub.keys.unsafe_index_iter = 0;
        }
        long current_smallest_key = NO_MORE;
        long prev_smallest_key = NO_MORE;
    FOREVER:
        for(int j = 0; j < 100; j++) {
            long next_smallest_key = NO_MORE;

            for (KV sub : kvs) {
                long cur = sub.keys.unsafe_index_iter_current();
                if (cur == current_smallest_key) cur = sub.keys.unsafe_index_iter_next();
                if (cur < next_smallest_key) next_smallest_key = cur;
            }

            if (next_smallest_key == NO_MORE)
                break FOREVER;

            // dedup
            if (next_smallest_key == prev_smallest_key)
                continue;

            current_smallest_key = next_smallest_key;

        INSERT:
            for (KV sub : kvs) {
                // the first KV that has the key, inseart is
                if (sub.keys.unsafe_index_iter_current() == current_smallest_key) {
                    long off = sub.offsets.get(sub.keys.unsafe_index_iter);
                    into.append(sub.keys.get(sub.keys.unsafe_index_iter), sub.values.buffer, (int)(off >> 32L), (int)(off & 0xFFFFFFFF));
                    break INSERT;
                }
            }
            prev_smallest_key = current_smallest_key;
        }

        return into;
    }

    void reset() {
        keys.reset();
        offsets.reset();
        values.reset();
    }
    public int count() {
        return keys.length;
    }
    public void reload() throws Exception {
        keys.reload();
        offsets.reload();
        values.reload();
    }
    public void forEach(BiConsumer<Long, long[]> consumer) {
        for (int i = 0; i < keys.length; i++) {
            long value_range = offsets.get(i);
            int from = (int)(value_range >> 32);
            int len   = (int)(value_range & 0xFFFFFFFF);
            consumer.accept(keys.get(i), values.slice(from,len));
        }
    }
}
