package panda;
import java.util.function.BiConsumer;
import java.nio.*;
import java.nio.file.*;
import java.io.*;
import java.util.Random;
// nothing here is thread safe, but certain assumptions can be made
// get() is fairly safe, and also you can append() and get() in the same time

// you can not append() from multiple threads, there is no synchronization
// at all, it is expected the caller to do so.
public class KV {
    // XXX: bloom
    public static final String SA_KEYS = "sa.keys";
    public static final String SA_OFFSETS = "sa.offsets";
    public static final String SA_VALUES = "sa.values";

    public static final FileSystem fs = FileSystems.getDefault();
    public static final long NO_MORE = Long.MAX_VALUE;
    StoredLongArray keys;
    StoredLongArray offsets;
    StoredLongArray values;
    NotSure not_sure;
    public Path path;

    public Path create_new_directory() throws Exception {
        // XXX: FileAlreadyExistsException
        String s = path.toFile().toString() + "_" + System.nanoTime() + "_" + Math.abs(new Random(System.nanoTime()).nextLong());
        Path p = fs.getPath(s);
        Files.createDirectories(p);
        return p;
    }

    public Path get_path_for(String from, String suffix) {
        return fs.getPath(from,suffix);
    }

    public Path get_path_for(Path from, String suffix) {
        return get_path_for(from.toFile().toString(),suffix);
    }

    public KV(String _path) throws Exception {
        path = fs.getPath(_path);
        if (!path.toFile().exists()) {
            Path tmp = create_new_directory();
            Files.createSymbolicLink(path,tmp);
        }

        keys = new StoredLongArray(get_path_for(_path,SA_KEYS));
        values = new StoredLongArray(get_path_for(_path,SA_VALUES));
        offsets = new StoredLongArray(get_path_for(_path,SA_OFFSETS));
        not_sure = new NotSure();
    }

    public long get_single(long key) {
        if (not_sure.maybe(key)) {
            int idx = keys.bsearch(key);
            if (idx >= 0) {
                long value_range = offsets.get(idx);
                int from = (int)(value_range >> 32);
                return values.get(from);
            }
        }
        return NO_MORE;
    }

    public long[] get(long key) {
        if (not_sure.maybe(key)) {
            int idx = keys.bsearch(key);
            if (idx >= 0) {
                long value_range = offsets.get(idx);
                int from = (int)(value_range >> 32);
                int len   = (int)(value_range & 0xFFFFFFFF);
                return values.slice(from,len);
            }
        }
        return null;
    }

    public void validate_key(long key) {
        if (key == NO_MORE)
            throw new RuntimeException("cannot store(reserved) " + key);
    }

    public void append(long key, long v) {
        validate_key(key);
        long from = (long) values.append(v);

        long offset_len = from << 32L | 1L;
        // if the key already exists, just make the offset point to the new value offset
        // on compaction the old value will be ignored

        int idx;
        if (not_sure.maybe(key) && (idx = keys.bsearch(key)) >= 0) {
            offsets.set(idx,offset_len);
        } else {
            offsets.append(offset_len);
            keys.append(key);
        }
        not_sure.add(key);
    }

    public void append(long key, long[] v) {
        append(key,v,0,v.length);
    }

    public void append(long key, long[] v,int off, int len) {
        validate_key(key);
        if (len > 0) {
            long from = (long) values.append(v,off,len);
            offsets.append(from << 32L | len);
            keys.append(key);
            not_sure.add(key);
        }
    }

    public synchronized void flush() throws Exception {
        Path current = Files.readSymbolicLink(path);

        Path path_tmp = create_new_directory();
        String s_path_tmp = path_tmp.toFile().toString();

        values.flush(get_path_for(s_path_tmp,SA_KEYS));
        offsets.flush(get_path_for(s_path_tmp,SA_OFFSETS));
        keys.flush(get_path_for(s_path_tmp,SA_VALUES));

        Path path_symlink_gen = fs.getPath(path_tmp.toFile().toString() + ".for_symlink");
        Files.createSymbolicLink(path_symlink_gen,path_tmp);
        Files.move(path_symlink_gen,path, StandardCopyOption.REPLACE_EXISTING,StandardCopyOption.ATOMIC_MOVE);

        Files.deleteIfExists(get_path_for(current,SA_KEYS));
        Files.deleteIfExists(get_path_for(current,SA_VALUES));
        Files.deleteIfExists(get_path_for(current,SA_OFFSETS));
        Files.delete(current);
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
        not_sure.clear();
        keys.reset();
        offsets.reset();
        values.reset();
    }

    public int count() {
        return keys.length;
    }

    public void reload() throws Exception {
        not_sure.clear();
        keys.reload(get_path_for(path,SA_KEYS));
        offsets.reload(get_path_for(path,SA_OFFSETS));
        values.reload(get_path_for(path,SA_VALUES));
        for (int i = 0; i < keys.length; i++)
            not_sure.add(keys.get(i));
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
