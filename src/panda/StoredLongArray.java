package panda;
import java.nio.*;
import java.nio.file.*;
import java.io.*;
import java.util.*;

public class StoredLongArray {
    public static final long NO_MORE = Long.MAX_VALUE;
    public static final long NOT_READY = Long.MIN_VALUE;

    public long[] buffer;
    public int length;
    public int unsafe_index_iter;
    Path path;
    Path path_tmp;

    public StoredLongArray(String _path) throws Exception {
        path = FileSystems.getDefault().getPath(_path);
        path_tmp = FileSystems.getDefault().getPath(_path + ".tmp");
        reload();
    }

    public void reload() throws Exception {
        length = 0;
        try {
            byte[] bytes = Files.readAllBytes(path); // XXX: reader
            ensure(bytes.length / 8);
            for (int i = 0; i < bytes.length; i+= 8)
                append(abyte_to_long(bytes,i));
        } catch(java.nio.file.NoSuchFileException e) {
            buffer = new long[0];
            flush();
        }

    }

    public void flush() throws Exception {
        BufferedOutputStream os = new BufferedOutputStream(Files.newOutputStream(path_tmp,StandardOpenOption.TRUNCATE_EXISTING,StandardOpenOption.WRITE,StandardOpenOption.CREATE));
        byte[] out = new byte[8];
        for (int i = 0; i < length; i++) {
            out[0] = (byte) (buffer[i] >> 56);
            out[1] = (byte) (buffer[i] >> 48);
            out[2] = (byte) (buffer[i] >> 40);
            out[3] = (byte) (buffer[i] >> 32);
            out[4] = (byte) (buffer[i] >> 24);
            out[5] = (byte) (buffer[i] >> 16);
            out[6] = (byte) (buffer[i] >> 8);
            out[7] = (byte) (buffer[i]);
            os.write(out,0,8);
        }
        os.flush();
        os.close();

        Files.move(path_tmp,path, StandardCopyOption.REPLACE_EXISTING,StandardCopyOption.ATOMIC_MOVE);
    }
    public long get(int idx) { return buffer[idx]; };
    public int bsearch(long key) {
        return Arrays.binarySearch(buffer, 0, length, key);
    }

    public long append(long l) {
        ensure((length + 1) * 2);
        buffer[length++] = l;
        return length-1;
    }

    public long append(long[] l) {
        return append(l,0,l.length);
    }

    public long append(long[] l, int offset, int rlen) {
        ensure(length + rlen);
        System.arraycopy(l,offset, buffer, length, rlen);
        int idx = length;
        length += rlen;
        return idx;
    }

    public void ensure(int n) {
        if (buffer == null)
            buffer = new long[n];
        else if (buffer.length < n)
            buffer = Arrays.copyOf(buffer,n);
    }

    public void reset() { length = 0; };

    public long[] slice(int from, int len) {
        return slice(from,len,new long[len]);
    }
    public long[] slice(int from, int len, long[] into) {
        System.arraycopy(buffer, from, into,0, len);
        return into;
    }

    public static long abyte_to_long(byte[] bytes, int off) {
        long r =
            ((long)(bytes[off + 0] & 0xFF)) << 56L |
            ((long)(bytes[off + 1] & 0xFF)) << 48L |
            ((long)(bytes[off + 2] & 0xFF)) << 40L |
            ((long)(bytes[off + 3] & 0xFF)) << 32L |
            ((long)(bytes[off + 4] & 0xFF)) << 24L |
            ((long)(bytes[off + 5] & 0xFF)) << 16L |
            ((long)(bytes[off + 6] & 0xFF)) << 8L  |
            ((long)(bytes[off + 7] & 0xFF));
        return r;
    }

    public long unsafe_index_iter_current() {
        if (unsafe_index_iter >= length)
            return Long.MAX_VALUE;

        return buffer[unsafe_index_iter];
    }

    public long unsafe_index_iter_next() {
        unsafe_index_iter++;
        return unsafe_index_iter_current();
    }

    public String toString() {
        return path.toString() + ":" + length + " [ " + unsafe_index_iter_current() + " ]";
    }
}
