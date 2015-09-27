package panda;
import java.io.File;
import org.junit.Test;
import org.junit.Ignore;
import static org.junit.Assert.assertEquals;

public class MergeTest {
    @Test
    public void testMerge() throws Exception {	
        new File("/tmp/KV").mkdir();
        KV a = new KV("/tmp/KV/aaa");
        a.reset();
        assertEquals(0, a.count());
        a.append(7,4);
        assertEquals(4, a.get_single(7));
        a.append(9,5);
        assertEquals(5, a.get_single(9));
        assertEquals(2, a.count());
        a.flush();

        KV a2 = new KV("/tmp/KV/aaa");
        assertEquals(2, a.count());
        assertEquals(4, a.get_single(7));
        assertEquals(5, a.get_single(9));

        KV b = new KV("/tmp/KV/bbb");
        b.reset();
        assertEquals(0, b.count());
        b.append(7,8);
        assertEquals(8, b.get_single(7));
        b.append(7,10);
        assertEquals(10, b.get_single(7));
        b.append(7,11);
        assertEquals(11, b.get_single(7));
        b.append(7,12);
        assertEquals(12, b.get_single(7));
        b.append(7,12);
        assertEquals(12, b.get_single(7));
        b.append(8,9);
        assertEquals(9, b.get_single(8));
        assertEquals(2, b.count());
        b.flush();

        KV c = new KV("/tmp/KV/ccc");
        c.reset();
        assertEquals(0, c.count());
        c.merge(a,b);
        c.flush();
        assertEquals(3, c.count());
        assertEquals(4, c.get_single(7));
        assertEquals(9, c.get_single(8));
        assertEquals(5, c.get_single(9));

        c.append(10,10);
        assertEquals(10, c.get_single(10));
        assertEquals(4, c.count());

        KV c2 = new KV("/tmp/KV/ccc");
        assertEquals(KV.NO_MORE, c2.get_single(10));
        assertEquals(3, c2.count());
        assertEquals(4, c.get_single(7));
        assertEquals(9, c.get_single(8));
        assertEquals(5, c.get_single(9));
        c.flush();
        c2.reload();
        assertEquals(10, c2.get_single(10));
        assertEquals(4, c2.count());

        c2.append(11,11);
        assertEquals(11, c2.get_single(11));
        assertEquals(5, c2.count());
        c2.flush();
        c.reload();
        assertEquals(11, c.get_single(11));
        assertEquals(5, c.count());
    }
}
