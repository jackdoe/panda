just some experiments with stored array of longs

* expects append to be called in sorted fashion
* merge() removes duplicated entries, preferring first stored value
* flush() dumps it to disk
* get() binary searches

build:

 $ gradle build
 $ gradle test


example usage (examples/Example.java):

import panda.KV;
import java.util.Arrays;
public class Example {
    public static void main(String[] args) throws Exception {
        KV a = new KV("/tmp/KV/aaa");
        a.append(7,4);

        KV b = new KV("/tmp/KV/bbb");
        b.append(7,8);

        b.append(7,10);
        b.append(8,9);

        long[] l = { 1,2,3,4,5,6,7 };
        b.append(999,l);

        KV c = new KV("/tmp/KV/ccc");
        c.merge(a,b);
        c.append(1000,1000000);
        c.forEach((k,v) -> {
                System.out.println(k + " = " + Arrays.toString(v));
        });
    }
}
