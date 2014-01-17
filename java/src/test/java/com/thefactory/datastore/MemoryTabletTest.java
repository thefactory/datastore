package com.thefactory.datastore;

import junit.framework.TestCase;
import java.util.Iterator;

public class MemoryTabletTest extends TestCase {

    private MemoryTablet tablet;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        tablet = new MemoryTablet();
    }

    @Override
    public void tearDown() {
        tablet.close();
    }

    public void testMemoryTabletSet() throws Exception {
        Slice k = new Slice("key".getBytes("UTF-8"));
        Slice v = new Slice("value".getBytes("UTF-8"));
        tablet.set(k, v);
        Iterator<KV> kvs = tablet.find(k);
        int count = 0; 

        while(kvs.hasNext()) {
          KV kv = kvs.next();
          assertEquals(kv.getKey(), k);
          assertEquals(kv.getValue(), v);
          count += 1;
        }
        assertEquals(count, 1);
    }

    public void testMemoryTabletReSet() throws Exception {
        Slice k = new Slice("key".getBytes("UTF-8"));
        Slice v = new Slice("value".getBytes("UTF-8"));
        tablet.set(k, new Slice("someinitialvalue".getBytes("UTF-8")));
        tablet.set(k, v);
        Iterator<KV> kvs = tablet.find(k);
        int count = 0; 

        while(kvs.hasNext()) {
          KV kv = kvs.next();
          assertEquals(kv.getKey(), k);
          assertEquals(kv.getValue(), v);
          count += 1;
        }
        assertEquals(count, 1);
    }

    public void testMemoryTabletDelete() throws Exception {
        Slice k = new Slice("key".getBytes("UTF-8"));
        Slice v = new Slice("value".getBytes("UTF-8"));
        tablet.set(k, v);
        tablet.delete(k);
        Iterator<KV> kvs = tablet.find(k);
        int count = 0; 

        while(kvs.hasNext()) {
          KV kv = kvs.next();
          assertTrue(kv.isDeleted());
          count += 1;
        }
        assertEquals(count, 1);
    }

    public void testMemoryTabletEnumerateEmpty() throws Exception {
        Iterator<KV> kvs = tablet.find();
        int count = 0; 

        while(kvs.hasNext()) {
          count += 1;
        }
        assertEquals(count, 0);
    }

    public void testMemoryTabletEnumerateAll() throws Exception {
        Slice[] pairs = new Slice[] {
            new Slice("key0".getBytes("UTF-8")),
            new Slice("value0".getBytes("UTF-8")),
            new Slice("key1".getBytes("UTF-8")),
            new Slice("value1".getBytes("UTF-8")),
            new Slice("key2".getBytes("UTF-8")),
            new Slice("value2".getBytes("UTF-8")),
            new Slice("key3".getBytes("UTF-8")),
            new Slice("value3".getBytes("UTF-8")),
            new Slice("key4".getBytes("UTF-8")),
            new Slice("value4".getBytes("UTF-8"))
        };

        assertTrue(pairs.length % 2 == 0);

        for (int i = 0; i < pairs.length; i += 2) {
            tablet.set(pairs[i], pairs[i + 1]);
        }

        Iterator<KV> kvs = tablet.find();
        int k = 0; 
        while(kvs.hasNext()) {
          KV kv = kvs.next();
          assertEquals(kv.getKey(), pairs[k++]);
          assertEquals(kv.getValue(), pairs[k++]);
        }
        assertEquals(k, pairs.length);
    }

    public void testMemoryTabletEnumerateFromAfter() throws Exception {
        Slice[] pairs = new Slice[] {
            new Slice("key0".getBytes("UTF-8")),
            new Slice("value0".getBytes("UTF-8")),
            new Slice("key1".getBytes("UTF-8")),
            new Slice("value1".getBytes("UTF-8")),
            new Slice("key2".getBytes("UTF-8")),
            new Slice("value2".getBytes("UTF-8")),
            new Slice("key3".getBytes("UTF-8")),
            new Slice("value3".getBytes("UTF-8")),
            new Slice("key4".getBytes("UTF-8")),
            new Slice("value4".getBytes("UTF-8"))
        };

        assertTrue(pairs.length % 2 == 0);

        for (int i = 0; i < pairs.length; i += 2) {
            tablet.set(pairs[i], pairs[i + 1]);
        }

        Iterator<KV> kvs = tablet.find(new Slice("key5".getBytes("UTF-8")));
        int count = 0; 
        while(kvs.hasNext()) {
          KV kv = kvs.next();
          count += 1;
        }
        assertEquals(count, 0);
    }

    public void testMemoryTabletEnumerateFromNFound() throws Exception {
        Slice[] pairs = new Slice[] {
            new Slice("key0".getBytes("UTF-8")),
            new Slice("value0".getBytes("UTF-8")),
            new Slice("key1".getBytes("UTF-8")),
            new Slice("value1".getBytes("UTF-8")),
            new Slice("key2".getBytes("UTF-8")),
            new Slice("value2".getBytes("UTF-8")),
            new Slice("key3".getBytes("UTF-8")),
            new Slice("value3".getBytes("UTF-8")),
            new Slice("key4".getBytes("UTF-8")),
            new Slice("value4".getBytes("UTF-8"))
        };

        assertTrue(pairs.length % 2 == 0);

        for (int i = 0; i < pairs.length; i += 2) {
            tablet.set(pairs[i], pairs[i + 1]);
        }

        int k = 4; 
        Iterator<KV> kvs = tablet.find(pairs[k]);
        while(kvs.hasNext()) {
          KV kv = kvs.next();
          assertEquals(kv.getKey(), pairs[k++]);
          assertEquals(kv.getValue(), pairs[k++]);
        }
        assertEquals(k, pairs.length);
    }

    public void testMemoryTabletEnumerateFromNNotFound() throws Exception {
        Slice[] pairs = new Slice[] {
            new Slice("key0".getBytes("UTF-8")),
            new Slice("value0".getBytes("UTF-8")),
            new Slice("key1".getBytes("UTF-8")),
            new Slice("value1".getBytes("UTF-8")),
            new Slice("key2".getBytes("UTF-8")),
            new Slice("value2".getBytes("UTF-8")),
            new Slice("key3".getBytes("UTF-8")),
            new Slice("value3".getBytes("UTF-8")),
            new Slice("key4".getBytes("UTF-8")),
            new Slice("value4".getBytes("UTF-8"))
        };

        assertTrue(pairs.length % 2 == 0);

        for (int i = 0; i < pairs.length; i += 2) {
            tablet.set(pairs[i], pairs[i + 1]);
        }

        int k = 4; 
        Iterator<KV> kvs = tablet.find(new Slice("key11111".getBytes("UTF-8")));
        while(kvs.hasNext()) {
          KV kv = kvs.next();
          assertEquals(kv.getKey(), pairs[k++]);
          assertEquals(kv.getValue(), pairs[k++]);
        }
        assertEquals(k, pairs.length);
    }
}