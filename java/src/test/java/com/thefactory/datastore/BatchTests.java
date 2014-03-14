package com.thefactory.datastore;

import junit.framework.TestCase;
import java.util.Iterator;

public class BatchTests extends TestCase {
        
    public void testBatchWriter() throws Exception {
        Batch batch = new Batch();
        assertTrue(batch.isEmpty());

        Slice[] pairs = new Slice[] {
            new Slice("foo".getBytes("UTF-8")), new Slice("bar".getBytes("UTF-8")),
            new Slice("baz".getBytes("UTF-8")), new Slice("quux".getBytes("UTF-8"))
        };

        int i;
        for (i = 0; i < pairs.length; i += 2) {
            batch.put(pairs[i], pairs[i + 1]);
        }

        Iterator<KV> kvs = batch.pairs();
        
        i = 0;
        while(kvs.hasNext()) {
          KV kv = kvs.next();
          assertEquals(kv.getKey(), pairs[i++]);
          assertEquals(kv.getValue(), pairs[i++]);
        }
        assertEquals(i, pairs.length);
    }

    public void testBatchWithDeletes() throws Exception {
        Batch batch = new Batch();
        assertTrue(batch.isEmpty());

        Slice[] pairs = new Slice[] {
            new Slice("foo".getBytes("UTF-8")), new Slice("bar".getBytes("UTF-8")),
            new Slice("baz".getBytes("UTF-8")), null,
            new Slice("quux".getBytes("UTF-8")), new Slice("quuux".getBytes("UTF-8"))
        };


        int i;
        for (i = 0; i < pairs.length; i += 2) {
            if (pairs[i + 1] == null) {
                batch.delete(pairs[i]);
            } else {
                batch.put(pairs[i], pairs[i + 1]);
            }
        }

        Iterator<KV> kvs = batch.pairs();

        i = 0;
        while(kvs.hasNext()) {
          KV kv = kvs.next();
          if(pairs[i + 1] == null) {
              assertEquals(kv.getKey(), pairs[i]);
              assertTrue(kv.isDeleted());
          } else {
              assertEquals(kv.getKey(), pairs[i]);
              assertEquals(kv.getValue(), pairs[i + 1]);
              assertFalse(kv.isDeleted());
          }
          i += 2;
        }
        assertEquals(i, pairs.length);
    }
}
