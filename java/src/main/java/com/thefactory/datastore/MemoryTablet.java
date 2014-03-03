package com.thefactory.datastore;

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class MemoryTablet {    
    private final ConcurrentSkipListMap<Slice, Slice> backing;
    private long size = 0;

    public static Slice tombstone = new Slice(new byte[] {(byte)0x74, (byte)0x6f, (byte)0x6d, (byte)0x62});

    public MemoryTablet() {
        backing = new ConcurrentSkipListMap<Slice, Slice>(
            new Comparator<Slice>() {
                public int compare(Slice x, Slice y) {
                    return Slice.compare(x, y);
                }
            }
        );        
    }

    public void set(Slice key, Slice value) {
        backing.put(key, value);
        synchronized(this) {
            size += key.getLength() + value.getLength();
        }
    }

    public void delete(Slice key) {
        set(key, tombstone);
    }

    public void close() {
        backing.clear();
    }

    public Iterator<KV> find() {
        return find(null);
    }

    public Iterator<KV> find(final Slice term) {
        if (backing.size() == 0) {
            return empty();
        }
        return new Iterator<KV>() {
            Iterator<Map.Entry<Slice, Slice>> itemIterator;
            {
                if (term == null){
                    itemIterator = backing.entrySet().iterator();
                } else {
                    itemIterator = backing.tailMap(term, true).entrySet().iterator();
                }

            }

            public boolean hasNext() {
                return itemIterator.hasNext();
            }

            public KV next() {
                Map.Entry<Slice, Slice> item = itemIterator.next();
                KV ret = new KV();
                if(item.getValue() == tombstone){
                    ret.tombstone(item.getKey());
                } else {
                    ret.reset(item.getKey(), item.getValue());
                }
                return ret;
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public void apply(Batch batch) {
        if (batch.isEmpty()) {
            return;
        }

        Iterator<KV> kvs = batch.pairs();
        while(kvs.hasNext()) {
            KV kv = kvs.next();
            if(kv.isDeleted()) {
                delete(kv.getKey().detach());
            } else {
                set(kv.getKey().detach(), kv.getValue().detach());
            }
        }
    }

    public long size() {
        return size;
    }

    private Iterator<KV> empty() {
        return new Iterator<KV>() {
            public boolean hasNext() {
                return false;
            }
            
            public KV next() {
                throw new NoSuchElementException();
            }
                
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
