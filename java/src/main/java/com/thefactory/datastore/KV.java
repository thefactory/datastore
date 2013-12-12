package com.thefactory.datastore;

import java.nio.charset.Charset;

public class KV {
    private Slice key;
    private Slice value;
    private boolean isDeleted;
    private final Slice emptySlice = new Slice(new byte[]{});

    public KV(){
        reset(null, null, true);
    }

    public KV(final String key, final String value){
        reset(key, value);
        isDeleted = false;
    }

    public KV tombstone(final Slice key){
        reset(key, null, true);
        return this; 
    }

    public KV reset(final Slice key, final Slice value){
        reset(key, value, false);
        return this;        
    }

    public KV reset(final String key, final String value){
        Charset utf8 = Charset.forName("UTF-8");
        reset(new Slice(key.getBytes(utf8)), new Slice(value.getBytes(utf8)), false);
        return this;       
    }

    private void reset(final Slice key, final Slice value, final boolean isDeleted) {
        this.key = key;
        this.value = value;
        this.isDeleted = isDeleted;
    }

    public Slice getKey() {
        return key;
    }

    public Slice getValue() {
        return value;
    }

    public byte[] getKeyBytes() {
        return key.getArray();
    }

    public byte[] getValueBytes() {
        return value.getArray();
    }

    public boolean isDeleted() {
        return isDeleted;
    }
}
