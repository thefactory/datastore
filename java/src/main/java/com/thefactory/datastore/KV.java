package com.thefactory.datastore;

import java.nio.charset.Charset;

public class KV {
    private byte[] key;
    private byte[] value;

    public KV(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public KV(String key, String value) {
        Charset utf8 = Charset.forName("UTF-8");
        this.key = key.getBytes(utf8);
        this.value = value.getBytes(utf8);
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }
}
