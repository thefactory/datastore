package com.thefactory.datastore;

import java.nio.channels.ByteChannel;
import java.nio.ByteBuffer;
import java.io.IOException;

public interface DatastoreChannel extends ByteChannel {
    public int read(ByteBuffer dst, long position) throws IOException;

    public long size() throws IOException;
}
