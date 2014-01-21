package com.thefactory.datastore;

import java.nio.channels.ByteChannel;
import java.nio.ByteBuffer;
import java.io.IOException;

public interface DatastoreChannel extends ByteChannel {
    public long read(ByteBuffer dst, long position) throws IOException;
}
