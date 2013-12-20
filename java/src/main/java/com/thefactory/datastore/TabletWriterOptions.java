package com.thefactory.datastore;

public class TabletWriterOptions {
    public int blockSize;
    public boolean useCompression;
    public int keyRestartInterval;

    public TabletWriterOptions() {
        this(4096, true, 16);
    }

    public TabletWriterOptions(int blockSize, boolean useCompression, int keyRestartInterval) {
        this.blockSize = blockSize;
        this.useCompression = useCompression;
        this.keyRestartInterval = keyRestartInterval;
    }
}
