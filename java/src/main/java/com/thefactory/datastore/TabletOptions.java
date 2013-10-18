package com.thefactory.datastore;

public class TabletOptions {
    public int blockSize;
    public boolean useCompression;
    public int keyRestartInterval;

    public TabletOptions() {
        this(4096, true, 16);
    }

    public TabletOptions(int blockSize, boolean useCompression, int keyRestartInterval) {
        this.blockSize = blockSize;
        this.useCompression = useCompression;
        this.keyRestartInterval = keyRestartInterval;
    }
}
