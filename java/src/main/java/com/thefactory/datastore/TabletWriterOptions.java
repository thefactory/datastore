package com.thefactory.datastore;

public class TabletWriterOptions {
    public int blockSize;
    public boolean useCompression;
    public int keyRestartInterval;
    public boolean checkKeyOrder;

    public TabletWriterOptions() {
        this(4096, true, 16, true);
    }

    public TabletWriterOptions(int blockSize, boolean useCompression, int keyRestartInterval, boolean checkKeyOrder) {
        this.blockSize = blockSize;
        this.useCompression = useCompression;
        this.keyRestartInterval = keyRestartInterval;
        this.checkKeyOrder = checkKeyOrder;
    }
}
