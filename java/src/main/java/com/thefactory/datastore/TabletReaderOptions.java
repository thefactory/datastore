package com.thefactory.datastore;

public class TabletReaderOptions {
    public final boolean verifyChecksums;

    public TabletReaderOptions(boolean verifyChecksums) {
        this.verifyChecksums = verifyChecksums;
    }

    public TabletReaderOptions() {
        this.verifyChecksums = false;
    }
}
