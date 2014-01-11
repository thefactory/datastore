package com.thefactory.datastore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.io.EOFException;
import java.util.NoSuchElementException;
import java.util.List;
import java.util.Iterator;
//import java.util.Comparator;
import java.util.ArrayList;

public class FileTablet {
    private final RandomAccessFile tabletFile;
    private final TabletReader reader = new TabletReader();
    private final TabletReaderOptions options;
    private List<TabletReader.TabletIndexRecord> dataIndex;
    private List<TabletReader.TabletIndexRecord> metaIndex;

    public FileTablet(File file, TabletReaderOptions options) throws FileNotFoundException, IOException {
        tabletFile = new RandomAccessFile(file, "r");
        this.options = options; 
        TabletReader.TabletFooter footer = loadFooter();
        metaIndex = loadIndex(footer.metaIndexOffset, footer.metaIndexLength, TabletConstants.META_INDEX_MAGIC);
        dataIndex = loadIndex(footer.dataIndexOffset, footer.dataIndexLength, TabletConstants.DATA_INDEX_MAGIC);
    }

    public void close() throws IOException {
        tabletFile.close();
    }

    public Iterator<KV> find() throws IOException {
        return find(null);
    }

    public Iterator<KV> find(final Slice term) throws IOException {

        return new Iterator<KV>() {
            private int currentBlockIndex = 0;
            private BlockReader currentBlock = null;
            private Iterator<KV> blockIterator = null;

            {
                if (term != null && term.getLength() != 0) {
                    currentBlockIndex = search(term);
                }

                currentBlock = loadBlock(currentBlockIndex);
                blockIterator = currentBlock.find(term);
            }

            public boolean hasNext() {
                return (blockIterator.hasNext() || (currentBlockIndex < dataIndex.size() - 1));
            }

            public KV next() {
                if (blockIterator.hasNext()) {
                    return blockIterator.next();
                }
                currentBlockIndex += 1;
                try {
                    currentBlock = loadBlock(currentBlockIndex);
                } catch (IOException e) {
                    throw new NoSuchElementException(e.getMessage());
                }
                blockIterator = currentBlock.find(null);
                return blockIterator.next();
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }

            private int search(Slice term) {
                int lower = 0;
                int upper = dataIndex.size();
                while (lower < upper) {
                    int mid = lower + (upper - lower) / 2;
                    if (Slice.compare(dataIndex.get(mid).data, term) <= 0) {
                        lower = mid + 1;
                    } else {
                        upper = mid;
                    }
                }
                return (lower > 0) ? lower - 1 : lower;
            }
        };
    }

    public List<BlockReader> blocks() throws IOException {
        ArrayList<BlockReader> ret = new ArrayList<BlockReader>();
        for(int i = 0; i < dataIndex.size(); i++){
            ret.add(loadBlock(i));
        }
        return ret;
    }

    public List<TabletReader.TabletIndexRecord> index() {
        return dataIndex;
    }

    private TabletReader.TabletFooter loadFooter() throws IOException {
        tabletFile.seek(tabletFile.length() - 40);
        byte[] bytes = new byte[40];
        try {
            tabletFile.readFully(bytes);
        } catch (EOFException e) {
            throw new IOException("tablet file with corrupt footer detected");
        }
        return reader.readFooter(new Slice(bytes));
    }

    private List<TabletReader.TabletIndexRecord> loadIndex(long offset, long length, int magic) throws IOException {
        tabletFile.seek(offset);
        byte[] bytes = new byte[(int)length];
        try {
            tabletFile.readFully(bytes);
        } catch (EOFException e) {
            throw new IOException("tablet file with corrupt index detected");
        }
        return reader.readIndex(new Slice(bytes), length, magic);
    }

    private BlockReader loadBlock(long index) throws IOException {
        long offset = dataIndex.get((int) index).offset;
        int length = dataIndex.get((int) index).length;

        tabletFile.seek(offset);
        byte[] bytes = new byte[(int)length];
        try {        
            tabletFile.readFully(bytes);
        } catch (EOFException e) {
            throw new IOException("tablet file with corrupt block detected");
        }
        BlockReader block = reader.readBlock(new Slice(bytes), options.verifyChecksums);

        return block;
    }
}

