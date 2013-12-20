package com.thefactory.datastore;

import java.io.DataInputStream;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.lang.UnsupportedOperationException;
import java.io.IOException;

public class BlockReader {
    private final Slice block;
    private final Slice kvs;

    private final int numRestarts;

    public BlockReader(final Slice block) {
        this.block = block;
        this.numRestarts = Utils.toUInt32(block.subslice(-4));

        int end = block.getLength() - 4 * this.numRestarts - 4;
        this.kvs = block.subslice(0, end);
    }

    public Iterator<KV> find() {
        return find(null);
    }

    public Iterator<KV> find(Slice term) {
        if (term == null || term.getLength() == 0 || numRestarts == 0) {
            return pairs(kvs, term);
        }

        int restart = 0;
        int upper = numRestarts;

        try {
            while (restart < upper) {
                int half = restart + ((upper - restart) / 2);
                if (Slice.compare(restartKey(half), term) < 0) {
                    restart = half + 1;
                } else {
                    upper = half;
                }
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("reading block failed");            
        }  

        if (restart == 0) {
            return pairs(kvs, term);
        } else if (restart >= numRestarts) {
            return empty();
        }

        return pairs(kvs.subslice(restartValue(restart - 1)), term);
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

    private Iterator<KV> pairs(final Slice slice, final Slice fromKey) {
        final SliceReader reader = new SliceReader(slice);
        return new Iterator<KV>() {
            private KV startKey = null;
            {
                if(fromKey != null) {
                    try {
                        while(hasNext()) {
                            startKey = reader.readOne();
                            if ((Slice.compare(startKey.getKey(), fromKey) >= 0)) {
                                break;
                            }
                        }
                    } catch (IOException e) {
                        throw new IllegalArgumentException("corrupt block");
                    }
                }
            }

            public boolean hasNext() {
                return reader.getPos() < kvs.getLength();
            }

            public KV next() {
                try {
                    KV ret;
                    if(startKey != null){
                        ret = startKey;
                        startKey = null; 
                    } else {
                        ret = reader.readOne();
                    }
                    return ret;
                } catch (IOException e) {
                    throw new NoSuchElementException(e.getMessage());
                }
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private class SliceReader {
        private int pos = 0;
        private Slice previousKey = null;
        private final DataInputStream stream;
        private final Slice slice;
        final KV kv = new KV(); 

        public SliceReader(final Slice slice) {
            this.stream = new DataInputStream(slice.toStream());
            this.slice = slice;
        }

        public int getPos(){
            return pos;
        }

        public KV readOne() throws IOException {
            Slice key;
            int common = readUInt32();
            int suffixLength = readRawLength();
            Slice suffix = readSubslice(suffixLength);

            if (common == 0) {
                key = suffix;
            } else {
                key = Slice.prefix(previousKey, suffix, common);
            }

            previousKey = key;

            int valueLength = readRawLength();
            if(valueLength == -1){
                return kv.tombstone(key);
            }
            return kv.reset(key, readSubslice(valueLength));                                  
        }

        private Slice readSubslice(int len) throws IOException{
            Slice ret = slice.subslice(pos, len);
            stream.skip(len);
            pos += len;
            return ret;
        }

        private int readUInt32() throws IOException {
            int num = 0;

            int flag = stream.readByte();
            if (flag <= Msgpack.MAXIMUM_FIXED_POS) {
                pos += 1;
                return flag;
            } else if (flag == Msgpack.MSG_UINT_16) {
                for (int i = 0; i < 2; i++) {
                    num = (num << 8) | (byte)stream.readByte();
                }
                pos += 2;
            } else if (flag == Msgpack.MSG_UINT_32) {
                for (int i = 0; i < 4; i++) {
                    num = (num << 8) | (byte)stream.readByte();
                }
                pos += 3;
            }

            return num;
        }

        private int readRawLength() throws IOException {
            int length = 0;

            int flag = stream.readByte();
            if (flag == Msgpack.NIL_VALUE) {
                return -1;
            }

            if ((flag & 0xe0) == Msgpack.MINIMUM_FIXED_RAW) {
                length = (int)(flag & 0x1f);
                pos += 1;
            } else if (flag == Msgpack.MSG_RAW_16) {
                for (int i = 0; i < 2; i++) {
                    length = (length << 8) | (byte)stream.readByte();
                }
                pos += 2;
            } else if (flag == Msgpack.MSG_RAW_32) {
                for (int i = 0; i < 4; i++) {
                    length = (length << 8) | (byte)stream.readByte();
                }
                pos += 3;
            } else {
                throw new IOException("Unexpected message pack raw flag byte: " + flag);
            }
            return length;
        }
    }

    private Slice restartKey(int n) throws IOException {
        int pos = restartValue(n);

        // skip the first byte at pos, which is guaranteed to be 0x0 because this is a restart
        SliceReader reader = new SliceReader(kvs.subslice(pos + 1));
        int keyLength = reader.readRawLength();
        return reader.readSubslice(keyLength);
    }

    private int restartValue(int n) {
        // decode the n'th restart to its position in the kv data
        return Utils.toUInt32(block.subslice(restartPosition(n)));
    }

    private int restartPosition(int n) {
        if (n >= numRestarts) {
            throw new IllegalArgumentException("Invalid restart: " + n);
        }

        // return the in-block position of the n'th restart
        int indexStart = block.getLength() - (4 * numRestarts + 4);
        return indexStart + 4 * n;
    }        
 }
