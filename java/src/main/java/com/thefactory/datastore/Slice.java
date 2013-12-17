package com.thefactory.datastore;

import java.lang.IndexOutOfBoundsException;
import java.io.UnsupportedEncodingException;
import java.lang.Comparable;
import java.lang.Override;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;

public class Slice implements Comparable<Slice> {
    private byte[] array;
    private int offset;
    private int length;

    public Slice(byte[] array, int offset, int length) {
        if (offset + length > array.length) {
            throw new IndexOutOfBoundsException("Slice index out of bounds");
        }
        this.array = array;
        this.offset = offset;
        this.length = length;
    }

    public Slice(byte[] array) {
        this.array = array;
        this.offset = 0;
        this.length = array.length;
    }

    public byte[] getArray() {
        return array;
    }
    
    public int getOffset() { 
        return offset; 
    }
        
    public int getLength() { 
        return length;
    }

    public void forward(int nbytes) {
        if(nbytes > length) {
            throw new IndexOutOfBoundsException("Slice index out of bounds");
        }
        offset += nbytes;
        length -= nbytes;
    }

    public byte readByte() {
        byte ret = getAt(0);
        forward(1);
        return ret;
    }

    public int readShort() {
        int ret = 0;        
        for (int i = 0; i < 2; i++) {
            ret = (ret << 8) | getAt(1 + i);
        }
        forward(2);
        return ret;
    }

    public int readInt() {
        int ret = 0;        
        for (int i = 0; i < 4; i++) {
            ret = (ret << 8) | getAt(1 + i);
        }
        forward(4);
        return ret;
    }

    public long readLong() {
        long ret = 0;        
        for (int i = 0; i < 8; i++) {
            ret = (ret << 8) | getAt(1 + i);
        }
        forward(8);
        return ret;
    }

    public byte[] toArray() {
        if (this.offset == 0 && this.length == array.length) {
            return array;
        }

        byte[] ret = new byte[this.length];

        System.arraycopy(this.array, this.offset, ret, 0, this.length);
        return ret;
    }

    public InputStream toStream() {
        return new ByteArrayInputStream(array, offset, length); 
    }

    public byte getAt(int i) {
        return array[offset + i];
    }

    public Slice subslice(int skip) {
        if (skip < 0) {
            // slicing from [-5:] will always produce a 5-element slice
            return subslice(skip, -skip);
        }

        return subslice(skip, this.length - skip);
    }

    public Slice subslice(int skip, int length) {
        int newOffset;
        if (skip < 0) {
            newOffset = this.offset + this.length + skip;
            if (newOffset < 0) {
                throw new IllegalArgumentException("Subslice offset less than zero");
            }
        } else {
            newOffset = this.offset + skip;
        }

        return new Slice(this.array, newOffset, length);
    }

    public int commonBytes(Slice that) {
        if (that == null) {
            return 0;
        }

        int end = java.lang.Math.min(this.length, that.length);

        for (int n = 0; n < end; n++) {
            if (this.array[n] != that.array[n]) {
                return n;
            }
        }
        return end - 1;
    }

    public static boolean isPrefix(Slice slice, Slice prefix) {
        if (slice.length < prefix.length) {
            return false;
        }

        for (int i = 0; i < prefix.length; i++) {
            if (slice.getAt(i) != prefix.getAt(i)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean equals(Object other){
        if (other == null) { 
          return false;
        }
        if (other == this) {
          return true;
        }
        if (!(other instanceof Slice)) {
          return false;
        }

        Slice otherSlice = (Slice) other;
        return compare(this, otherSlice) == 0;
    }

    @Override
    public int hashCode() {
        int result = this.length;
        result = 31 * result + offset;
        return 31 * result + (array != null ? array.hashCode() : 0);
    }    

    @Override
    public String toString() {
        StringWriter w = new StringWriter();
        w.write("TheFactory.Datastore.Slice[");

        for (int i = offset; i < offset + length; i++) {
            w.write(String.format("{0:%02X}", array[i]));
            if (i != offset + length - 1)
                w.write(" ");
        }
        w.write("]");

        return w.toString();
    }  

    public String toUTF8String() throws UnsupportedEncodingException {
        return new String(toArray(), "UTF-8");
    }

    public static int compare(Slice x, Slice y) {
        if (x == y) {
            return 0;
        }

        if (x == null) {
            return -1;
        }

        if (y == null) {
            return 1;
        }

        int length = java.lang.Math.min(x.length, y.length);

        for (int xi = x.offset, yi = y.offset; xi < x.offset + length; xi++, yi++) {
            if (x.array[xi] != y.array[yi]) {
                return x.array[xi] - y.array[yi];
            }
        }

        return x.length - y.length;
    }

    public Slice detach() {
        return new Slice(toArray(), 0, this.length);
    }

    @Override
    public int compareTo(Slice that) {
        return compare(this, that);
    }

}
