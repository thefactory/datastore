package com.thefactory.datastore;

import java.lang.IndexOutOfBoundsException;
import java.io.UnsupportedEncodingException;
import java.lang.Comparable;
import java.lang.Override;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;

public class Slice implements Comparable<Slice> {
    public final byte[] array;
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

    public int readByte() {
        int ret = getAt(0) & 0x000000FF;
        forward(1);
        return ret;
    }

    public int readShort() {
        int ret = 0;        
        for (int i = 0; i < 2; i++) {
            ret = (ret << 8) | getAt(i);
        }
        forward(2);
        return ret & 0xffff;
    }

    public long readInt() {
        long ret = 0;        
        for (int i = 0; i < 4; i++) {
            ret = (ret << 8) | getAt(i);
        }
        forward(4);
        return ret & 0xffffffff;
    }

    public long readLong() {
        long ret = 0;        
        for (int i = 0; i < 8; i++) {
            ret = (ret << 8) | getAt(i);
        }
        forward(8);
        return ret;
    }

    public byte[] toArray() {
        if (this.offset == 0 && this.length == array.length) {
            return array;
        }
        return detach().array;
    }

    public InputStream toStream() {
        return new ByteArrayInputStream(array, offset, length); 
    }

    public int getAt(int i) {
        return (array[offset + i] & 0xff);
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

    public static Slice prefix(Slice prefix, Slice suffix, int common){
        byte[] tmp = new byte[common + suffix.getLength()];
        System.arraycopy(prefix.array, prefix.getOffset(), tmp, 0, common);
        System.arraycopy(suffix.array, suffix.getOffset(), tmp, common, suffix.getLength());
        return new Slice(tmp);
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

    public String toUTF8String() {
        try {
            return new String(array, offset, length, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return toString();
        }
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
                // Java bytes are signed, cast to int before substracting to ensure
                // correct results for all values with bit 7 set ...
                return ((int)x.array[xi] & 0xff) - ((int)y.array[yi] & 0xff);
            }
        }

        return x.length - y.length;
    }

    public Slice detach() {
        if (this.offset == 0 && this.length == this.array.length) {
            return new Slice(this.array);
        }        

        byte[] ret = new byte[this.length];
        System.arraycopy(this.array, this.offset, ret, 0, this.length);
        return new Slice(ret);
    }

    @Override
    public int compareTo(Slice that) {
        return compare(this, that);
    }
}