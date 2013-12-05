package com.thefactory.datastore;

import junit.framework.TestCase;

import static org.junit.Assert.assertArrayEquals;

public class SliceTest extends TestCase {

    public void testCompareSameLength() throws Exception {
        Slice s0 = new Slice(new byte[]{0, 1, 2, 3}, 0, 4);
        Slice s1 = new Slice(new byte[]{3, 2, 1, 0}, 0, 4);

        assertTrue(Slice.compare(s0, s1) < 0);
        assertTrue(Slice.compare(s0, s0) == 0);
        assertTrue(Slice.compare(s1, s0) > 0);

        assertTrue(s0.equals(s0));

        Slice s3 = new Slice(new byte[]{0, 1, 2, 3}, 0, 4);
        assertTrue(s3.equals(s0));
        assertTrue(s0.equals(s3));
    }

    public void testEquals() throws Exception {
        Slice s0 = new Slice(new byte[]{0, 1, 2, 3}, 0, 4);
        Slice s1 = new Slice(new byte[]{0, 1, 2, 3}, 0, 1);
        Slice s2 = new Slice(new byte[]{0, 1, 2, 3}, 0, 4);

        assertTrue(s2.equals(s0));
        assertTrue(s0.equals(s2));
        assertFalse(s0.equals(s1));
    }

    public void testCompare() {
        Slice s0 = new Slice(new byte[]{0});
        Slice s1 = new Slice(new byte[]{0, 1});

        assertTrue(Slice.compare(s0, s1) < 0);
        assertTrue(Slice.compare(s0, s0) == 0);
        assertTrue(Slice.compare(s1, s0) > 0);
    }

    public void testSubslice() {
        Slice s0 =  new Slice(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

        /* one-argument slicing */
        assertTrue(Slice.compare(s0.subslice(0), s0) == 0);
        assertTrue(Slice.compare(s0.subslice(2), new Slice(new byte[]{2, 3, 4, 5, 6, 7, 8, 9})) == 0);
        assertTrue(Slice.compare(s0.subslice(-4), new Slice(new byte[]{6, 7, 8, 9})) == 0);

        /* two-argument slicing */
        assertTrue(Slice.compare(s0.subslice(0, 4), new Slice(new byte[]{0, 1, 2, 3})) == 0);
        assertTrue(Slice.compare(s0.subslice(2, 4), new Slice(new byte[]{2, 3, 4, 5})) == 0);
        assertTrue(Slice.compare(s0.subslice(-4, 2), new Slice(new byte[]{6, 7})) == 0);
    }

    public void testCommonBytes() {
        assertTrue(new Slice(new byte[]{0, 1, 2, 3, 4}).commonBytes(new Slice(new byte[]{0, 1, 9, 9, 9})) == 2);
    }

    public void testIsPrefix() {
        assertTrue(Slice.isPrefix(new Slice(new byte[]{0, 1, 2, 3, 4}), new Slice(new byte[]{0, 1})));
        assertFalse(Slice.isPrefix(new Slice(new byte[]{0, 1}), new Slice(new byte[]{0, 1, 2, 3, 4})));
    }

}

