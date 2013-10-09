package com.thefactory.datastore;

import junit.framework.TestCase;
import org.junit.Test;

public class UtilsTest extends TestCase {
    @Test
    public void testCommonPrefix() throws Exception {
        assertEquals(3, Utils.commonPrefix("foo".getBytes(), "foo".getBytes()));
        assertEquals(3, Utils.commonPrefix("foo".getBytes(), "food".getBytes()));
        assertEquals(0, Utils.commonPrefix("foo".getBytes(), "bar".getBytes()));
    }
}
