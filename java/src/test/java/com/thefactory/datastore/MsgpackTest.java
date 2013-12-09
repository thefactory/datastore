package com.thefactory.datastore;

import junit.framework.TestCase;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;

import static org.junit.Assert.assertArrayEquals;

public class MsgpackTest extends TestCase {

    private class EncodeTestData {
        public EncodeTestData(int num, byte[] bytes) {
            this.num = num;
            this.bytes = bytes;
        }
        public int num;
        public byte[] bytes;
    }

    public void testWriteRawLength () throws Exception {
        // spot checks for raw lengths
        EncodeTestData[] tests = new EncodeTestData[] {
            new EncodeTestData(0, new byte[]{(byte)0xa0}),
            new EncodeTestData(31, new byte[]{(byte)0xbf}),
            new EncodeTestData(32, new byte[]{(byte)0xda, (byte)0x00, (byte)0x20}),
            new EncodeTestData(65535, new byte[]{(byte)0xda, (byte)0xff, (byte)0xff}),
            new EncodeTestData(65536, new byte[]{(byte)0xdb, (byte)0x00, (byte)0x01, (byte)0x00, (byte)0x00}),
            new EncodeTestData(2147483647, new byte[]{(byte)0xdb, (byte)0x7f, (byte)0xff, (byte)0xff, (byte)0xff})
        };

        for (int i = 0; i < tests.length; i++) {
            ByteArrayOutputStream out = new ByteArrayOutputStream(10);
            DataOutput buf = new DataOutputStream(out);
            EncodeTestData test = tests[i];

            // make sure the expected number of bytes were written
            assertTrue(Msgpack.writeRawLength(buf, test.num) == test.bytes.length);

            ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());

            // check the exact expected bytes
            byte[] result = new byte[test.bytes.length];
            assertEquals(in.read(result, 0, test.bytes.length), test.bytes.length);
            Slice resultSlice = new Slice(result);
            Slice referenceSlice = new Slice(test.bytes);
            assertTrue(String.format("test {%d}: {%s} != {%s}", i, referenceSlice, resultSlice), 
                new Slice(test.bytes).equals(new Slice(result)));
        }
    }
}

