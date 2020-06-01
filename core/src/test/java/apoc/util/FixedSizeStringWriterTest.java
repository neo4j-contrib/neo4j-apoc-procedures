package apoc.util;

import org.junit.Test;

import static org.junit.Assert.*;

public class FixedSizeStringWriterTest {

    @Test
    public void shouldStoreASubstring() {
        FixedSizeStringWriter writer = new FixedSizeStringWriter(10);
        writer.write("This is");
        writer.write(" a string");
        assertEquals("This is a ", writer.toString());
        assertTrue("Should be exceeded", writer.isExceeded());
    }

    @Test
    public void shouldStoreASubstring1() {
        FixedSizeStringWriter writer = new FixedSizeStringWriter(10);
        char[] chars = "This is".toCharArray();
        writer.write(chars, 0, chars.length);
        writer.write(" a string");
        assertEquals("This is a ", writer.toString());
        assertTrue("Should be exceeded", writer.isExceeded());
    }

    @Test
    public void shouldStoreASubstring2() {
        FixedSizeStringWriter writer = new FixedSizeStringWriter(10);
        char[] thisIs = "This is".toCharArray();
        writer.write(thisIs, 5, thisIs.length);
        char[] aString = " a string".toCharArray();
        writer.write(aString, 0, 6);
        assertEquals("is a str", writer.toString());
        assertFalse("Should not be exceeded", writer.isExceeded());
    }

}
