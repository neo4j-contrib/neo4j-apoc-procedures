package apoc.load;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.*;

public class SkipWhitespaceInputStreamTest {

    @Test
    public void testRemovalOfWhitespace() throws IOException {
        assertFilterWhitespace("  this is a test \n   with some whitespace", "this is a test with some whitespace");
        assertFilterWhitespace("  this is a test \r\n   with some whitespace", "this is a test with some whitespace");
    }

    private void assertFilterWhitespace(String input, String expected) throws IOException {
        InputStream inputStream = IOUtils.toInputStream(input, "UTF-8");
        assertEquals(expected, IOUtils.toString(new SkipWhitespaceInputStream(inputStream), "UTF-8"));
    }

}
