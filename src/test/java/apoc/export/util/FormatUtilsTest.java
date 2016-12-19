package apoc.export.util;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author mh
 * @since 19.12.16
 */
public class FormatUtilsTest {
    @Test
    public void formatString() throws Exception {
        assertEquals("\"\\n\"",FormatUtils.formatString("\n"));
        assertEquals("\"\\t\"",FormatUtils.formatString("\t"));
        assertEquals("\"\\\"\"",FormatUtils.formatString("\""));
        assertEquals("\"\\\\\"",FormatUtils.formatString("\\"));
        assertEquals("\"\\n\"",FormatUtils.formatString('\n'));
        assertEquals("\"\\t\"",FormatUtils.formatString('\t'));
        assertEquals("\"\\\"\"",FormatUtils.formatString('"'));
        assertEquals("\"\\\\\"",FormatUtils.formatString('\\'));
    }

}
