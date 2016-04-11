package apoc.help;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author mh
 * @since 11.04.16
 */
public class HelpScannerTest {

    @Test
    public void testScanAll() throws Exception {
        new HelpScanner().scanAll();
    }
}
