package apoc.util;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class FileTestUtil {
    
    public static void assertStreamEquals(File directoryExpected, String fileName, String actualText) {
        String expectedText = TestUtil.readFileToString(new File(directoryExpected, fileName));
        String[] actualArray = actualText.split("\n");
        String[] expectArray = expectedText.split("\n");
        assertEquals(expectArray.length, actualArray.length);
        for (int i = 0; i < actualArray.length; i++) {
            assertEquals(JsonUtil.parse(expectArray[i],null, Object.class), JsonUtil.parse(actualArray[i],null, Object.class));
        }
    }
}
