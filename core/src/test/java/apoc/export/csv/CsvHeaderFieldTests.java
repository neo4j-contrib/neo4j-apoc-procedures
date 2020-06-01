package apoc.export.csv;

import apoc.meta.Meta;
import org.junit.Test;

import java.util.regex.Matcher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CsvHeaderFieldTests {

    public static final String TEST_NAME = "name";
    public static final String TEST_TYPE = "Type";
    public static final String TEST_IDSPACE = "IDSPACE";
    public static final String TEST_ARRAY = "[]";

    public static final String TEST_FIELD_1 = "name:Type(IDSPACE)[]";
    public static final String TEST_FIELD_2 = "name:Type(IDSPACE)";
    public static final String TEST_FIELD_3 = "name:Type";
    public static final String TEST_FIELD_4 = "name";

    @Test
    public void testCsvField1() {
        CsvHeaderField field = CsvHeaderField.parse(0, TEST_FIELD_1, '"');
        assertEquals(TEST_NAME,    field.getName());
        assertEquals(TEST_TYPE,    field.getType());
        assertEquals(TEST_IDSPACE, field.getIdSpace());
        assertTrue(field.isArray());
    }

    @Test
    public void testCsvField2() {
        CsvHeaderField field = CsvHeaderField.parse(0, TEST_FIELD_2, '"');
        assertEquals(TEST_NAME,    field.getName());
        assertEquals(TEST_TYPE,    field.getType());
        assertEquals(TEST_IDSPACE, field.getIdSpace());
        assertFalse(field.isArray());
    }

    @Test
    public void testCsvField3() {
        CsvHeaderField field = CsvHeaderField.parse(0, TEST_FIELD_3, '"');
        assertEquals(TEST_NAME, field.getName());
        assertEquals(TEST_TYPE, field.getType());
        assertEquals(CsvLoaderConstants.DEFAULT_IDSPACE, field.getIdSpace());
        assertFalse(field.isArray());
    }

    @Test
    public void testCsvField4() {
        CsvHeaderField field = CsvHeaderField.parse(0, TEST_FIELD_4, '"');
        assertEquals(TEST_NAME, field.getName());
        assertEquals(Meta.Types.STRING.name(), field.getType());
        assertEquals(CsvLoaderConstants.DEFAULT_IDSPACE, field.getIdSpace());
        assertFalse(field.isArray());
    }

    @Test
    public void testNamedGroups() {
        Matcher matcher = CsvLoaderConstants.FIELD_PATTERN.matcher(TEST_FIELD_1);

        assertTrue(matcher.find());
        assertEquals(6, matcher.groupCount());
        assertEquals(TEST_NAME,    matcher.group("name"));
        assertEquals(TEST_TYPE,    matcher.group("type"));
        assertEquals(TEST_IDSPACE, matcher.group("idspace"));
        assertEquals(TEST_ARRAY,   matcher.group("array"));
    }

}
