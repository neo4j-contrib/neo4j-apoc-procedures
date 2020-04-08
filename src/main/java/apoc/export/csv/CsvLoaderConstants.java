package apoc.export.csv;

import java.util.regex.Pattern;

public class CsvLoaderConstants {

    public static final Pattern FIELD_PATTERN = Pattern.compile("^(?<name>[^:]*)(:(?<type>\\w+))?(\\((?<idspace>[-a-zA-Z_0-9]+)\\))?(?<array>\\[\\])?$");
    public static final String ARRAY_PATTERN = "[]";

    public static final String IGNORE_FIELD = "IGNORE";
    public static final String ID_FIELD = "ID";
    public static final String START_ID_FIELD = "START_ID";
    public static final String END_ID_FIELD = "END_ID";
    public static final String LABEL_FIELD = "LABEL";
    public static final String TYPE_FIELD = "TYPE";

    public static final String IDSPACE_ATTR_PREFIX = "__csv";
    public static final String DEFAULT_IDSPACE = "__CSV_DEFAULT_IDSPACE";

    public static final String ID_ATTR = IDSPACE_ATTR_PREFIX + "_" + ID_FIELD.toLowerCase();
    public static final String START_ID_ATTR = IDSPACE_ATTR_PREFIX + "_" + START_ID_FIELD.toLowerCase();
    public static final String END_ID_ATTR = IDSPACE_ATTR_PREFIX + "_" + END_ID_FIELD.toLowerCase();
    public static final String LABEL_ATTR = IDSPACE_ATTR_PREFIX + "_" + LABEL_FIELD.toLowerCase();
    public static final String TYPE_ATTR = IDSPACE_ATTR_PREFIX + "_" + TYPE_FIELD.toLowerCase();

}
