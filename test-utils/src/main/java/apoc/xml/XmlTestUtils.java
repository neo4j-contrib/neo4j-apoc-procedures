package apoc.xml;

import java.util.Arrays;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

public class XmlTestUtils {

    public static final Map<String, Object> XML_AS_NESTED_MAP = map(
            "_type", "parent",
            "name", "databases",
            "_children", asList(
                    map("_type", "child", "name", "Neo4j", "_text", "Neo4j is a graph database"),
                    map("_type", "child", "name", "relational", "_children", asList(
                            map("_type", "grandchild", "name", "MySQL", "_text", "MySQL is a database & relational"),
                            map("_type", "grandchild", "name", "Postgres", "_text", "Postgres is a relational database")
                    ))
            )
    );
    public static final Map<String, Object> XML_AS_NESTED_SIMPLE_MAP = map(
        "_type", "parent",
        "name", "databases",
        "_child", asList(
                    map("_type", "child", "name", "Neo4j", "_text", "Neo4j is a graph database"),
                    map("_type", "child", "name", "relational", "_grandchild", asList(
                            map("_type", "grandchild", "name", "MySQL", "_text", "MySQL is a database & relational"),
                            map("_type", "grandchild", "name", "Postgres", "_text", "Postgres is a relational database")
                    ))
            )
    );
    public static final Map<String, Object> XML_AS_SINGLE_LINE_SIMPLE = map(
            "_type", "table",
            "_table", asList(map(
                    "_type", "tr",
                    "_tr", asList(map(
                            "_type", "td",
                            "_td", asList(map(
                                    "_type", "img",
                                    "src", "pix/logo-tl.gif"
                            ))
                    ))))
    );
    public static final Map<String, Object> XML_AS_SINGLE_LINE = map(
            "_type", "table",
            "_children", asList( map(
                    "_type", "tr",
                    "_children", asList(map(
                        "_type", "td",
                        "_children", asList(map(
                                "_type", "img",
                                    "src", "pix/logo-tl.gif"
                            ))
                    ))
            ))
    );
    public static final Map<String, Object> XML_XPATH_AS_NESTED_MAP = map("_type", "book", "id", "bk103", "_children",
            Arrays.asList(map("_type", "author", "_text", "Corets, Eva"),
                    map("_type", "title", "_text", "Maeve Ascendant"),
                    map("_type", "genre", "_text", "Fantasy"),
                    map("_type", "price", "_text", "5.95"),
                    map("_type", "publish_date", "_text", "2000-11-17"),
                    map("_type", "description", "_text", "After the collapse of a nanotechnology society in England, the young survivors lay the foundation for a new society.")
            ));
}
