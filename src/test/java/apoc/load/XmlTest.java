package apoc.load;

import apoc.util.TestUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.xml.sax.SAXParseException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static apoc.util.TestUtil.*;
import static org.junit.Assert.*;

public class XmlTest {

    public static final String XML_AS_NESTED_MAP =
            "{_type=parent, name=databases, " +
                    "_children=[" +
                    "{_type=child, name=Neo4j, _text=Neo4j is a graph database}, " +
                    "{_type=child, name=relational, _children=[" +
                    "{_type=grandchild, name=MySQL, _text=MySQL is a database & relational}, " +
                    "{_type=grandchild, name=Postgres, _text=Postgres is a relational database}]}]}";
    public static final String XML_AS_NESTED_SIMPLE_MAP =
            "{_type=parent, name=databases, " +
                    "_child=[" +
                    "{_type=child, name=Neo4j, _text=Neo4j is a graph database}, " +
                    "{_type=child, name=relational, _grandchild=[" +
                    "{_type=grandchild, name=MySQL, _text=MySQL is a database & relational}, " +
                    "{_type=grandchild, name=Postgres, _text=Postgres is a relational database}]}]}";
    public static final String XML_XPATH_AS_NESTED_MAP =
            "[{_type=book, id=bk103, _children=[{_type=author, _text=Corets, Eva}, {_type=title, _text=Maeve Ascendant}, {_type=genre, _text=Fantasy}, {_type=price, _text=5.95}, {_type=publish_date, _text=2000-11-17}, {_type=description, _text=After the collapse of a nanotechnology " +
                    "society in England, the young survivors lay the " +
                    "foundation for a new society.}]}]";

    public static final String XML_AS_SINGLE_LINE =
            "{_type=table, _children=[{_type=tr, _children=[{_type=td, _children=[{_type=img, src=pix/logo-tl.gif}]}]}]}";

    public static final String XML_AS_SINGLE_LINE_SIMPLE =
            "{_type=table, _table=[{_type=tr, _tr=[{_type=td, _td=[{_type=img, src=pix/logo-tl.gif}]}]}]}";

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig("apoc.import.file.use_neo4j_config", "false")
                .setConfig("apoc.import.file.enabled", "true")
                .newGraphDatabase();
        TestUtil.registerProcedure(db, Xml.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testLoadXml() throws Exception {
        testCall(db, "CALL apoc.load.xml('file:databases.xml')", //  YIELD value RETURN value
                (row) -> {
                    Object value = row.get("value");
                    assertEquals(XML_AS_NESTED_MAP, value.toString());
                });
    }

    @Test
    public void testLoadXmlSimple() throws Exception {
        testCall(db, "CALL apoc.load.xmlSimple('file:databases.xml')", //  YIELD value RETURN value
                (row) -> {
                    Object value = row.get("value");
                    assertEquals(XML_AS_NESTED_SIMPLE_MAP, value.toString());
                });
    }

    @Test
    public void testMixedContent() {
        testCall(db, "CALL apoc.load.xml('file:src/test/resources/xml/mixedcontent.xml')", //  YIELD value RETURN value
                (row) -> {
                    Object value = row.get("value");
                    //assertEquals("{_type=root, _children=[{_type=text, _children=[text0, {_type=mixed}, text1]}, {_type=text, _text=text as cdata}]}", value.toString());
                    assertEquals("{_type=root, _children=[{_type=text, _children=[{_type=mixed}, text0, text1]}, {_type=text, _text=text as cdata}]}", value.toString());

                });
    }

    @Test
    public void testBookIds() {
        testResult(db, "call apoc.load.xml('file:src/test/resources/xml/books.xml') yield value as catalog\n" +
                "UNWIND catalog._children as book\n" +
                "RETURN book.id as id\n", result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1,12).allMatch(value -> ids.contains(String.format("bk1%02d",value))));
        });
    }

    @Test
    public void testFilterIntoCollection() {
        testResult(db, "call apoc.load.xml('file:src/test/resources/xml/books.xml') yield value as catalog\n" +
                        "    UNWIND catalog._children as book\n" +
                        "    RETURN book.id, [attr IN book._children WHERE attr._type IN ['author','title'] | [attr._type, attr._text]] as pairs"
                , result -> {
                    assertEquals("+----------------------------------------------------------------------------------------------------------------+\n" +
                            "| book.id | pairs                                                                                                |\n" +
                            "+----------------------------------------------------------------------------------------------------------------+\n" +
                            "| \"bk101\" | [[\"author\",\"Gambardella, Matthew\"],[\"author\",\"Arciniegas, Fabio\"],[\"title\",\"XML Developer's Guide\"]] |\n" +
                            "| \"bk102\" | [[\"author\",\"Ralls, Kim\"],[\"title\",\"Midnight Rain\"]]                                                  |\n" +
                            "| \"bk103\" | [[\"author\",\"Corets, Eva\"],[\"title\",\"Maeve Ascendant\"]]                                               |\n" +
                            "| \"bk104\" | [[\"author\",\"Corets, Eva\"],[\"title\",\"Oberon's Legacy\"]]                                               |\n" +
                            "| \"bk105\" | [[\"author\",\"Corets, Eva\"],[\"title\",\"The Sundered Grail\"]]                                            |\n" +
                            "| \"bk106\" | [[\"author\",\"Randall, Cynthia\"],[\"title\",\"Lover Birds\"]]                                              |\n" +
                            "| \"bk107\" | [[\"author\",\"Thurman, Paula\"],[\"title\",\"Splish Splash\"]]                                              |\n" +
                            "| \"bk108\" | [[\"author\",\"Knorr, Stefan\"],[\"title\",\"Creepy Crawlies\"]]                                             |\n" +
                            "| \"bk109\" | [[\"author\",\"Kress, Peter\"],[\"title\",\"Paradox Lost\"]]                                                 |\n" +
                            "| \"bk110\" | [[\"author\",\"O'Brien, Tim\"],[\"title\",\"Microsoft .NET: The Programming Bible\"]]                        |\n" +
                            "| \"bk111\" | [[\"author\",\"O'Brien, Tim\"],[\"title\",\"MSXML3: A Comprehensive Guide\"]]                                |\n" +
                            "| \"bk112\" | [[\"author\",\"Galos, Mike\"],[\"title\",\"Visual Studio 7: A Comprehensive Guide\"]]                        |\n" +
                            "+----------------------------------------------------------------------------------------------------------------+\n" +
                            "12 rows\n", result.resultAsString());
                });
    }

    @Test
    public void testReturnCollectionElements() {
        testResult(db, "call apoc.load.xml('file:src/test/resources/xml/books.xml') yield value as catalog\n"+
                        "UNWIND catalog._children as book\n" +
                        "WITH book.id as id, [attr IN book._children WHERE attr._type IN ['author','title'] | attr._text] as pairs\n" +
                        "RETURN id, pairs[0] as author, pairs[1] as title"
                , result -> {
                    assertEquals("+-----------------------------------------------------------------------------+\n" +
                            "| id      | author                 | title                                    |\n" +
                            "+-----------------------------------------------------------------------------+\n" +
                            "| \"bk101\" | \"Gambardella, Matthew\" | \"Arciniegas, Fabio\"                      |\n" +
                            "| \"bk102\" | \"Ralls, Kim\"           | \"Midnight Rain\"                          |\n" +
                            "| \"bk103\" | \"Corets, Eva\"          | \"Maeve Ascendant\"                        |\n" +
                            "| \"bk104\" | \"Corets, Eva\"          | \"Oberon's Legacy\"                        |\n" +
                            "| \"bk105\" | \"Corets, Eva\"          | \"The Sundered Grail\"                     |\n" +
                            "| \"bk106\" | \"Randall, Cynthia\"     | \"Lover Birds\"                            |\n" +
                            "| \"bk107\" | \"Thurman, Paula\"       | \"Splish Splash\"                          |\n" +
                            "| \"bk108\" | \"Knorr, Stefan\"        | \"Creepy Crawlies\"                        |\n" +
                            "| \"bk109\" | \"Kress, Peter\"         | \"Paradox Lost\"                           |\n" +
                            "| \"bk110\" | \"O'Brien, Tim\"         | \"Microsoft .NET: The Programming Bible\"  |\n" +
                            "| \"bk111\" | \"O'Brien, Tim\"         | \"MSXML3: A Comprehensive Guide\"          |\n" +
                            "| \"bk112\" | \"Galos, Mike\"          | \"Visual Studio 7: A Comprehensive Guide\" |\n" +
                            "+-----------------------------------------------------------------------------+\n" +
                            "12 rows\n", result.resultAsString());
                });
    }

    @Test
    public void testLoadXmlXpathAuthorFromBookId () {
        testCall(db, "CALL apoc.load.xml('file:src/test/resources/xml/books.xml', '/catalog/book[@id=\"bk102\"]/author') yield value as result",
                (r) -> {
                    assertEquals("author", ((Map) r.get("result")).get("_type"));
                    assertEquals("Ralls, Kim", ((Map) r.get("result")).get("_text"));
                });
    }

    @Test
    public void testLoadXmlXpathGenreFromBookTitle () {
        testCall(db, "CALL apoc.load.xml('file:src/test/resources/xml/books.xml', '/catalog/book[title=\"Maeve Ascendant\"]/genre') yield value as result",
                (r) -> {
                    assertEquals("genre", ((Map) r.get("result")).get("_type"));
                    assertEquals("Fantasy", ((Map) r.get("result")).get("_text"));
                });
    }

    @Test
    public void testLoadXmlXpathReturnBookFromBookTitle () {
        testCall(db, "CALL apoc.load.xml('file:src/test/resources/xml/books.xml', '/catalog/book[title=\"Maeve Ascendant\"]/.') yield value as result",
                (r) -> {
                    Object value = r.values();
                    assertEquals(XML_XPATH_AS_NESTED_MAP, value.toString());
                });
    }

    @Test
    public void testLoadXmlXpathBooKsFromGenre () {
        testResult(db, "CALL apoc.load.xml('file:src/test/resources/xml/books.xml', '/catalog/book[genre=\"Computer\"]') yield value as result",
                (r) -> {
                    Map<String, Object> next = r.next();
                    Object result = next.get("result");
                    Map resultMap = (Map) next.get("result");
                    Object children = resultMap.get("_children");

                    List<Object>  childrenList = (List<Object>) children;
                    assertEquals("bk101", ((Map) result).get("id"));
                    assertEquals("author", ((Map) childrenList.get(0)).get("_type"));
                    assertEquals("Gambardella, Matthew", ((Map) childrenList.get(0)).get("_text"));
                    assertEquals("author", ((Map) childrenList.get(1)).get("_type"));
                    assertEquals("Arciniegas, Fabio", ((Map) childrenList.get(1)).get("_text"));
                    next = r.next();
                    result = next.get("result");
                    resultMap = (Map) next.get("result");
                    children = resultMap.get("_children");
                    childrenList = (List<Object>) children;
                    assertEquals("bk110", ((Map) result).get("id"));
                    assertEquals("author", ((Map) childrenList.get(0)).get("_type"));
                    assertEquals("O'Brien, Tim", ((Map) childrenList.get(0)).get("_text"));
                    assertEquals("title", ((Map) childrenList.get(1)).get("_type"));
                    assertEquals("Microsoft .NET: The Programming Bible", ((Map) childrenList.get(1)).get("_text"));
                    next = r.next();
                    result = next.get("result");
                    resultMap = (Map) next.get("result");
                    children = resultMap.get("_children");
                    childrenList = (List<Object>) children;
                    assertEquals("bk111", ((Map) result).get("id"));
                    assertEquals("author", ((Map) childrenList.get(0)).get("_type"));
                    assertEquals("O'Brien, Tim", ((Map) childrenList.get(0)).get("_text"));
                    assertEquals("title", ((Map) childrenList.get(1)).get("_type"));
                    assertEquals("MSXML3: A Comprehensive Guide", ((Map) childrenList.get(1)).get("_text"));
                    next = r.next();
                    result = next.get("result");
                    resultMap = (Map) next.get("result");
                    children = resultMap.get("_children");
                    childrenList = (List<Object>) children;
                    assertEquals("bk112", ((Map) result).get("id"));
                    assertEquals("author", ((Map) childrenList.get(0)).get("_type"));
                    assertEquals("Galos, Mike", ((Map) childrenList.get(0)).get("_text"));
                    assertEquals("title", ((Map) childrenList.get(1)).get("_type"));
                    assertEquals("Visual Studio 7: A Comprehensive Guide", ((Map) childrenList.get(1)).get("_text"));
                    assertEquals(false, r.hasNext());
                });
    }

    @Test
    public void testLoadXmlNoFailOnError () {
        testCall(db, "CALL apoc.load.xml('file:src/test/resources/books.xm', '', {failOnError:false}) yield value as result",
                (r) -> {
                    Map resultMap = (Map) r.get("result");
                    assertEquals(Collections.emptyMap(), resultMap);
                });
    }

    @Test
    public void testLoadXmlWithNextWordRels() {
        testCall(db, "call apoc.xml.import('file:src/test/resources/xml/humboldt_soemmering01_1791.TEI-P5.xml', " +
                        "{createNextWordRelationships: true, filterLeadingWhitespace: true}) yield node",
                row -> assertNotNull(row.get("node")));
        testResult(db, "match (n) return labels(n)[0] as label, count(*) as count", result -> {
            final Map<String, Long> resultMap = result.stream().collect(Collectors.toMap(o -> (String)o.get("label"), o -> (Long)o.get("count")));
            assertEquals(2l, (long)resultMap.get("XmlProcessingInstruction"));
            assertEquals(1l, (long)resultMap.get("XmlDocument"));
            assertEquals(3263l, (long)resultMap.get("XmlWord"));
            assertEquals(454l, (long)resultMap.get("XmlTag"));
        });

        // no node more than one NEXT/NEXT_SIBLING
        testCallEmpty(db, "match (n) where size( (n)-[:NEXT]->() ) > 1 return n", null);
        testCallEmpty(db, "match (n) where size( (n)-[:NEXT_SIBLING]->() ) > 1 return n", null);

        // no node more than one IS_FIRST_CHILD / IS_LAST_CHILD
        testCallEmpty(db, "match (n) where size( (n)<-[:FIRST_CHILD_OF]-() ) > 1 return n", null);
        testCallEmpty(db, "match (n) where size( (n)<-[:LAST_CHILD_OF]-() ) > 1 return n", null);

        // NEXT_WORD relationship do connect all word nodes
        testResult(db, "match p=(:XmlDocument)-[:NEXT_WORD*]->(e:XmlWord) where not (e)-[:NEXT_WORD]->() return length(p) as len",
                result -> {
                    Map<String, Object> r = Iterators.single(result);
                    assertEquals(3263l, r.get("len"));
                });
    }

    @Test
    public void testLoadXmlWithNextEntityRels() {
        testCall(db, "call apoc.xml.import('file:src/test/resources/xml/humboldt_soemmering01_1791.TEI-P5.xml', " +
                        "{connectCharacters: true, filterLeadingWhitespace: true}) yield node",
                row -> assertNotNull(row.get("node")));
        testResult(db, "match (n) return labels(n)[0] as label, count(*) as count", result -> {
            final Map<String, Long> resultMap = result.stream().collect(Collectors.toMap(o -> (String)o.get("label"), o -> (Long)o.get("count")));
            assertEquals(2l, (long)resultMap.get("XmlProcessingInstruction"));
            assertEquals(1l, (long)resultMap.get("XmlDocument"));
            assertEquals(3263l, (long)resultMap.get("XmlCharacters"));
            assertEquals(454l, (long)resultMap.get("XmlTag"));
        });

        // no node more than one NEXT/NEXT_SIBLING
        testCallEmpty(db, "match (n) where size( (n)-[:NEXT]->() ) > 1 return n", null);
        testCallEmpty(db, "match (n) where size( (n)-[:NEXT_SIBLING]->() ) > 1 return n", null);

        // no node more than one IS_FIRST_CHILD / IS_LAST_CHILD
        testCallEmpty(db, "match (n) where size( (n)<-[:FIRST_CHILD_OF]-() ) > 1 return n", null);
        testCallEmpty(db, "match (n) where size( (n)<-[:LAST_CHILD_OF]-() ) > 1 return n", null);

        // NEXT_WORD relationship do connect all word nodes
        testResult(db, "match p=(:XmlDocument)-[:NE*]->(e:XmlCharacters) where not (e)-[:NE]->() return length(p) as len",
                result -> {
                    Map<String, Object> r = Iterators.single(result);
                    assertEquals(3263l, r.get("len"));
                });
    }

    @Test
    public void testLoadXmlFromZip() {
        testResult(db, "call apoc.load.xml('file:src/test/resources/testload.zip!xml/books.xml') yield value as catalog\n" +
                "UNWIND catalog._children as book\n" +
                "RETURN book.id as id\n", result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1,12).allMatch(value -> ids.contains(String.format("bk1%02d",value))));
        });
    }

    @Test
    public void testLoadXmlFromTar() {
        testResult(db, "call apoc.load.xml('file:src/test/resources/testload.tar!xml/books.xml') yield value as catalog\n" +
                "UNWIND catalog._children as book\n" +
                "RETURN book.id as id\n", result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1,12).allMatch(value -> ids.contains(String.format("bk1%02d",value))));
        });
    }

    @Test
    public void testLoadXmlFromTarGz() {
        testResult(db, "call apoc.load.xml('file:src/test/resources/testload.tar.gz!xml/books.xml') yield value as catalog\n" +
                "UNWIND catalog._children as book\n" +
                "RETURN book.id as id\n", result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1,12).allMatch(value -> ids.contains(String.format("bk1%02d",value))));
        });
    }

    @Test
    public void testLoadXmlFromTgz() {
        testResult(db, "call apoc.load.xml('file:src/test/resources/testload.tgz!xml/books.xml') yield value as catalog\n" +
                "UNWIND catalog._children as book\n" +
                "RETURN book.id as id\n", result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1,12).allMatch(value -> ids.contains(String.format("bk1%02d",value))));
        });
    }

    @Test
    public void testLoadXmlFromZipByUrl() {
        testResult(db, "call apoc.load.xml('https://github.com/neo4j-contrib/neo4j-apoc-procedures/blob/3.4/src/test/resources/testload.zip?raw=true!xml/books.xml') yield value as catalog\n" +
                "UNWIND catalog._children as book\n" +
                "RETURN book.id as id\n", result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1,12).allMatch(value -> ids.contains(String.format("bk1%02d",value))));
        });
    }

    @Test
    public void testLoadXmlFromTarByUrl() {
        testResult(db, "call apoc.load.xml('https://github.com/neo4j-contrib/neo4j-apoc-procedures/blob/3.4/src/test/resources/testload.tar?raw=true!xml/books.xml') yield value as catalog\n" +
                "UNWIND catalog._children as book\n" +
                "RETURN book.id as id\n", result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1,12).allMatch(value -> ids.contains(String.format("bk1%02d",value))));
        });
    }

    @Test
    public void testLoadXmlFromTarGzByUrl() {
        testResult(db, "call apoc.load.xml('https://github.com/neo4j-contrib/neo4j-apoc-procedures/blob/3.4/src/test/resources/testload.tar.gz?raw=true!xml/books.xml') yield value as catalog\n" +
                "UNWIND catalog._children as book\n" +
                "RETURN book.id as id\n", result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1,12).allMatch(value -> ids.contains(String.format("bk1%02d",value))));
        });
    }

    @Test
    public void testLoadXmlFromTgzByUrl() {
        testResult(db, "call apoc.load.xml('https://github.com/neo4j-contrib/neo4j-apoc-procedures/blob/3.4/src/test/resources/testload.tgz?raw=true!xml/books.xml') yield value as catalog\n" +
                "UNWIND catalog._children as book\n" +
                "RETURN book.id as id\n", result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1,12).allMatch(value -> ids.contains(String.format("bk1%02d",value))));
        });
    }

    @Test(expected = QueryExecutionException.class)
    public void testLoadXmlPreventXXEVulnerabilityThrowsQueryExecutionException() {
        try {
            testResult(db, "CALL apoc.load.xml('file:src/test/resources/xml/xxe.xml', '/catalog/book[genre=\"Computer\"]') yield value as result", (r) -> {});
        } catch (QueryExecutionException e) {
            // We want test that the cause of the exception is SAXParseException with the correct cause message
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof SAXParseException);
            assertEquals("DOCTYPE is disallowed when the feature \"http://apache.org/xml/features/disallow-doctype-decl\" set to true.", except.getMessage());
            throw e;
        }
    }

    @Test
    public void testLoadXmlSingleLineSimple() throws Exception {
        testCall(db, "CALL apoc.load.xml('file:src/test/resources/xml/singleLine.xml', '/', null, true)", //  YIELD value RETURN value
                (row) -> {
                    Object value = row.get("value");
                    assertEquals(XML_AS_SINGLE_LINE_SIMPLE, value.toString());
                });
    }

    @Test
    public void testLoadXmlSingleLine() throws Exception {
        testCall(db, "CALL apoc.load.xml('file:src/test/resources/xml/singleLine.xml')", //  YIELD value RETURN value
                (row) -> {
                    Object value = row.get("value");
                    assertEquals(XML_AS_SINGLE_LINE, value.toString());
                });
    }
}
