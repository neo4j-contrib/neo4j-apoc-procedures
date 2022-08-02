/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.load;

import apoc.ApocSettings;
import apoc.util.CompressionAlgo;
import apoc.util.MapUtil;
import apoc.util.TestUtil;
import apoc.xml.XmlTestUtils;
import inet.ipaddr.IPAddressString;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static apoc.kernel.KernelTestUtils.checkStatusDetails;
import static apoc.util.BinaryTestUtil.fileToBinary;
import static apoc.util.CompressionConfig.COMPRESSION;
import static apoc.util.TestUtil.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

public class XmlTest {
    public static final String FILE_SHORTENED = "src/test/resources/xml/humboldt_soemmering01_1791.TEI-P5-shortened.xml";
    
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(ApocSettings.apoc_import_file_enabled, true)
            .withSetting(ApocSettings.apoc_import_file_use__neo4j__config, false)
            .withSetting( GraphDatabaseInternalSettings.cypher_ip_blocklist, List.of(new IPAddressString("127.0.0.0/8")));

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Xml.class);
    }

    @Test
    public void testLoadXml() {
        testCall(db, "CALL apoc.load.xml('file:databases.xml')", //  YIELD value RETURN value
                (row) -> {
                    assertEquals(XmlTestUtils.XML_AS_NESTED_MAP, row.get("value"));
                });
    }

    @Test
    public void testLoadXmlAsStream() {
        testResult(db, "CALL apoc.load.xml('file:databases.xml', '/parent/child')", //  YIELD value RETURN value
                (res) -> {
                    final ResourceIterator<Map<String, Object>> value = res.columnAs("value");
                    final Map<String, String> expectedFirstRow = Map.of("_type", "child", "name", "Neo4j", "_text", "Neo4j is a graph database");
                    final Map<String, Object> expectedSecondRow = Map.of("_type", "child", "name", "relational", "_children",
                            List.of(
                                    Map.of("_type", "grandchild", "name", "MySQL", "_text", "MySQL is a database & relational"),
                                    Map.of("_type", "grandchild", "name", "Postgres", "_text", "Postgres is a relational database")
                            ));
                    Map<String, Object> next = value.next();
                    assertEquals(expectedFirstRow, next);
                    next = value.next();
                    assertEquals(expectedSecondRow, next);
                    assertFalse(value.hasNext());
                });
    }

    @Test
    public void testMixedContent() {
        testCall(db, "CALL apoc.load.xml('" + TestUtil.getUrlFileName("xml/mixedcontent.xml") + "')", //  YIELD value RETURN value
                this::commonAssertionsMixedContent);
    }
    
    @Test
    public void testXmlStatusDetails() {
        final String file = ClassLoader.getSystemResource("largeFile.xml").toString();
        checkStatusDetails(db, "CALL apoc.import.xml($file)", MapUtil.map("file", file));
    }

    @Test
    public void testMixedContentWithBinary()  {
        testCall(db, "CALL apoc.load.xml($data, null, $config)",
                map("data", fileToBinary(new File("src/test/resources/xml/mixedcontent.xml"), CompressionAlgo.BLOCK_LZ4.name()),
                        "config", map(COMPRESSION, CompressionAlgo.BLOCK_LZ4.name())),
                this::commonAssertionsMixedContent);
    }

    private void commonAssertionsMixedContent(Map<String, Object> row) {
        assertEquals(map("_type", "root",
                "_children", asList(
                        map("_type", "text",
                                "_children", asList(
                                        map("_type", "mixed"),
                                        "text0",
                                        "text1"
                                )),
                        map("_type", "text",
                                "_text", "text as cdata")
                )), row.get("value"));
    }

    @Test
    public void testBookIds() {
        testResult(db, "call apoc.load.xml('" + TestUtil.getUrlFileName("xml/books.xml") + "') yield value as catalog\n" +
                "UNWIND catalog._children as book\n" +
                "RETURN book.id as id\n", result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1,12).allMatch(value -> ids.contains(String.format("bk1%02d",value))));
        });
    }

    @Test
    public void testFilterIntoCollection() {
        testResult(db, "call apoc.load.xml('" + TestUtil.getUrlFileName("xml/books.xml") + "') yield value as catalog\n" +
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
        testResult(db, "call apoc.load.xml('" + TestUtil.getUrlFileName("xml/books.xml") + "') yield value as catalog " +
                "unwind catalog._children as book " +
                "return book.id as id, [x in book._children where x._type = 'author' | x._text] as authors, [x in book._children where x._type='title' | x._text] as title", result -> {

                    assertEquals("+-----------------------------------------------------------------------------------------------------+\n" +
                            "| id      | authors                                      | title                                      |\n" +
                            "+-----------------------------------------------------------------------------------------------------+\n" +
                            "| \"bk101\" | [\"Gambardella, Matthew\",\"Arciniegas, Fabio\"] | [\"XML Developer's Guide\"]                  |\n" +
                            "| \"bk102\" | [\"Ralls, Kim\"]                               | [\"Midnight Rain\"]                          |\n" +
                            "| \"bk103\" | [\"Corets, Eva\"]                              | [\"Maeve Ascendant\"]                        |\n" +
                            "| \"bk104\" | [\"Corets, Eva\"]                              | [\"Oberon's Legacy\"]                        |\n" +
                            "| \"bk105\" | [\"Corets, Eva\"]                              | [\"The Sundered Grail\"]                     |\n" +
                            "| \"bk106\" | [\"Randall, Cynthia\"]                         | [\"Lover Birds\"]                            |\n" +
                            "| \"bk107\" | [\"Thurman, Paula\"]                           | [\"Splish Splash\"]                          |\n" +
                            "| \"bk108\" | [\"Knorr, Stefan\"]                            | [\"Creepy Crawlies\"]                        |\n" +
                            "| \"bk109\" | [\"Kress, Peter\"]                             | [\"Paradox Lost\"]                           |\n" +
                            "| \"bk110\" | [\"O'Brien, Tim\"]                             | [\"Microsoft .NET: The Programming Bible\"]  |\n" +
                            "| \"bk111\" | [\"O'Brien, Tim\"]                             | [\"MSXML3: A Comprehensive Guide\"]          |\n" +
                            "| \"bk112\" | [\"Galos, Mike\"]                              | [\"Visual Studio 7: A Comprehensive Guide\"] |\n" +
                            "+-----------------------------------------------------------------------------------------------------+\n" +
                            "12 rows\n", result.resultAsString());
                });
    }

    @Test
    public void testLoadXmlXpathAuthorFromBookId () {
        testCall(db, "CALL apoc.load.xml('" + TestUtil.getUrlFileName("xml/books.xml") + "', '/catalog/book[@id=\"bk102\"]/author') yield value as result",
                (r) -> {
                    assertEquals("author", ((Map) r.get("result")).get("_type"));
                    assertEquals("Ralls, Kim", ((Map) r.get("result")).get("_text"));
                });
    }

    @Test
    public void testLoadXmlXpathGenreFromBookTitle () {
        testCall(db, "CALL apoc.load.xml('" + TestUtil.getUrlFileName("xml/books.xml") + "', '/catalog/book[title=\"Maeve Ascendant\"]/genre') yield value as result",
                (r) -> {
                    assertEquals("genre", ((Map) r.get("result")).get("_type"));
                    assertEquals("Fantasy", ((Map) r.get("result")).get("_text"));
                });
    }

    @Test
    public void testLoadXmlXpathReturnBookFromBookTitle () {
        testCall(db, "CALL apoc.load.xml('" + TestUtil.getUrlFileName("xml/books.xml") + "', '/catalog/book[title=\"Maeve Ascendant\"]/.') yield value as result",
                (r) -> {
                    Object value = Iterables.single(r.values());
                    assertEquals(XmlTestUtils.XML_XPATH_AS_NESTED_MAP, value);
                });
    }

    @Test
    public void testLoadXmlWithBlockedIP () {
        QueryExecutionException e = Assert.assertThrows(QueryExecutionException.class,
                () -> testCall(db,
                        "CALL apoc.load.xml('http://127.0.0.0/fake.xml') yield value as result",
                        map(),
                        (r) -> {}
                )
        );
        TestCase.assertTrue(e.getMessage().contains("access to /127.0.0.0 is blocked via the configuration property unsupported.dbms.cypher_ip_blocklist"));
    }

    @Test
    public void testLoadXmlXpathBooKsFromGenre () {
        testResult(db, "CALL apoc.load.xml('" + TestUtil.getUrlFileName("xml/books.xml") + "', '/catalog/book[genre=\"Computer\"]') yield value as result",
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
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("usage of `createNextWordRelationships` is no longer allowed. Use `{relType:'NEXT_WORD', label:'XmlWord'}` instead.");

        db.executeTransactionally("call apoc.xml.import('file:" + FILE_SHORTENED + "', " +
                "{createNextWordRelationships: true, filterLeadingWhitespace: true}) yield node");
    }

    @Test
    public void testLoadXmlWithNextWordRelsWithBinaryFile() {
        final String query = "CALL apoc.import.xml($data, $config) YIELD node";
        final String compression = CompressionAlgo.GZIP.name();
        final Map<String, Object> config = Map.of("data", fileToBinary(new File(FILE_SHORTENED), compression),
                "config", Map.of("compression", compression, 
                        "relType", "NEXT_WORD", "label", "XmlWord", "filterLeadingWhitespace", true)
        );
        commonAssertionsWithNextWordRels(query, config);
    }

    @Test
    public void testLoadXmlWithNextWordRelsWithNewConfigOptions() {
        final String query = "call apoc.xml.import('file:" + FILE_SHORTENED + "', " +
                "{relType: 'NEXT_WORD', label: 'XmlWord', filterLeadingWhitespace: true}) yield node";
        commonAssertionsWithNextWordRels(query, Collections.emptyMap());
    }

    private void commonAssertionsWithNextWordRels(String query, Map<String, Object> config) {
        testCall(db, query, config, row -> assertNotNull(row.get("node")));
        testResult(db, "match (n) return labels(n)[0] as label, count(*) as count", result -> {
            final Map<String, Long> resultMap = result.stream().collect(Collectors.toMap(o -> (String) o.get("label"), o -> (Long) o.get("count")));
            assertEquals(2L, (long) resultMap.get("XmlProcessingInstruction"));
            assertEquals(1L, (long) resultMap.get("XmlDocument"));
            assertEquals(369L, (long) resultMap.get("XmlWord"));
            assertEquals(158L, (long) resultMap.get("XmlTag"));
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
                    assertEquals(369L, r.get("len"));
                });
    }


    @Test
    public void testLoadXmlWithNextEntityRels() {
        testCall(db, "call apoc.xml.import('file:" + FILE_SHORTENED + "', " +
                        "{connectCharacters: true, filterLeadingWhitespace: true}) yield node",
                row -> assertNotNull(row.get("node")));
        testResult(db, "match (n) return labels(n)[0] as label, count(*) as count", result -> {
            final Map<String, Long> resultMap = result.stream().collect(Collectors.toMap(o -> (String)o.get("label"), o -> (Long)o.get("count")));
            assertEquals(2L, (long)resultMap.get("XmlProcessingInstruction"));
            assertEquals(1L, (long)resultMap.get("XmlDocument"));
            assertEquals(369L, (long)resultMap.get("XmlCharacters"));
            assertEquals(158L, (long)resultMap.get("XmlTag"));
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
                    assertEquals(369L, r.get("len"));
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
        testResult(db, "call apoc.load.xml('https://github.com/neo4j/apoc/blob/dev/core/src/test/resources/testload.tar.gz?raw=true!xml/books.xml') yield value as catalog\n" +
                "UNWIND catalog._children as book\n" +
                "RETURN book.id as id\n", result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1,12).allMatch(value -> ids.contains(String.format("bk1%02d",value))));
        });
    }

    @Test
    public void testLoadXmlFromTarGzByUrl() {
        testResult(db, "call apoc.load.xml('https://github.com/neo4j/apoc/blob/dev/core/src/test/resources/testload.tar.gz?raw=true!xml/books.xml') yield value as catalog\n" +
                "UNWIND catalog._children as book\n" +
                "RETURN book.id as id\n", result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1,12).allMatch(value -> ids.contains(String.format("bk1%02d",value))));
        });
    }

    @Test
    public void testLoadXmlFromTgzByUrl() {
        testResult(db, "call apoc.load.xml('https://github.com/neo4j/apoc/blob/dev/core/src/test/resources/testload.tgz?raw=true!xml/books.xml') yield value as catalog\n" +
                "UNWIND catalog._children as book\n" +
                "RETURN book.id as id\n", result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1,12).allMatch(value -> ids.contains(String.format("bk1%02d",value))));
        });
    }

    @Test
    public void testLoadXmlSingleLineSimple() {
        testCall(db, "CALL apoc.load.xml('" + TestUtil.getUrlFileName("xml/singleLine.xml") + "', '/', null, true)", //  YIELD value RETURN value
                (row) -> {
                    assertEquals(XmlTestUtils.XML_AS_SINGLE_LINE_SIMPLE, row.get("value"));
                });
    }

    @Test
    public void testLoadXmlSingleLine() {
        testCall(db, "CALL apoc.load.xml('" + TestUtil.getUrlFileName("xml/singleLine.xml") + "')", //  YIELD value RETURN value
                (row) -> {
                    assertEquals(XmlTestUtils.XML_AS_SINGLE_LINE, row.get("value"));
                });
    }

    @Test
    public void testParse() {
        testCall(db, "WITH '<?xml version=\"1.0\"?><table><tr><td><img src=\"pix/logo-tl.gif\"></img></td></tr></table>' AS xmlString RETURN apoc.xml.parse(xmlString) AS value",
                (row) -> assertEquals(XmlTestUtils.XML_AS_SINGLE_LINE, row.get("value")));
    }

    @Test
    public void testParseWithXPath() throws Exception {
        String xmlString = FileUtils.readFileToString(new File("src/test/resources/xml/books.xml"), Charset.forName("UTF-8"));
        testCall(db, "RETURN apoc.xml.parse($xmlString, '/catalog/book[title=\"Maeve Ascendant\"]/.') AS result",
                map("xmlString", xmlString),
                (r) -> assertEquals(XmlTestUtils.XML_XPATH_AS_NESTED_MAP, r.get("result")));
    }

    @Test(expected = QueryExecutionException.class)
    public void testLoadXmlPreventXXEVulnerabilityThrowsQueryExecutionException() {
        try {
            testResult(db, "CALL apoc.load.xml('" + TestUtil.getUrlFileName("xml/xxe.xml") + "')", (r) -> {
                r.next();
                r.close();
            });
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals(except.getMessage(), "XML documents with a DOCTYPE are not allowed.");
            throw e;
        }
    }

    @Test(expected = QueryExecutionException.class)
    public void testLoadXmlPreventBillionLaughVulnerabilityThrowsQueryExecutionException() {
        try {
            testResult(db, "CALL apoc.load.xml('" + TestUtil.getUrlFileName("xml/billion_laughs.xml") + "')", (r) -> {
                r.next();
                r.close();
            });
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals(except.getMessage(), "XML documents with a DOCTYPE are not allowed.");
            throw e;
        }
    }

    @Test(expected = QueryExecutionException.class)
    public void testXmlParsePreventXXEVulnerabilityThrowsQueryExecutionException() {
        try {
            final var xml = "<?xml version=\"1.0\"?><!DOCTYPE GVI [<!ENTITY xxe SYSTEM \"file:///etc/passwd\">]><foo>&xxe;</foo>";
            testResult(db, "RETURN apoc.xml.parse('" + xml + "')", (r) -> {
                r.next();
                r.close();
            });
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals(except.getMessage(), "XML documents with a DOCTYPE are not allowed.");
            throw e;
        }
    }

    @Test(expected = QueryExecutionException.class)
    public void testXmlParsePreventBillionLaughsVulnerabilityThrowsQueryExecutionException() {
        try {
            final var xml = "<?xml version=\"1.0\"?><!DOCTYPE lolz [<!ENTITY lol \"lol\"><!ENTITY lol1 \"&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;\">]><foo>&lol1;</foo>";
            testResult(db, "RETURN apoc.xml.parse('" + xml + "')", (r) -> {
                r.next();
                r.close();
            });
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals(except.getMessage(), "XML documents with a DOCTYPE are not allowed.");
            throw e;
        }
    }

    @Test(expected = QueryExecutionException.class)
    public void testImportXmlPreventXXEVulnerabilityThrowsQueryExecutionException() {
        try {
            testResult(db, "CALL apoc.import.xml('" + TestUtil.getUrlFileName("xml/xxe.xml") + "')", (r) -> {
                r.next();
                r.close();
            });
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals(except.getMessage(), "XML documents with a DOCTYPE are not allowed.");
            throw e;
        }
    }

    @Test(expected = QueryExecutionException.class)
    public void testImportXmlPreventBillionLaughsVulnerabilityThrowsQueryExecutionException() {
        try {
            testResult(db, "CALL apoc.import.xml('" + TestUtil.getUrlFileName("xml/billion_laughs.xml") + "')", (r) -> {
                r.next();
                r.close();
            });
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals(except.getMessage(), "XML documents with a DOCTYPE are not allowed.");
            throw e;
        }
    }
}
