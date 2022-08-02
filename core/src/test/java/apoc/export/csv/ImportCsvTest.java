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
package apoc.export.csv;

import apoc.ApocSettings;
import apoc.util.CompressionAlgo;
import apoc.util.TestUtil;
import org.apache.commons.io.FileUtils;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.kernel.impl.core.RelationshipEntity;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.Values;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.util.BinaryTestUtil.fileToBinary;
import static apoc.util.CompressionConfig.COMPRESSION;
import static apoc.util.MapUtil.map;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.TransactionStateMemoryAllocation.OFF_HEAP;
import static org.neo4j.configuration.SettingValueParsers.BYTES;
import static org.junit.Assert.fail;

public class ImportCsvTest {
    public static final String BASE_URL_FILES = "src/test/resources/csv-inputs";
    private static final ZoneId DEFAULT_TIMEZONE = ZoneId.of("Asia/Tokyo");
    
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(ApocSettings.apoc_import_file_enabled, true)
            .withSetting(ApocSettings.apoc_export_file_enabled, true)
            .withSetting(GraphDatabaseSettings.allow_file_urls, true)
            .withSetting(GraphDatabaseSettings.db_temporal_timezone, DEFAULT_TIMEZONE)
            .withSetting(GraphDatabaseSettings.tx_state_max_off_heap_memory, BYTES.parse("250m"))
            .withSetting(GraphDatabaseSettings.tx_state_memory_allocation, OFF_HEAP)
            .withSetting(GraphDatabaseSettings.memory_tracking, true)
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, new File(BASE_URL_FILES).toPath().toAbsolutePath());

    final Map<String, String> testCsvs = Collections
            .unmodifiableMap(Stream.of(
                    new AbstractMap.SimpleEntry<>("array", ":ID|name:STRING[]\n" +
                            "1|John;Bob;Alice\n"),
                    new AbstractMap.SimpleEntry<>("custom-ids-basic-affiliated-with", ":START_ID,:END_ID\n" +
                            "1,3\n" +
                            "2,4\n"),
                    new AbstractMap.SimpleEntry<>("custom-ids-basic-companies", "companyId:ID,name:STRING\n" +
                            "4,Neo4j\n"),
                    new AbstractMap.SimpleEntry<>("custom-ids-basic-persons", "personId:ID,name:STRING\n" +
                            "1,John\n" +
                            "2,Jane\n"),
                    new AbstractMap.SimpleEntry<>("custom-ids-basic-unis", "uniId:ID,name:STRING\n" +
                            "3,TU Munich\n"),
                    new AbstractMap.SimpleEntry<>("custom-ids-idspaces-affiliated-with", ":START_ID(Person),:END_ID(Organisation)\n" +
                            "1,1\n" +
                            "2,2\n"),
                    new AbstractMap.SimpleEntry<>("custom-ids-idspaces-companies", "companyId:ID(Organisation),name:STRING\n" +
                            "2,Neo4j\n"),
                    new AbstractMap.SimpleEntry<>("custom-ids-idspaces-persons", "personId:ID(Person),name:STRING\n" +
                            "1,John\n" +
                            "2,Jane\n"),
                    new AbstractMap.SimpleEntry<>("custom-ids-idspaces-unis", "uniId:ID(Organisation),name:STRING\n" +
                            "1,TU Munich\n"),
                    new AbstractMap.SimpleEntry<>("id-idspaces", ":ID(Person)|name:STRING\n" +
                            "1|John\n" +
                            "2|Jane\n"),
                    new AbstractMap.SimpleEntry<>("id-idspaces-with-dash", ":ID(Person-Id)|name:STRING\n" +
                            "1|John\n" +
                            "2|Jane\n"),
                    new AbstractMap.SimpleEntry<>("id", "id:ID|name:STRING\n" +
                            "1|John\n" +
                            "2|Jane\n"),
                    new AbstractMap.SimpleEntry<>("csvPoint", 
                            ":ID,location:point{crs:WGS-84}\n" +
                            "1,\"{latitude:55.6121514, longitude:12.9950357}\"\n" +
                            "2,\"{y:51.507222, x:-0.1275}\"\n" +
                            "3,\"{latitude:37.554167, longitude:-122.313056, height: 100, crs:'WGS-84-3D'}\"\n"),
                    new AbstractMap.SimpleEntry<>("nodesMultiTypes",
                            ":ID(MultiType-ID)|date1:datetime{timezone:Europe/Stockholm}|date2:datetime|foo:string|joined:date|active:boolean|points:int\n" +
                            "1|2018-05-10T10:30|2018-05-10T12:30|Joe Soap|2017-05-05|true|10\n" +
                            "2|2018-05-10T10:30[Europe/Berlin]|2018-05-10T12:30[Europe/Berlin]|Jane Doe|2017-08-21|true|15\n"),
                    new AbstractMap.SimpleEntry<>("emptyDate",
                            "id:ID,:LABEL,str:STRING,int:INT,date:DATE\n" +
                                    "1,Lab,hello,1,2020-01-01\n" +
                                    "2,Lab,world,2,2020-01-01\n" +
                                    "3,Lab,,,\n"),
                    new AbstractMap.SimpleEntry<>("relMultiTypes",
                            ":START_ID(MultiType-ID)|:END_ID(MultiType-ID)|prop1:IGNORE|prop2:time{timezone:+02:00}[]|foo:int|time:duration[]|baz:localdatetime[]|bar:localtime[]\n" +
                            "1|2|a|15:30|1|P14DT16H12M|2020-01-01T00:00:00|11:00:00\n" +
                            "2|1|b|15:30+01:00|2|P5M1.5D|2021|12:00:00\n"),
                    new AbstractMap.SimpleEntry<>("id-with-duplicates", "id:ID|name:STRING\n" +
                            "1|John\n" +
                            "1|Jane\n"),
                    new AbstractMap.SimpleEntry<>("ignore-nodes", ":ID|firstname:STRING|lastname:IGNORE|age:INT\n" +
                            "1|John|Doe|25\n" +
                            "2|Jane|Doe|26\n"),
                    new AbstractMap.SimpleEntry<>("ignore-relationships", ":START_ID|:END_ID|prop1:IGNORE|prop2:INT\n" +
                            "1|2|a|3\n" +
                            "2|1|b|6\n"),
                    new AbstractMap.SimpleEntry<>("label", ":ID|:LABEL|name:STRING\n" +
                            "1|Student;Employee|John\n"),
                    new AbstractMap.SimpleEntry<>("knows", ":START_ID,:END_ID,since:INT\n" +
                            "1,2,2016\n" +
                            "10,11,2014\n" +
                            "11,12,2013"),
                    new AbstractMap.SimpleEntry<>("persons", ":ID,name:STRING,speaks:STRING[]\n" +
                            "1,John,\"en,fr\"\n" +
                            "2,Jane,\"en,de\""),
                    new AbstractMap.SimpleEntry<>("quoted", "id:ID|:LABEL|name:STRING\n" +
                            "'1'|'Student:Employee'|'John'\n"),
                    new AbstractMap.SimpleEntry<>("rel-on-ids-idspaces", ":START_ID(Person)|:END_ID(Person)|since:INT\n" +
                            "1|2|2016\n"),
                    new AbstractMap.SimpleEntry<>("rel-on-ids", "x:START_ID|:END_ID|since:INT\n" +
                            "1|2|2016\n"),
                    new AbstractMap.SimpleEntry<>("rel-type", ":START_ID|:END_ID|:TYPE|since:INT\n" +
                            "1|2|FRIENDS_WITH|2016\n" +
                            "2|1||2016\n"),
                    new AbstractMap.SimpleEntry<>("typeless", ":ID|name\n" +
                            "1|John\n" +
                            "2|Jane\n"),
                    new AbstractMap.SimpleEntry<>("personsWithoutIdField", "name:STRING\n" +
                            "John\n" +
                            "Jane\n"),
                    new AbstractMap.SimpleEntry<>("emptyInteger", 
                            ":ID(node_space_1),:LABEL,str_attribute:STRING,int_attribute:INT,int_attribute_array:INT[],double_attribute_array:FLOAT[]\n" +
                            "n1,Thing,once upon a time,1,\"2;3\",\"2.3;3.5\"\n" +
                            "n2,Thing,,2,\"4;5\",\"2.6;3.6\"\n" +
                            "n3,Thing,,,,\n"),
                    new AbstractMap.SimpleEntry<>("emptyArray", 
                            "id:ID,:LABEL,arr:STRING[],description:STRING\n" +
                            "1,Arrays,a;b;c;d;e,normal,\n" +
                            "2,Arrays,,withNull\n" +
                            "3,Arrays,a;;c;;e,withEmptyItem\n" +
                            "4,Arrays,a; ;c; ;e,withBlankItem\n" +
                            "5,Arrays, ,withWhiteSpace\n"
                    )
            ).collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue)));

    @Before
    public void setUp() throws IOException {
        for (Map.Entry<String, String> entry : testCsvs.entrySet()) {
            CsvTestUtil.saveCsvFile(entry.getKey(), entry.getValue());
        }

        TestUtil.registerProcedure(db, ImportCsv.class);
    }
    
    @Test
    public void testImportCsvLargeFile() {
        TestUtil.testCall(db, "CALL apoc.import.csv([{fileName: $nodeFile, labels: ['Person']}], [], $config)",
                map("nodeFile", "file:/largeFile.csv",
                        "config", map("batchSize", 100L)),
                (r) -> assertEquals(277442L, r.get("nodes")));
    }

    @Test
    public void testNodesWithIds() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                map(
                        "file", "file:/id.csv",
                        "config", map("delimiter", '|', "stringIds", false)
                ),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                }
        );

        List<String> names = TestUtil.firstColumn(db, "MATCH (n:Person) RETURN n.name AS name ORDER BY name");
        assertThat(names, Matchers.containsInAnyOrder("Jane", "John"));

        List<Long> ids = TestUtil.firstColumn(db, "MATCH (n:Person) RETURN n.id AS id ORDER BY id");
        assertThat(ids, Matchers.contains(1L, 2L));
    }
    
    @Test
    public void testImportCsvWithSkipLines() {
        // skip only-header (default config)
        testSkipLine(1L, 2);

        // skip header and another one
        testSkipLine(2L, 1);

        // skip header and another two (no result because the file has 3 lines)
        testSkipLine(3L, 0);
    }

    private void testSkipLine(long skipLine, int nodes) {
        TestUtil.testCall(db,
                "call apoc.import.csv([{fileName: 'id-idspaces.csv', labels: ['SkipLine']}], [], $config)",
                map("config", 
                        map("delimiter", '|', "skipLines", skipLine)),
                (r) -> assertEquals((long) nodes, r.get("nodes"))
        );

        TestUtil.testCallCount(db, "MATCH (n:SkipLine) RETURN n", nodes);
        
        db.executeTransactionally("MATCH (n:SkipLine) DETACH DELETE n");
    }

    @Test
    public void issue2826WithImportCsv() {
        db.executeTransactionally("CREATE (n:Person {name: 'John'})");
        db.executeTransactionally("CREATE CONSTRAINT unique_person ON (n:Person) ASSERT n.name IS UNIQUE");
        try {
            TestUtil.testCall(db,
                    "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                    map("file", "file:/id.csv", "config", map("delimiter", '|')),
                    (r) -> fail());
        } catch (RuntimeException e) {
            String expected = "Failed to invoke procedure `apoc.import.csv`: " +
                    "Caused by: IndexEntryConflictException{propertyValues=( String(\"John\") ), addedNodeId=-1, existingNodeId=0}";
            assertEquals(expected, e.getMessage());
        }

        // should return only 1 node due to constraint exception
        TestUtil.testCall(db, "MATCH (n:Person) RETURN properties(n) AS props",
                r -> assertEquals(Map.of("name", "John"), r.get("props")));

        db.executeTransactionally("DROP CONSTRAINT unique_person");
    }

    @Test
    public void testNodesAndRelsWithMultiTypes() {
        TestUtil.testCall(db,
                "CALL apoc.import.csv([{fileName: $nodeFile, labels: ['Person']}], [{fileName: $relFile, type: 'KNOWS'}], $config)",
                map("nodeFile", "nodesMultiTypes.csv", 
                        "relFile", "file:/relMultiTypes.csv",
                        "config", map("delimiter", '|')),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(2L, r.get("relationships")); 
                }
        );
        TestUtil.testCall(db, "MATCH p=(start:Person)-[rel {foo: 1}]->(end:Person)-[relSecond {foo:2}]->(start) RETURN start, end, rel, relSecond", r-> {
            final Map<String, Object> expectedStart = Map.of("joined", LocalDate.of(2017, 5, 5), 
                    "foo", "Joe Soap", "active", true, 
                    "date2", ZonedDateTime.of(2018, 5, 10, 12, 30, 0, 0, DEFAULT_TIMEZONE),
                    "date1", ZonedDateTime.of(2018, 5, 10, 10, 30, 0, 0, ZoneId.of("Europe/Stockholm")), 
                    "points", 10L, "__csv_id", "1");
            assertEquals(expectedStart, ((NodeEntity) r.get("start")).getAllProperties());
            
            final Map<String, Object> expectedEnd = Map.of("joined", LocalDate.of(2017, 8, 21), 
                    "foo", "Jane Doe", "active", true, 
                    "date2", ZonedDateTime.of(2018, 5, 10, 12, 30, 0, 0, ZoneId.of("Europe/Berlin")),
                    "date1", ZonedDateTime.of(2018, 5, 10, 10, 30, 0, 0, ZoneId.of("Europe/Berlin")), 
                    "points", 15L, "__csv_id", "2");
            assertEquals(expectedEnd, ((NodeEntity) r.get("end")).getAllProperties());

            final RelationshipEntity rel = (RelationshipEntity) r.get("rel");
            assertEquals(asList(DurationValue.parse("P14DT16H12M")), asList((DurationValue[]) rel.getProperty("time")));
            final List<Object> expectedTime = asList(OffsetTime.of(15, 30, 0, 0, ZoneOffset.of("+02:00")));
            assertEquals(expectedTime, asList((OffsetTime[]) rel.getProperty("prop2")));
            final List<LocalDateTime> expectedBaz = asList(LocalDateTime.of(2020, 1, 1, 0, 0));
            assertEquals(expectedBaz, asList((LocalDateTime[]) rel.getProperty("baz")));
            final List<LocalTime> expectedBar = asList(LocalTime.of(11, 0, 0));
            assertEquals(expectedBar, asList((LocalTime[]) rel.getProperty("bar")));
            assertEquals(1L, rel.getProperty("foo"));
            final RelationshipEntity relSecond = (RelationshipEntity) r.get("relSecond");
            assertEquals(asList(DurationValue.parse("P5M1.5D")), asList((DurationValue[]) relSecond.getProperty("time")));
            final List<Object> expectedTimeRelSecond = asList(OffsetTime.of(15, 30, 0, 0, ZoneOffset.of("+01:00")));
            assertEquals(expectedTimeRelSecond, asList((OffsetTime[]) relSecond.getProperty("prop2")));
            final List<LocalDateTime> expectedBazRelSecond = asList(LocalDateTime.of(2021, 1, 1, 0, 0));
            assertEquals(expectedBazRelSecond, asList((LocalDateTime[]) relSecond.getProperty("baz")));
            final List<LocalTime> expectedBarRelSecond = asList(LocalTime.of(12, 0, 0));
            assertEquals(expectedBarRelSecond, asList((LocalTime[]) relSecond.getProperty("bar")));
            assertEquals(2L, relSecond.getProperty("foo"));
        });
    }

    @Test
    public void testNodesWithPoints() {
        TestUtil.testCall(db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Point']}], [], {})",
                map("file", "file:/csvPoint.csv"),
                (r) -> {
                    assertEquals(3L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                }
        );
        TestUtil.testResult(db, "MATCH (n:Point) RETURN n ORDER BY n.id", r -> {
            final ResourceIterator<Node> iterator = r.columnAs("n");
            final NodeEntity first = (NodeEntity) iterator.next();
            assertEquals(Values.pointValue(CoordinateReferenceSystem.WGS84, 12.9950357, 55.6121514), first.getProperty("location"));
            final NodeEntity second = (NodeEntity) iterator.next();
            assertEquals(Values.pointValue(CoordinateReferenceSystem.WGS84, -0.1275, 51.507222), second.getProperty("location"));
            final NodeEntity third = (NodeEntity) iterator.next();
            assertEquals(Values.pointValue(CoordinateReferenceSystem.WGS84_3D, -122.313056, 37.554167, 100D), third.getProperty("location"));
        });
    }

    @Test
    public void testCallAsString() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv(" +
                        "[{fileName: 'file:/quoted.csv', labels: ['Person']}], " +
                        "[], " +
                        "{delimiter: '|', arrayDelimiter: ':', quotationCharacter: '\\'', stringIds: false})",
                map(),
                (r) -> {
                    assertEquals(1L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                }
        );

        Assert.assertEquals("John", TestUtil.<String>singleResultFirstColumn(db, "MATCH (n:Person:Student:Employee) RETURN n.name AS name ORDER BY name"));

        long id = TestUtil.<Long>singleResultFirstColumn(db, "MATCH (n:Person:Student:Employee) RETURN n.id AS id ORDER BY id");
        Assert.assertEquals(1L, id);
    }

    @Test
    public void testNodesWithIdSpaces() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                map(
                        "file", "file:/id-idspaces.csv",
                        "config", map("delimiter", '|')
                ),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                }
        );

        List<String> names = TestUtil.firstColumn(db, "MATCH (n:Person) RETURN n.name AS name ORDER BY name");
        assertThat(names, Matchers.containsInAnyOrder("Jane", "John"));
    }

    @Test
    public void testNodesWithIdSpacesWithDoubleDash() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                map(
                        "file", "file://id-idspaces-with-dash.csv",
                        "config", map("delimiter", '|')
                ),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                }
        );

        List<String> names = TestUtil.firstColumn(db, "MATCH (n:Person) RETURN n.name AS name ORDER BY name");
        assertThat(names, Matchers.containsInAnyOrder("Jane", "John"));
    }

    @Test
    public void testNodesWithIdSpacesWithTripleDash() {
        db.executeTransactionally("CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                map(
                        "file", "file:///id-idspaces-with-dash.csv",
                        "config", map("delimiter", '|')
                ),
                Result::resultAsString);

    }

    @Test
    public void testNodesWithIdSpacesWithDash() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                map(
                        "file", "file:/id-idspaces-with-dash.csv",
                        "config", map("delimiter", '|')
                ),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                }
        );

        List<String> names = TestUtil.firstColumn(db, "MATCH (n:Person) RETURN n.name AS name ORDER BY name");
        assertThat(names, Matchers.containsInAnyOrder("Jane", "John"));
    }

    @Test
    public void testCustomLabels() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                map(
                        "file", "file:/label.csv",
                        "config", map("delimiter", '|')
                ),
                (r) -> {
                    assertEquals(1L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                }
        );
        List<String> names = TestUtil.firstColumn(db, "MATCH (n) UNWIND labels(n) AS label RETURN label ORDER BY label");
        assertThat(names, Matchers.contains("Employee", "Person", "Student"));
    }

    @Test
    public void testArray() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                map(
                        "file", "file:/array.csv",
                        "config", map("delimiter", '|')
                ),
                (r) -> {
                    assertEquals(1L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                }
        );

        List<String> names = TestUtil.firstColumn(db, "MATCH (n:Person) UNWIND n.name AS name RETURN name ORDER BY name");
        assertThat(names, Matchers.contains("Alice", "Bob", "John"));
    }

    @Test
    public void testDefaultTypedField() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                map(
                        "file", "file:/typeless.csv",
                        "config", map("delimiter", '|')
                ),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                }
        );

        List<String> names = TestUtil.firstColumn(db, "MATCH (n:Person) RETURN n.name AS name ORDER BY name");
        assertThat(names, Matchers.containsInAnyOrder("Jane", "John"));
    }

    @Test
    public void testCsvWithoutIdField() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                map(
                        "file", "file:/personsWithoutIdField.csv",
                        "config", map()
                ),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                }
        );

        List<String> names = TestUtil.firstColumn(db, "MATCH (n:Person) RETURN n.name AS name ORDER BY name");
        assertEquals(List.of("Jane", "John"), names);
    }

    @Test
    public void testCustomRelationshipTypes() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $nodeFile, labels: ['Person']}], [{fileName: $relFile, type: 'KNOWS'}], $config)",
                map(
                        "nodeFile", "file:/id.csv",
                        "relFile", "file:/rel-type.csv",
                        "config", map("delimiter", '|')
                ),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(2L, r.get("relationships"));
                }
        );

        Assert.assertEquals("John Jane", TestUtil.singleResultFirstColumn(db, "MATCH (p1:Person)-[:FRIENDS_WITH]->(p2:Person) RETURN p1.name + ' ' + p2.name AS pair ORDER BY pair"));
        Assert.assertEquals("Jane John", TestUtil.singleResultFirstColumn(db, "MATCH (p1:Person)-[:KNOWS]->(p2:Person) RETURN p1.name + ' ' + p2.name AS pair ORDER BY pair"));
    }

    @Test
    public void testEmptyDate() {
        TestUtil.testCall(db, "CALL apoc.import.csv([{fileName: 'file:/emptyDate.csv', labels: ['Entity']}], [], {date: {nullValues: ['']}})",
                r -> assertEquals(3L, r.get("nodes")));

        TestUtil.testResult(db, "MATCH (node:Entity:Lab) RETURN node ORDER BY node.id", r -> {
            final Node firstNode = (Node) r.next().get("node");
            final Map<String, Object> expectedFirstNode = Map.of("date", LocalDate.of(2020, 1, 1),
                    "int", 1L, "id", "1", "str", "hello");
            assertEquals(expectedFirstNode, firstNode.getAllProperties());
            final Node secondNode = (Node) r.next().get("node");
            final Map<String, Object> expectedSecondNode = Map.of("date", LocalDate.of(2020, 1, 1), 
                    "int", 2L, "id", "2", "str", "world");
            assertEquals(expectedSecondNode, secondNode.getAllProperties());
            final Node thirdNode = (Node) r.next().get("node");
            assertEquals(Map.of("str", "", "id", "3"), thirdNode.getAllProperties());
            assertFalse(r.hasNext());
        });
    }
    
    @Test
    public void testEmptyInteger() {
        TestUtil.testCall(db, "CALL apoc.import.csv([{fileName: 'file:/emptyInteger.csv', labels: ['entity']}], [], {ignoreBlankString: true})",
                r -> assertEquals(3L, r.get("nodes")));

        TestUtil.testResult(db, "MATCH (node:Thing) RETURN node ORDER BY node.int_attribute", r -> {
                    final Node firstNode = (Node) r.next().get("node");
                    final Map<String, Object> firstProps = firstNode.getAllProperties();
                    assertEquals(1L, firstProps.get("int_attribute"));
                    assertArrayEquals(new long[] { 2L, 3L }, (long[]) firstProps.get("int_attribute_array"));
                    assertArrayEquals(new double[] { 2.3D, 3.5D }, (double[]) firstProps.get("double_attribute_array"), 0);
                    final Node secondNode = (Node) r.next().get("node");
                    final Map<String, Object> secondProps = secondNode.getAllProperties();
                    assertEquals(2L, secondProps.get("int_attribute"));
                    assertArrayEquals(new long[] { 4L, 5L }, (long[]) secondProps.get("int_attribute_array"));
                    assertArrayEquals(new double[] { 2.6D, 3.6D }, (double[]) secondProps.get("double_attribute_array"), 0);
                    final Node thirdNode = (Node) r.next().get("node");
                    final Map<String, Object> thirdProps = thirdNode.getAllProperties();
                    assertNull(thirdProps.get("int_attribute"));
                    assertNull(thirdProps.get("int_attribute_array"));
                    assertNull(thirdProps.get("double_attribute_array"));
                    assertNull(thirdProps.get("str_attribute"));
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void testEmptyArray() {
        TestUtil.testCall(db, "CALL apoc.import.csv([{fileName: 'file:/emptyArray.csv', labels:[]}], [], $conf)",
                map( "conf", map("ignoreEmptyCellArray", true)),
                r -> assertEquals(5L, r.get("nodes")));

        TestUtil.testResult(db, "MATCH (node:Arrays) RETURN node ORDER BY node.id", r -> {
            final Map<String, Object> propsOne = ((Node) r.next().get("node")).getAllProperties();
            assertEquals("normal", propsOne.get("description"));
            assertArrayEquals(new String[] { "a", "b", "c", "d", "e" }, (String[]) propsOne.get("arr"));
            
            final Map<String, Object> propsTwo = ((Node) r.next().get("node")).getAllProperties();
            assertEquals("withNull", propsTwo.get("description"));
            assertFalse(propsTwo.containsKey("arr"));
            
            final Map<String, Object> propsThree = ((Node) r.next().get("node")).getAllProperties();
            assertEquals("withEmptyItem", propsThree.get("description"));
            assertArrayEquals(new String[] { "a", "", "c", "", "e" }, (String[]) propsThree.get("arr"));
            
            final Map<String, Object> propsFour = ((Node) r.next().get("node")).getAllProperties();
            assertEquals("withBlankItem", propsFour.get("description"));
            assertArrayEquals(new String[] { "a", " ", "c", " ", "e" }, (String[]) propsFour.get("arr"));
            
            final Map<String, Object> propsFive = ((Node) r.next().get("node")).getAllProperties();
            assertEquals("withWhiteSpace", propsFive.get("description"));
            assertArrayEquals(new String[] { " " }, (String[]) propsFive.get("arr"));
            
            assertFalse(r.hasNext());
        });
        
    }

    @Test
    public void testRelationshipWithoutIdSpaces() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $nodeFile, labels: ['Person']}], [{fileName: $relFile, type: 'KNOWS'}], $config)",
                map(
                        "nodeFile", "file:/id.csv",
                        "relFile", "file:/rel-on-ids.csv",
                        "config", map("delimiter", '|')
                ),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                }
        );
        Assert.assertEquals("John Jane", TestUtil.singleResultFirstColumn(db, "MATCH (p1:Person)-[:KNOWS]->(p2:Person) RETURN p1.name + ' ' + p2.name AS pair ORDER BY pair"));
    }

    @Test
    public void testRelationshipWithIdSpaces() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $nodeFile, labels: ['Person']}], [{fileName: $relFile, type: 'KNOWS'}], $config)",
                map(
                        "nodeFile", "file:/id-idspaces.csv",
                        "relFile", "file:/rel-on-ids-idspaces.csv",
                        "config", map("delimiter", '|')
                ),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                }
        );
        Assert.assertEquals("John Jane", TestUtil.singleResultFirstColumn(db, "MATCH (p1:Person)-[:KNOWS]->(p2:Person) RETURN p1.name + ' ' + p2.name AS pair ORDER BY pair"));
    }

    @Test
    public void testRelationshipWithCustomIdNames() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv(" +
                        "[" +
                        "  {fileName: $personFile, labels: ['Person']}," +
                        "  {fileName: $companyFile, labels: ['Company']}," +
                        "  {fileName: $universityFile, labels: ['University']}" +
                        "]," +
                        "[" +
                        "  {fileName: $relFile, type: 'AFFILIATED_WITH'}" +
                        "]," +
                        " $config)",
                map(
                        "personFile", "file:/custom-ids-basic-persons.csv",
                        "companyFile", "file:/custom-ids-basic-companies.csv",
                        "universityFile", "file:/custom-ids-basic-unis.csv",
                        "relFile", "file:/custom-ids-basic-affiliated-with.csv",
                        "config", map()
                ),
                (r) -> {
                    assertEquals(4L, r.get("nodes"));
                    assertEquals(2L, r.get("relationships"));
                }
        );

        List<String> pairs = TestUtil.firstColumn(db, "MATCH (p:Person)-[:AFFILIATED_WITH]->(org) RETURN p.name + ' ' + org.name AS pair ORDER BY pair");
        assertThat(pairs, Matchers.contains("Jane Neo4j", "John TU Munich"));
    }

    @Test
    public void testRelationshipWithCustomIdNamesAndIdSpaces() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv(" +
                        "[" +
                        "  {fileName: $personFile, labels: ['Person']}," +
                        "  {fileName: $companyFile, labels: ['Company']}," +
                        "  {fileName: $universityFile, labels: ['University']}" +
                        "]," +
                        "[" +
                        "  {fileName: $relFile, type: 'AFFILIATED_WITH'}" +
                        "]," +
                        " $config)",
                map(
                        "personFile", "file:/custom-ids-idspaces-persons.csv",
                        "companyFile", "file:/custom-ids-idspaces-companies.csv",
                        "universityFile", "file:/custom-ids-idspaces-unis.csv",
                        "relFile", "file:/custom-ids-idspaces-affiliated-with.csv",
                        "config", map()
                ),
                (r) -> {
                    assertEquals(4L, r.get("nodes"));
                    assertEquals(2L, r.get("relationships"));
                }
        );

        List<String> pairs = TestUtil.firstColumn(db, "MATCH (p:Person)-[:AFFILIATED_WITH]->(org) RETURN p.name + ' ' + org.name AS pair ORDER BY pair");
        assertThat(pairs, Matchers.contains("Jane Neo4j", "John TU Munich"));
    }

    @Test
    public void ignoreFieldType() {
        final String query = "CALL apoc.import.csv([{fileName: $nodeFile, labels: ['Person']}], [{fileName: $relFile, type: 'KNOWS'}], $config)";
        final Map<String, Object> config = map("nodeFile", "file:/ignore-nodes.csv",
                "relFile", "file:/ignore-relationships.csv",
                "config", map("delimiter", '|', "batchSize", 1)
        );
        commonAssertionIgnoreFieldType(config, query, true);
    }

    @Test
    public void ignoreFieldTypeWithByteArrayFile() {
        final Map<String, Object> config = map("nodeFile", fileToBinary(new File(BASE_URL_FILES, "ignore-nodes.csv"), CompressionAlgo.GZIP.name()),
                "relFile", fileToBinary(new File(BASE_URL_FILES, "ignore-relationships.csv"), CompressionAlgo.GZIP.name()),
                "config", map("delimiter", '|', "batchSize", 1, COMPRESSION, CompressionAlgo.GZIP.name())
        );
        final String query = "CALL apoc.import.csv([{data: $nodeFile, labels: ['Person']}], [{data: $relFile, type: 'KNOWS'}], $config)";
        commonAssertionIgnoreFieldType(config, query, false);
    }

    @Test
    public void ignoreFieldTypeWithBothBinaryAndFileUrl() throws IOException {
        FileUtils.writeByteArrayToFile(new File(BASE_URL_FILES, "ignore-relationships.csv.zz"),
                fileToBinary(new File(BASE_URL_FILES, "ignore-relationships.csv"), CompressionAlgo.DEFLATE.name()));
        
        final Map<String, Object> config = map("nodeFile", fileToBinary(new File(BASE_URL_FILES, "ignore-nodes.csv"), CompressionAlgo.DEFLATE.name()),
                "relFile", "file:/ignore-relationships.csv.zz",
                "config", map("delimiter", '|', "batchSize", 1, COMPRESSION, CompressionAlgo.DEFLATE.name())
        );
        final String query = "CALL apoc.import.csv([{data: $nodeFile, labels: ['Person']}], [{data: $relFile, type: 'KNOWS'}], $config)";
        commonAssertionIgnoreFieldType(config, query, false);
    }

    private void commonAssertionIgnoreFieldType(Map<String, Object> config, String query, boolean isFile) {
        TestUtil.testCall(
                db,
                query,
                config,
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(2L, r.get("relationships"));
                    assertEquals(isFile ? "progress.csv" : null, r.get("file"));
                    assertEquals(isFile ? "file" : "file/binary", r.get("source"));
                    assertEquals(8L, r.get("properties"));
                }
        );

        List<Long> ages = TestUtil.firstColumn(db, "MATCH (p:Person) RETURN p.age AS age ORDER BY age");
        assertThat(ages, Matchers.contains(25L, 26L));

        List<String> pairs = TestUtil.firstColumn(db, "MATCH (p1:Person)-[k:KNOWS]->(p2:Person)\n" +
                "WHERE p1.lastname IS NULL\n" +
                "  AND p2.lastname IS NULL\n" +
                "  AND k.prop1 IS NULL\n" +
                "RETURN p1.firstname + ' ' + p1.age + ' <' + k.prop2 + '> ' + p2.firstname + ' ' + p2.age AS pair ORDER BY pair"
        );
        assertThat(pairs, Matchers.contains("Jane 26 <6> John 25", "John 25 <3> Jane 26"));
    }

    @Test(expected = QueryExecutionException.class)
    public void testNoDuplicateEndpointsCreated() {
        // some of the endpoints of the edges in 'knows.csv' do not exist,
        // hence this should throw an exception
        db.executeTransactionally("CALL apoc.import.csv([{fileName: $nodeFile, labels: ['Person']}], [{fileName: $relFile, type: 'KNOWS'}], $config)",
                map("nodeFile", "file:/persons.csv",
                    "relFile", "file:/knows.csv",
                    "config", map("stringIds", false)));
    }

    @Test(expected = QueryExecutionException.class)
    public void testIgnoreDuplicateNodes() {
        db.executeTransactionally(
                "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                map(
                        "file", "file:/id-with-duplicates.csv",
                        "config", map("delimiter", '|', "stringIds", false, "ignoreDuplicateNodes", false)
                ));
    }

    @Test
    public void testLoadDuplicateNodes() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                map(
                        "file", "file:/id-with-duplicates.csv",
                        "config", map("delimiter", '|', "stringIds", false, "ignoreDuplicateNodes", true)
                ),
                (r) -> {
                    assertEquals(1L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                }
        );

        Assert.assertEquals("John", TestUtil.singleResultFirstColumn(db, "MATCH (n:Person) RETURN n.name AS name ORDER BY name"));

        long id = TestUtil.<Long>singleResultFirstColumn(db, "MATCH (n:Person) RETURN n.id AS id ORDER BY id");
        Assert.assertEquals(1L, id);
    }

}
