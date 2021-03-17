package apoc.graph;

import apoc.graph.util.GraphsConfig;
import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.SettingImpl;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.util.MapUtil.map;
import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.graphdb.Label.label;

/**
 * @author mh
 * @since 27.05.16
 */
public class GraphsTest {

    private static Map<String,Object> graph = map("name","test","properties",map("answer",42L));

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(SettingImpl.newBuilder("unsupported.dbms.debug.track_cursor_close", SettingValueParsers.BOOL, false).build(), false)
            .withSetting(SettingImpl.newBuilder("unsupported.dbms.debug.trace_cursors", SettingValueParsers.BOOL, false).build(), false);

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db,Graphs.class);
        db.executeTransactionally("CREATE (a:Actor {name:'Tom Hanks'})-[r:ACTED_IN {roles:'Forrest'}]->(m:Movie {title:'Forrest Gump'}) RETURN [a,m] as nodes, [r] as relationships", Collections.emptyMap(),
                result -> {
                    result.stream().forEach(m -> graph.putAll(m));
                    return null;
                });
    }

    @Test
    public void testFromData() throws Exception {
        TestUtil.testCall(db,"MATCH (n)-[r]->(m) CALL apoc.graph.fromData([n,m],[r],'test',{answer:42}) YIELD graph RETURN *",
                r -> assertEquals(graph, r.get("graph")));
    }

    @Test
    public void testFromPath() throws Exception {
        TestUtil.testCall(db,"MATCH path = (n)-[r]->(m) CALL apoc.graph.fromPath(path,'test',{answer:42}) YIELD graph RETURN *",
                r -> assertEquals(graph, r.get("graph")));
    }

    @Test
    public void testFromPaths() {
        // given
        final Map<String, Object> myGraph = map("name","test","properties",map("answer",42L));
        db.executeTransactionally("MATCH (a:Actor {name:'Tom Hanks'}) CREATE (a)-[r:ACTED_IN {roles:'Dott. Henry Goose'}]->(m:Movie {title:'Cloud Atlas'}) RETURN [m] as nodes, [r] as relationships");
        db.executeTransactionally("MATCH p = (a:Actor {name:'Tom Hanks'})-[r:ACTED_IN]->(m:Movie) RETURN reduce(output = [], n in collect(nodes(p)) | output + n) AS nodes, reduce(output = [], r in collect(relationships(p)) | output + r) AS relationships", Collections.emptyMap(),
                result -> {
                    result.stream().flatMap(m -> m.entrySet().stream())
                            .forEach(e -> myGraph.put(e.getKey(), new ArrayList<>(new HashSet<>((Collection) e.getValue()))));
                    return null;
                });
        // when
        TestUtil.testCall(db,"MATCH path = (n)-[r]->(m) WITH collect(path) AS paths CALL apoc.graph.fromPaths(paths,'test',{answer:42}) YIELD graph RETURN *",
                // then
                r -> assertEquals(myGraph, r.get("graph")));

        db.executeTransactionally("MATCH (m:Movie {title:'Cloud Atlas'}) DETACH DELETE m");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testFromDB() throws Exception {
        TestUtil.testCall(db," CALL apoc.graph.fromDB('test',{answer:42})",
                r -> {
                    Map graph1 = new HashMap((Map)r.get("graph"));
                    graph1.put("nodes", Iterables.asList((Iterable) graph1.get("nodes")));
                    graph1.put("relationships", Iterables.asList((Iterable) graph1.get("relationships")));
                    assertEquals(graph, graph1);
                });
    }

    @Test
    public void testFromCypher() throws Exception {
        TestUtil.testCall(db,"CALL apoc.graph.fromCypher('MATCH (n)-[r]->(m) RETURN *',null,'test',{answer:42}) YIELD graph RETURN *",
                r -> assertEquals(graph, r.get("graph")));
    }

    @Test
    public void testFromDocument() throws Exception {
        Map<String, Object> artistGenesisMap = Util.map("type", "artist", "name", "Genesis", "id", 1L);
        Map<String, Object> albumGenesisMap = Util.map("type", "album", "producer", "Jonathan King", "id", 1L, "title", "From Genesis to Revelation");
        Map<String, Object> artistGenesisMapExt = new HashMap() {{
            putAll(artistGenesisMap);
            put("albums", Arrays.asList(albumGenesisMap));
        }};

        TestUtil.testResult(db, "CALL apoc.graph.fromDocument($json, $config) yield graph",
                Util.map("json", JsonUtil.OBJECT_MAPPER.writeValueAsString(artistGenesisMapExt), "config", Util.map("write", true)),
                stringObjectMap -> {
                    Map<String, Object> map = stringObjectMap.next();
                    assertEquals("Graph", ((Map) map.get("graph")).get("name"));
                    Collection<Node> nodes = (Collection<Node>) ((Map) map.get("graph")).get("nodes");
                    Collection<Relationship> relationships = (Collection<Relationship>) ((Map) map.get("graph")).get("relationships");
                    assertEquals(2, nodes.size());
                    assertEquals(1, relationships.size());
                    Iterator<Node> nodeIterator = nodes.iterator();
                    Iterator<Relationship> relationshipIterator = relationships.iterator();

                    Node albumGenesis = nodeIterator.next();
                    assertEquals(asList(label("Album")), albumGenesis.getLabels());
                    assertTrue(albumGenesis.getId() > 0);
                    assertEquals(albumGenesisMap, albumGenesis.getAllProperties());
                    Node artistGenesis = nodeIterator.next();
                    assertEquals(asList(label("Artist")), artistGenesis.getLabels());
                    assertTrue(artistGenesis.getId() > 0);
                    assertEquals(artistGenesisMap, artistGenesis.getAllProperties());
                    Relationship rel = relationshipIterator.next();
                    assertEquals("ALBUMS", rel.getType().name());
                    assertTrue(rel.getId() > 0);
                });
        db.executeTransactionally("MATCH p = (a:Artist)-[r:ALBUMS]->(b:Album) detach delete p");
    }

    @Test
    public void testFromDocumentVirtual() throws Exception {
        Map<String, Object> artistGenesisMap = Util.map("type", "artist", "name", "Genesis", "id", 1L);
        Map<String, Object> albumGenesisMap = Util.map("type", "album", "producer", "Jonathan King", "id", 1L, "title", "From Genesis to Revelation");
        Map<String, Object> artistGenesisMapExt = new HashMap() {{
            putAll(artistGenesisMap);
            put("albums", Arrays.asList(albumGenesisMap));
        }};

        TestUtil.testResult(db, "CALL apoc.graph.fromDocument($json) yield graph", Util.map("json", JsonUtil.OBJECT_MAPPER.writeValueAsString(artistGenesisMapExt)), result -> {
            Map<String, Object> map = result.next();
            assertEquals("Graph", ((Map) map.get("graph")).get("name"));
            Collection<Node> nodes = (Collection<Node>) ((Map) map.get("graph")).get("nodes");
            Collection<Relationship> relationships = (Collection<Relationship>) ((Map) map.get("graph")).get("relationships");
            assertEquals(2, nodes.size());
            assertEquals(1, relationships.size());
            Iterator<Node> nodeIterator = nodes.iterator();
            Iterator<Relationship> relationshipIterator = relationships.iterator();

            Node artistGenesis = nodeIterator.next();
            assertEquals(asList(label("Artist")), artistGenesis.getLabels());
            assertTrue(artistGenesis.getId() < 0);
            assertEquals(artistGenesisMap, artistGenesis.getAllProperties());

            Node albumGenesis = nodeIterator.next();
            assertEquals(asList(label("Album")), albumGenesis.getLabels());
            assertTrue(albumGenesis.getId() < 0);
            assertEquals(albumGenesisMap, albumGenesis.getAllProperties());

            Relationship rel = relationshipIterator.next();
            assertEquals(RelationshipType.withName("ALBUMS"), rel.getType());
            assertTrue(rel.getId() < 0);


        });
    }

    @Test
    public void testFromArrayOfDocumentsVirtual() throws Exception {
        Map<String, Object> artistGenesisMap = Util.map("type", "artist", "name", "Genesis", "id", 1L);
        Map<String, Object> albumGenesisMap = Util.map("type", "album", "producer", "Jonathan King", "id", 1L, "title", "From Genesis to Revelation");
        Map<String, Object> artistGenesisMapExt = new HashMap() {{
            putAll(artistGenesisMap);
            put("albums", Arrays.asList(albumGenesisMap));
        }};
        Map<String, Object> artistDaftPunkMap = Util.map("id", 2L,
                "name", "Daft Punk",
                "type", "artist");
        Map<String, Object> albumDaftPunkMap = Util.map("id", 2L,
                "producer", "Daft Punk",
                "type", "album",
                "title", "Random Access Memory");
        Map<String, Object> artistDaftPunkMapExt = new HashMap() {{
            putAll(artistDaftPunkMap);
            put("albums", Arrays.asList(albumDaftPunkMap));
        }};

        TestUtil.testResult(db, "CALL apoc.graph.fromDocument($json) yield graph", Util.map("json", JsonUtil.OBJECT_MAPPER.writeValueAsString(Arrays.asList(artistGenesisMapExt, artistDaftPunkMapExt))),
                result -> {
                    Map<String, Object> map = result.next();
                    assertEquals("Graph", ((Map) map.get("graph")).get("name"));
                    Collection<Node> nodes = (Collection<Node>) ((Map) map.get("graph")).get("nodes");
                    Collection<Relationship> relationships = (Collection<Relationship>) ((Map) map.get("graph")).get("relationships");
                    assertEquals(4, nodes.size());
                    assertEquals(2, relationships.size());
                    Iterator<Node> nodeIterator = nodes.iterator();
                    Iterator<Relationship> relationshipIterator = relationships.iterator();

                    Node artistGenesis = nodeIterator.next();
                    assertEquals(asList(label("Artist")), artistGenesis.getLabels());
                    assertTrue(artistGenesis.getId() < 0);
                    assertEquals(artistGenesisMap, artistGenesis.getAllProperties());

                    Node artistDaftPunk = nodeIterator.next();
                    assertEquals(asList(label("Artist")), artistDaftPunk.getLabels());
                    assertTrue(artistDaftPunk.getId() < 0);
                    assertEquals(artistDaftPunkMap, artistDaftPunk.getAllProperties());

                    Node albumGenesis = nodeIterator.next();
                    assertEquals(asList(label("Album")), albumGenesis.getLabels());
                    assertTrue(albumGenesis.getId() < 0);
                    assertEquals(albumGenesisMap, albumGenesis.getAllProperties());

                    Node albumDaftPunk = nodeIterator.next();
                    assertEquals(asList(label("Album")), albumDaftPunk.getLabels());
                    assertTrue(albumDaftPunk.getId() < 0);
                    assertEquals(albumDaftPunkMap, albumDaftPunk.getAllProperties());

                    Relationship rel = relationshipIterator.next();
                    assertEquals(RelationshipType.withName("ALBUMS"), rel.getType());
                    assertTrue(rel.getId() < 0);

                    rel = relationshipIterator.next();
                    assertEquals(RelationshipType.withName("ALBUMS"), rel.getType());
                    assertTrue(rel.getId() < 0);
                });
    }

    @Test
    public void testFromArrayOfDocuments() throws Exception {
        Map<String, Object> artistGenesisMap = Util.map("type", "artist", "name", "Genesis", "id", 1L);
        Map<String, Object> albumGenesisMap = Util.map("type", "album", "producer", "Jonathan King", "id", 1L, "title", "From Genesis to Revelation");
        Map<String, Object> artistGenesisMapExt = new HashMap() {{
            putAll(artistGenesisMap);
            put("albums", Arrays.asList(albumGenesisMap));
        }};
        Map<String, Object> artistDaftPunkMap = Util.map("id", 2L,
                "name", "Daft Punk",
                "type", "artist");
        Map<String, Object> albumDaftPunkMap = Util.map("id", 2L,
                "producer", "Daft Punk",
                "type", "album",
                "title", "Random Access Memory");
        Map<String, Object> artistDaftPunkMapExt = new HashMap() {{
            putAll(artistDaftPunkMap);
            put("albums", Arrays.asList(albumDaftPunkMap));
        }};

        TestUtil.testResult(db, "CALL apoc.graph.fromDocument($json, $config) yield graph",
                Util.map("json", JsonUtil.OBJECT_MAPPER.writeValueAsString(Arrays.asList(artistGenesisMapExt, artistDaftPunkMapExt)), "config", Util.map("write", true)),
                result -> {
                    Map<String, Object> map = result.next();
                    assertEquals("Graph", ((Map) map.get("graph")).get("name"));
                    Collection<Node> nodes = (Collection<Node>) ((Map) map.get("graph")).get("nodes");
                    Collection<Relationship> relationships = (Collection<Relationship>) ((Map) map.get("graph")).get("relationships");
                    assertEquals(4, nodes.size());
                    assertEquals(2, relationships.size());
                    Iterator<Node> nodeIterator = nodes.iterator();
                    Iterator<Relationship> relationshipIterator = relationships.iterator();

                    Node albumGenesis = nodeIterator.next();
                    assertEquals(asList(label("Album")), albumGenesis.getLabels());
                    assertTrue(albumGenesis.getId() > 0);
                    assertEquals(albumGenesisMap, albumGenesis.getAllProperties());

                    Node albumDaftPunk = nodeIterator.next();
                    assertEquals(asList(label("Album")), albumDaftPunk.getLabels());
                    assertTrue(albumDaftPunk.getId() > 0);
                    assertEquals(albumDaftPunkMap, albumDaftPunk.getAllProperties());

                    Node artistGenesis = nodeIterator.next();
                    assertEquals(asList(label("Artist")), artistGenesis.getLabels());
                    assertTrue(artistGenesis.getId() > 0);
                    assertEquals(artistGenesisMap, artistGenesis.getAllProperties());

                    Node artistDaftPunk = nodeIterator.next();
                    assertEquals(asList(label("Artist")), artistDaftPunk.getLabels());
                    assertTrue(artistDaftPunk.getId() > 0);
                    assertEquals(artistDaftPunkMap, artistDaftPunk.getAllProperties());

                    Relationship rel = relationshipIterator.next();
                    assertEquals("ALBUMS", rel.getType().name());
                    assertTrue(rel.getId() > 0);
                    rel = relationshipIterator.next();
                    assertEquals("ALBUMS", rel.getType().name());
                    assertTrue(rel.getId() > 0);
                });
        db.executeTransactionally("MATCH p = (a:Artist)-[r:ALBUMS]->(b:Album) detach delete p");
        Long count = TestUtil.singleResultFirstColumn(db, "MATCH p = (a:Artist)-[r:ALBUMS]->(b:Album) RETURN count(p) AS count");
        assertEquals(0L, count.longValue());
    }

    @Test
    public void testFromDocumentVirtualWithCustomIdAndLabel() throws Exception {
        Map<String, Object> genesisMap = Util.map("myCustomType", "artist", "name", "Genesis", "myCustomId", 1L);
        Map<String, Object> albumMap = Util.map("myCustomType", "album", "producer", "Jonathan King", "myCustomId", 1L, "title", "From Genesis to Revelation");
        Map<String, Object> genesisExt = new HashMap() {{
            putAll(genesisMap);
            put("albums", Arrays.asList(albumMap));
        }};
        TestUtil.testResult(db, "CALL apoc.graph.fromDocument($json, $config) yield graph",
                Util.map("json", JsonUtil.OBJECT_MAPPER.writeValueAsString(genesisExt), "config",
                        Util.map("labelField", "myCustomType", "idField", "myCustomId")),
                stringObjectMap -> {
                    Map<String, Object> map = stringObjectMap.next();
                    assertEquals("Graph", ((Map) map.get("graph")).get("name"));
                    Collection<Node> nodes = (Collection<Node>) ((Map) map.get("graph")).get("nodes");
                    Collection<Relationship> relationships = (Collection<Relationship>) ((Map) map.get("graph")).get("relationships");
                    assertEquals(2, nodes.size());
                    assertEquals(1, relationships.size());
                    Iterator<Node> nodeIterator = nodes.iterator();
                    Iterator<Relationship> relationshipIterator = relationships.iterator();

                    Node artistGenesis = nodeIterator.next();
                    assertEquals(asList(label("Artist")), artistGenesis.getLabels());
                    assertTrue(artistGenesis.getId() < 0);
                    assertEquals(genesisMap, artistGenesis.getAllProperties());

                    Node albumGenesis = nodeIterator.next();
                    assertEquals(asList(label("Album")), albumGenesis.getLabels());
                    assertTrue(albumGenesis.getId() < 0);
                    assertEquals(albumMap, albumGenesis.getAllProperties());

                    Relationship rel = relationshipIterator.next();
                    assertEquals(RelationshipType.withName("ALBUMS"), rel.getType());
                    assertTrue(rel.getId() < 0);
                });
    }

    @Test
    public void testFromDocumentVirtualWithDuplicates() throws Exception {
        Map<String, Object> genesisMap = Util.map("type", "artist", "name", "Genesis", "id", 1L);
        Map<String, Object> albumMap = Util.map("type", "album", "producer", "Jonathan King", "id", 1L, "title", "From Genesis to Revelation");
        Map<String, Object> genesisExt = new HashMap() {{
            putAll(genesisMap);
            put("albums", Arrays.asList(albumMap, albumMap));
        }};
        TestUtil.testResult(db, "CALL apoc.graph.fromDocument($json) yield graph",
                Util.map("json", JsonUtil.OBJECT_MAPPER.writeValueAsString(genesisExt)),
                stringObjectMap -> {
                    Map<String, Object> map = stringObjectMap.next();
                    assertEquals("Graph", ((Map) map.get("graph")).get("name"));
                    Collection<Node> nodes = (Collection<Node>) ((Map) map.get("graph")).get("nodes");
                    Collection<Relationship> relationships = (Collection<Relationship>) ((Map) map.get("graph")).get("relationships");
                    assertEquals(2, nodes.size());
                    assertEquals(1, relationships.size());
                    Iterator<Node> nodeIterator = nodes.iterator();
                    Iterator<Relationship> relationshipIterator = relationships.iterator();

                    Node artistGenesis = nodeIterator.next();
                    assertEquals(asList(label("Artist")), artistGenesis.getLabels());
                    assertTrue(artistGenesis.getId() < 0);
                    assertEquals(genesisMap, artistGenesis.getAllProperties());

                    Node albumGenesis = nodeIterator.next();
                    assertEquals(asList(label("Album")), albumGenesis.getLabels());
                    assertTrue(albumGenesis.getId() < 0);
                    assertEquals(albumMap, albumGenesis.getAllProperties());

                    Relationship rel = relationshipIterator.next();
                    assertEquals(RelationshipType.withName("ALBUMS"), rel.getType());
                    assertTrue(rel.getId() < 0);
                });
    }

    @Test
    public void testFromDocumentWithDuplicates() throws Exception {
        Map<String, Object> genesisMap = Util.map("type", "artist", "name", "Genesis", "id", 1L);
        Map<String, Object> albumMap = Util.map("type", "album", "producer", "Jonathan King", "id", 1L, "title", "From Genesis to Revelation");
        Map<String, Object> genesisExt = new HashMap() {{
            putAll(genesisMap);
            put("albums", Arrays.asList(albumMap, albumMap));
        }};
        TestUtil.testResult(db, "CALL apoc.graph.fromDocument($json, $config) yield graph",
                Util.map("json", JsonUtil.OBJECT_MAPPER.writeValueAsString(genesisExt), "config", Util.map("write", true)),
                stringObjectMap -> {
                    Map<String, Object> map = stringObjectMap.next();
                    assertEquals("Graph", ((Map) map.get("graph")).get("name"));
                    Collection<Node> nodes = (Collection<Node>) ((Map) map.get("graph")).get("nodes");
                    Collection<Relationship> relationships = (Collection<Relationship>) ((Map) map.get("graph")).get("relationships");
                    assertEquals(2, nodes.size());
                    assertEquals(1, relationships.size());
                    Iterator<Node> nodeIterator = nodes.iterator();
                    Iterator<Relationship> relationshipIterator = relationships.iterator();

                    Node albumGenesis = nodeIterator.next();
                    assertEquals(asList(label("Album")), albumGenesis.getLabels());
                    assertTrue(albumGenesis.getId() > 0);
                    assertEquals(albumMap, albumGenesis.getAllProperties());
                    Node artistGenesis = nodeIterator.next();
                    assertEquals(asList(label("Artist")), artistGenesis.getLabels());
                    assertTrue(artistGenesis.getId() > 0);
                    assertEquals(genesisMap, artistGenesis.getAllProperties());
                    Relationship rel = relationshipIterator.next();
                    assertEquals("ALBUMS", rel.getType().name());
                    assertTrue(rel.getId() > 0);
                });
        db.executeTransactionally("MATCH p = (a:Artist)-[r:ALBUMS]->(b:Album) detach delete p");
        Long count = TestUtil.singleResultFirstColumn( db, "MATCH p = (a:Artist)-[r:ALBUMS]->(b:Album) RETURN count(p) AS count");
        assertEquals(0L, count.longValue());
    }

    @Test
    public void testCreateVirtualSimpleNodeWithErrorId() throws Exception{
        Map<String, Object> genesisMap = Util.map("type", "artist", "name", "Genesis");
        try {
            TestUtil.testCall(db, "CALL apoc.graph.fromDocument($json) yield graph", Util.map("json", JsonUtil.OBJECT_MAPPER.writeValueAsString(genesisMap)), stringObjectMap -> { });
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals("The object `{\"type\":\"artist\",\"name\":\"Genesis\"}` must have `id` as id-field name", except.getMessage());
        }
    }

    @Test
    public void testValidateDocument() throws Exception {
        List<Object> list = Arrays.asList(Util.map("type", "artist", "name", "Daft Punk"),
                Util.map("id", 1, "type", "artist", "name", "Daft Punk"),
                Util.map("id", 1, "name", "Daft Punk"),
                Util.map("name", "Daft Punk"));

        TestUtil.testResult(db, "CALL apoc.graph.validateDocument($json, $config) yield row",
                Util.map("json", JsonUtil.OBJECT_MAPPER.writeValueAsString(list), "config", Util.map("generateId", false, "defaultLabel", "")), result -> {
                    Map<String, Object> row = (Map<String, Object>) result.next().get("row");
                    assertEquals(0L, row.get("index"));
                    assertEquals("The object `{\"type\":\"artist\",\"name\":\"Daft Punk\"}` must have `id` as id-field name", row.get("message"));
                    row = (Map<String, Object>) result.next().get("row");
                    assertEquals(2L, row.get("index"));
                    assertEquals("The object `{\"id\":1,\"name\":\"Daft Punk\"}` must have `type` as label-field name", row.get("message"));
                    row = (Map<String, Object>) result.next().get("row");
                    assertEquals(3L, row.get("index"));
                    assertEquals("The object `{\"name\":\"Daft Punk\"}` must have `id` as id-field name and `type` as label-field name", row.get("message"));
                    assertFalse("should not have next", result.hasNext());
                });
    }

    @Test
    public void testValidateDocumentWithCutErrorFormatter() throws Exception {
        String json = "{\"quiz\":{\"sport\":{\"q1\":{\"question\":\"Which one is correct team name in NBA?\",\"options\":[\"New York Bulls\",\"Los Angeles Kings\",\"Golden State Warriros\",\"Huston Rocket\"],\"answer\":\"Huston Rocket\"}},\"maths\":{\"q1\":{\"question\":\"5 + 7 = ?\",\"options\":[\"10\",\"11\",\"12\",\"13\"],\"answer\":\"12\"},\"q2\":{\"question\":\"12 - 8 = ?\",\"options\":[\"1\",\"2\",\"3\",\"4\"],\"answer\":\"4\"}}}}";

        Set<String> errors = new HashSet<>();
        errors.add("The object `{\"quiz\":{\"sport\":{\"q1\":{\"question\":\"Which one is correct team name in NBA?\",\"options\":[\"New York Bul...}` must have `id` as id-field name and `type` as label-field name");
        errors.add("The object `{\"sport\":{\"q1\":{\"question\":\"Which one is correct team name in NBA?\",\"options\":[\"New York Bulls\",\"Los...}` must have `id` as id-field name and `type` as label-field name");
        errors.add("The object `{\"q1\":{\"question\":\"Which one is correct team name in NBA?\",\"options\":[\"New York Bulls\",\"Los Angeles ...}` must have `id` as id-field name and `type` as label-field name");
        errors.add("The object `{\"question\":\"Which one is correct team name in NBA?\",\"options\":[\"New York Bulls\",\"Los Angeles Kings\"...}` must have `id` as id-field name and `type` as label-field name");
        errors.add("The object `{\"q1\":{\"question\":\"5 + 7 = ?\",\"options\":[\"10\",\"11\",\"12\",\"13\"],\"answer\":\"12\"},\"q2\":{\"question\":\"12 - ...}` must have `id` as id-field name and `type` as label-field name");
        errors.add("The object `{\"question\":\"5 + 7 = ?\",\"options\":[\"10\",\"11\",\"12\",\"13\"],\"answer\":\"12\"}` must have `id` as id-field name and `type` as label-field name");
        errors.add("The object `{\"question\":\"12 - 8 = ?\",\"options\":[\"1\",\"2\",\"3\",\"4\"],\"answer\":\"4\"}` must have `id` as id-field name and `type` as label-field name");
        TestUtil.testResult(db, "CALL apoc.graph.validateDocument($json, $config) yield row",
                Util.map("json", json, "config", Util.map("generateId", false, "defaultLabel", "")), result -> {
                    Map<String, Object> row = (Map<String, Object>) result.next().get("row");
                    assertEquals(0L, row.get("index"));
                    Set<String> message = messageToSet(row);
                    assertEquals(errors, message);
                    assertFalse("should not have next", result.hasNext());
                });
    }

    private Set<String> messageToSet(Map<String, Object> row) {
        return Stream.of(row.get("message").toString().split("\n"))
                .collect(Collectors.toSet());
    }

    @Test
    public void testFromDocumentWithReusedEntity() throws Exception {
        Map<String, Object> productMap = Util.map("id", 1L, "type", "Console", "name", "Nintendo Switch");
        Map<String, Object> johnMap = Util.map("id", 1L, "type", "User", "name", "John");
        Map<String, Object> janeMap = Util.map("id", 2L, "type", "User", "name", "Jane");
        Map<String, Object> johnExt = new HashMap() {{
            putAll(johnMap);
            put("bought", Arrays.asList(productMap));
        }};
        Map<String, Object> janeExt = new HashMap() {{
            putAll(janeMap);
            put("bought", Arrays.asList(productMap));
        }};
        List<Map<String, Object>> list = Arrays.asList(johnExt, janeExt);

        TestUtil.testResult(db, "CALL apoc.graph.fromDocument($json) yield graph",
                Util.map("json", JsonUtil.OBJECT_MAPPER.writeValueAsString(list)), result -> {
                    Map<String, Object> map = result.next();
                    Collection<Node> nodes = (Collection<Node>) ((Map) map.get("graph")).get("nodes");
                    Collection<Relationship> relationships = (Collection<Relationship>) ((Map) map.get("graph")).get("relationships");
                    assertEquals(3, nodes.size());
                    assertEquals(2, relationships.size());
                    Iterator<Node> nodeIterator = nodes.iterator();
                    Iterator<Relationship> relationshipIterator = relationships.iterator();

                    Node john = nodeIterator.next();
                    assertEquals(asList(label("User")), john.getLabels());
                    assertTrue(john.getId() < 0);
                    assertEquals(johnMap, john.getAllProperties());

                    Node jane = nodeIterator.next();
                    assertEquals(asList(label("User")), jane.getLabels());
                    assertTrue(jane.getId() < 0);
                    assertEquals(janeMap, jane.getAllProperties());

                    Node product = nodeIterator.next();
                    assertEquals(asList(label("Console")), product.getLabels());
                    assertTrue(product.getId() < 0);
                    assertEquals(productMap, product.getAllProperties());

                    Relationship rel = relationshipIterator.next();
                    Node startJohn = rel.getStartNode();
                    assertEquals(asList(label("User")), startJohn.getLabels());
                    assertTrue(startJohn.getId() < 0);
                    assertEquals(johnMap, startJohn.getAllProperties());
                    Node endConsole = rel.getEndNode();
                    assertEquals(asList(label("Console")), endConsole.getLabels());
                    assertTrue(endConsole.getId() < 0);
                    assertEquals(productMap, endConsole.getAllProperties());

                    rel = relationshipIterator.next();
                    assertEquals("BOUGHT", rel.getType().name());
                    Node startJane = rel.getStartNode();
                    assertEquals(asList(label("User")), startJane.getLabels());
                    assertTrue(startJane.getId() < 0);
                    assertEquals(janeMap, startJane.getAllProperties());
                    endConsole = rel.getEndNode();
                    assertEquals(asList(label("Console")), endConsole.getLabels());
                    assertTrue(endConsole.getId() < 0);
                    assertEquals(productMap, endConsole.getAllProperties());
                });
    }

    @Test
    public void testFromDocumentWithNestedStructure() throws Exception {
        Map<String, Object> jamesMap = Util.map("id", 1L, "type", "Father", "name", "James");
        Map<String, Object> johnMap = Util.map("id", 2L, "type", "Father", "name", "John");
        Map<String, Object> robertMap = Util.map("id", 1L, "type", "Person", "name", "Robert");
        Map<String, Object> johnExt = new HashMap() {{
            putAll(johnMap);
            put("son", Arrays.asList(robertMap));
        }};
        Map<String, Object> jamesExt = new HashMap() {{
            putAll(jamesMap);
            put("son", Arrays.asList(johnExt));
        }};
        TestUtil.testResult(db, "CALL apoc.graph.fromDocument($json) yield graph",
                Util.map("json", jamesExt),
                result -> {
                    Map<String, Object> map = result.next();
                    Collection<Node> nodes = (Collection<Node>) ((Map) map.get("graph")).get("nodes");
                    Collection<Relationship> relationships = (Collection<Relationship>) ((Map) map.get("graph")).get("relationships");
                    assertEquals(3, nodes.size());
                    assertEquals(2, relationships.size());
                    Iterator<Node> nodeIterator = nodes.iterator();
                    Iterator<Relationship> relationshipIterator = relationships.iterator();

                    Node john = nodeIterator.next();
                    assertEquals(asList(label("Father")), john.getLabels());
                    assertTrue(john.getId() < 0);
                    assertEquals(johnMap, john.getAllProperties());

                    Node james = nodeIterator.next();
                    assertEquals(asList(label("Father")), james.getLabels());
                    assertTrue(james.getId() < 0);
                    assertEquals(jamesMap, james.getAllProperties());

                    Node robert = nodeIterator.next();
                    assertEquals(asList(label("Person")), robert.getLabels());
                    assertTrue(robert.getId() < 0);
                    assertEquals(robertMap, robert.getAllProperties());

                    Relationship rel = relationshipIterator.next();
                    assertEquals(RelationshipType.withName("SON"), rel.getType());
                    assertEquals(john, rel.getStartNode());
                    assertEquals(robert, rel.getEndNode());

                    rel = relationshipIterator.next();
                    assertEquals(RelationshipType.withName("SON"), rel.getType());
                    assertEquals(james, rel.getStartNode());
                    assertEquals(john, rel.getEndNode());
                });
    }

    @Test
    public void testFromDocumentWithReusedEntityToGraph() throws Exception {
        Map<String, Object> productMap = Util.map("id", 1L, "type", "Console", "name", "Nintendo Switch");
        Map<String, Object> johnMap = Util.map("id", 1L, "type", "User", "name", "John");
        Map<String, Object> janeMap = Util.map("id", 2L, "type", "User", "name", "Jane");
        Map<String, Object> johnExt = new HashMap() {{
            putAll(johnMap);
            put("bought", Arrays.asList(productMap));
        }};
        Map<String, Object> janeExt = new HashMap() {{
            putAll(janeMap);
            put("bought", Arrays.asList(productMap));
        }};
        List<Map<String, Object>> list = Arrays.asList(johnExt, janeExt);

        db.executeTransactionally("CALL apoc.graph.fromDocument($json, $config) yield graph",
                Util.map("json", JsonUtil.OBJECT_MAPPER.writeValueAsString(list), "config", Util.map("write", true)));

        Long count = TestUtil.singleResultFirstColumn(db, "MATCH p = (a:User{id: 1})-[r:BOUGHT]->(c:Console)<-[r1:BOUGHT]-(b:User{id: 2}) RETURN count(p) AS count");
        assertEquals(1L, count.longValue());

        db.executeTransactionally("MATCH p = (a:User)-[r:BOUGHT]->(c:Console)<-[r1:BOUGHT]-(b:User) detach delete p");

        count = TestUtil.singleResultFirstColumn(db, "MATCH p = (a:User)-[r:BOUGHT]->(c:Console)<-[r1:BOUGHT]-(b:User) RETURN count(p) AS count");
        assertEquals(0L, count.longValue());
    }

    @Test
    public void testCreateVirtualSimpleNodeWithErrorType() throws Exception{
        Map<String, Object> genesisMap = Util.map("id", 1L, "name", "Genesis");
        try {
            TestUtil.testCall(db, "CALL apoc.graph.fromDocument($json, $config) yield graph",
                    Util.map("json", JsonUtil.OBJECT_MAPPER.writeValueAsString(genesisMap),
                            "config", map("defaultLabel", "")),
                    result -> { });
        } catch (QueryExecutionException e) {
            Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertEquals("The object `{\"id\":1,\"name\":\"Genesis\"}` must have `type` as label-field name", rootCause.getMessage());
        }
    }

    @Test
    public void testCreateVirtualSimpleNode() throws Exception {
        Map<String, Object> genesisMap = Util.map("id", 1L, "type", "artist", "name", "Genesis");
        TestUtil.testResult(db, "CALL apoc.graph.fromDocument($json) yield graph", Util.map("json", JsonUtil.OBJECT_MAPPER.writeValueAsString(genesisMap)), result -> {
            Map<String, Object> map = result.next();
            assertEquals("Graph", ((Map) map.get("graph")).get("name"));
            Collection<Node> nodes = (Collection<Node>) ((Map) map.get("graph")).get("nodes");
            assertEquals(1, nodes.size());
            Node node = nodes.iterator().next();
            assertEquals(asList(label("Artist")), node.getLabels());
            assertTrue(node.getId() < 0);
            assertEquals(genesisMap, node.getAllProperties());
            assertFalse("should not have next", result.hasNext());
        });
    }

    @Test
    public void testCreateVirtualSimpleNodeFromCypherMap() {
        Map<String, Object> genesisMap = Util.map("id", 1L, "type", "artist", "name", "Genesis");
        TestUtil.testResult(db, "CALL apoc.graph.fromDocument($json) yield graph", Util.map("json", genesisMap), result -> {
            Map<String, Object> map = result.next();
            assertEquals("Graph", ((Map) map.get("graph")).get("name"));
            Collection<Node> nodes = (Collection<Node>) ((Map) map.get("graph")).get("nodes");
            assertEquals(1, nodes.size());
            Node node = nodes.iterator().next();
            assertEquals(asList(label("Artist")), node.getLabels());
            assertTrue(node.getId() < 0);
            assertEquals(genesisMap, node.getAllProperties());
            assertFalse("should not have next", result.hasNext());
        });
    }

    @Test
    public void testCreateVirtualSimpleNodeFromCypherList() {
        Map<String, Object> genesisMap = Util.map("id", 1L, "type", "artist", "name", "Genesis");
        Map<String, Object> daftPunkMap = Util.map("id", 2L, "type", "artist", "name", "Daft Punk");
        TestUtil.testResult(db, "CALL apoc.graph.fromDocument($json) yield graph", Util.map("json", Arrays.asList(genesisMap, daftPunkMap)), result -> {
            Map<String, Object> map = result.next();
            assertEquals("Graph", ((Map) map.get("graph")).get("name"));
            Collection<Node> nodes = (Collection<Node>) ((Map) map.get("graph")).get("nodes");
            assertEquals(2, nodes.size());
            Iterator<Node> nodeIterator = nodes.iterator();
            Node node = nodeIterator.next();
            assertEquals(asList(label("Artist")), node.getLabels());
            assertTrue(node.getId() < 0);
            assertEquals(genesisMap, node.getAllProperties());
            node = nodeIterator.next();
            assertEquals(asList(label("Artist")), node.getLabels());
            assertTrue(node.getId() < 0);
            assertEquals(daftPunkMap, node.getAllProperties());
            assertFalse("should not have next", result.hasNext());
        });
    }

    @Test
    public void testCreateVirtualNodeArray() throws Exception {
        Map<String, Object> genesisMap = Util.map("type", "artist", "name", "Genesis", "id", 1L);
        Map<String, Object> album1Map = Util.map("type", "album", "producer", "Jonathan King", "id", 1L, "title", "From Genesis to Revelation");
        Map<String, Object> album2Map = Util.map("producer", "Jonathan King", "id", 2L, "title", "John Anthony", "type", "album");
        Map<String, Object> genesisExt = new HashMap() {{
            putAll(genesisMap);
            put("albums", Arrays.asList(album1Map, album2Map));
        }};

        TestUtil.testResult(db, "CALL apoc.graph.fromDocument($json) yield graph",
                Util.map("json", JsonUtil.OBJECT_MAPPER.writeValueAsString(genesisExt)),
                result -> {
                    Map<String, Object> map = result.next();
                    assertEquals("Graph", ((Map) map.get("graph")).get("name"));
                    Collection<Node> nodes = (Collection<Node>) ((Map) map.get("graph")).get("nodes");
                    Collection<Relationship> relationships = (Collection<Relationship>) ((Map) map.get("graph")).get("relationships");
                    assertEquals(3, nodes.size());
                    assertEquals(2, relationships.size());
                    Iterator<Node> nodeIterator = nodes.iterator();
                    Iterator<Relationship> relationshipIterator = relationships.iterator();
                    Node artist = nodeIterator.next();
                    assertEquals(asList(label("Artist")), artist.getLabels());
                    assertTrue(artist.getId() < 0);
                    assertEquals(genesisMap, artist.getAllProperties());
                    Node album = nodeIterator.next();
                    assertEquals(asList(label("Album")), album.getLabels());
                    assertTrue(album.getId() < 0);
                    assertEquals(album1Map, album.getAllProperties());
                    Node album2 = nodeIterator.next();
                    assertEquals(asList(label("Album")), album2.getLabels());
                    assertTrue(album2.getId() < 0);
                    assertEquals(album2Map, album2.getAllProperties());
                    Relationship rel = relationshipIterator.next();
                    assertEquals(RelationshipType.withName("ALBUMS"), rel.getType());
                    assertTrue(rel.getId() < 0);
                    rel = relationshipIterator.next();
                    assertEquals(RelationshipType.withName("ALBUMS"), rel.getType());
                    assertTrue(rel.getId() < 0);
                });
    }

    @Test
    public void shouldCreatePrimitiveArray() throws Exception {
        Map<String, Object> genesisMap = Util.map("type", "artist", "name", "Genesis", "id", 1L,
                "years", new Long[]{1967L, 1998L, 1999L, 2000L, 2006L},
                "members", new String[]{"Tony Banks","Mike Rutherford","Phil Collins"});

        TestUtil.testResult(db, "CALL apoc.graph.fromDocument($json) yield graph",
                Util.map("json", JsonUtil.OBJECT_MAPPER.writeValueAsString(genesisMap)),
                result -> {
                    Map<String, Object> map = result.next();
                    assertEquals("Graph", ((Map) map.get("graph")).get("name"));
                    Collection<Node> nodes = (Collection<Node>) ((Map) map.get("graph")).get("nodes");
                    Iterator<Node> nodeIterator = nodes.iterator();
                    Node artist = nodeIterator.next();
                    assertEquals(asList(label("Artist")), artist.getLabels());
                    assertTrue(artist.getId() < 0);
                    assertEquals(genesisMap.get("type"), artist.getProperty("type"));
                    assertEquals(genesisMap.get("name"), artist.getProperty("name"));
                    assertEquals(genesisMap.get("id"), artist.getProperty("id"));
                    assertArrayEquals((Long[]) genesisMap.get("years"), (Long[]) artist.getProperty("years"));
                    assertArrayEquals((String[]) genesisMap.get("members"), (String[]) artist.getProperty("members"));
                });
    }

    @Test
    public void shouldFindDuplicatesWithValidation() {
        Map<String, Object> child = Util.map("key", "childKey");
        List<Map<String, Object>> data = Arrays.asList(
                Util.map("key", "value", "key1", "Foo"), // index 0
                Util.map("key", "value", "key1", "Foo"), // index 1 -> dup of index 0
                Util.map("key", "value1", "key1", "Foo", "child", child), // index 2
                Util.map("key", "value1", "key1", "Foo"), // index 3
                Util.map("key", "value2", "key1", "Foo", "childA", child), // index 4 -> dup of "child" field at index 1
                Util.map("key", "value2", "key1", "Foo", "childA", Util.map("child", child)) // index 5 -> dup of "child" field at index 1
        );

        TestUtil.testResult(db, "CALL apoc.graph.validateDocument($json, $config)",
                Util.map("json", data, "config", Util.map("generateId", false, "defaultLabel", "")), result -> {
                    Map<String, Object> row = (Map<String, Object>) result.next().get("row");
                    assertEquals(0L, row.get("index"));
                    Set<String> errors = new HashSet<>();
                    Set<String> messages = messageToSet(row);
                    errors.add("The object `{\"key1\":\"Foo\",\"key\":\"value\"}` has duplicate at lines [1]");
                    errors.add("The object `{\"key1\":\"Foo\",\"key\":\"value\"}` must have `id` as id-field name and `type` as label-field name");
                    assertEquals(errors, messages);

                    row = (Map<String, Object>) result.next().get("row");
                    assertEquals(1L, row.get("index"));
                    assertEquals("The object `{\"key1\":\"Foo\",\"key\":\"value\"}` must have `id` as id-field name and `type` as label-field name", row.get("message"));

                    row = (Map<String, Object>) result.next().get("row");
                    assertEquals(2L, row.get("index"));
                    errors = new HashSet<>();
                    messages = messageToSet(row);
                    errors.add("The object `{\"key\":\"childKey\"}` has duplicate at lines [4,5]");
                    errors.add("The object `{\"key1\":\"Foo\",\"key\":\"value1\",\"child\":{\"key\":\"childKey\"}}` must have `id` as id-field name and `type` as label-field name");
                    errors.add("The object `{\"key\":\"childKey\"}` must have `id` as id-field name and `type` as label-field name");
                    assertEquals(errors, messages);

                    row = (Map<String, Object>) result.next().get("row");
                    assertEquals(3L, row.get("index"));
                    assertEquals("The object `{\"key1\":\"Foo\",\"key\":\"value1\"}` must have `id` as id-field name and `type` as label-field name", row.get("message"));

                    row = (Map<String, Object>) result.next().get("row");
                    assertEquals(4L, row.get("index"));
                    errors = new HashSet<>();
                    messages = messageToSet(row);
                    errors.add("The object `{\"key1\":\"Foo\",\"key\":\"value2\",\"childA\":{\"key\":\"childKey\"}}` must have `id` as id-field name and `type` as label-field name");
                    errors.add("The object `{\"key\":\"childKey\"}` must have `id` as id-field name and `type` as label-field name");
                    assertEquals(errors, messages);

                    row = (Map<String, Object>) result.next().get("row");
                    assertEquals(5L, row.get("index"));
                    errors = new HashSet<>();
                    messages = messageToSet(row);
                    errors.add("The object `{\"key1\":\"Foo\",\"key\":\"value2\",\"childA\":{\"child\":{\"key\":\"childKey\"}}}` must have `id` as id-field name and `type` as label-field name");
                    errors.add("The object `{\"child\":{\"key\":\"childKey\"}}` must have `id` as id-field name and `type` as label-field name");
                    errors.add("The object `{\"key\":\"childKey\"}` must have `id` as id-field name and `type` as label-field name");
                    assertEquals(errors, messages);

                    assertFalse("should not have next", result.hasNext());
                });
    }

    @Test
    public void shouldCreateTheGraphMappingObjectAccordingToThePattern() {
        GraphsConfig.GraphMapping mapping = GraphsConfig.GraphMapping.from("Person{*,@sizes}");
        assertEquals(Arrays.asList("sizes"), mapping.getValueObjects());
        assertEquals(Collections.emptyList(), mapping.getProperties());
        assertEquals(Collections.emptyList(), mapping.getIds());
        assertTrue(mapping.isAllProps());
        assertEquals(Arrays.asList("Person"), mapping.getLabels());

        mapping = GraphsConfig.GraphMapping.from("Book{!title, released}");
        assertEquals(Collections.emptyList(), mapping.getValueObjects());
        assertEquals(Arrays.asList("released", "title"), mapping.getProperties());
        assertEquals(Arrays.asList("title"), mapping.getIds());
        assertEquals(Arrays.asList("Book"), mapping.getLabels());
        assertFalse(mapping.isAllProps());
    }

    @Test
    public void shouldCreateFlattenValueObjectAndNewNodes() {
        String[] strings = {"foo", "bar"};
        Map<String, Object> book1 = map("title", "Flow My Tears, the Policeman Said", "released", 1974);
        Map<String, Object> book2 = map("title", "The man in the High Castle", "released", 1962);
        Map<String, Object> inputMap = map("id", 1, "type", "Person", "name", "Andrea",
                "sizes", map("weight", map("value", 70, "um", "Kg"), "height", map("value", 174, "um", "cm"),
                        "array", strings),
                "books", Arrays.asList(book1,book2)
        );
        Map<String, Object> expectedMap = map("id", 1, "type", "Person", "name", "Andrea",
                "sizes.weight.value", 70, "sizes.weight.um", "Kg", "sizes.height.value", 174, "sizes.height.um", "cm");

        TestUtil.testResult(db, "CALL apoc.graph.fromDocument($json, $config) yield graph",
                Util.map("json", inputMap, "config", map("mappings", map("$", "Person:Reader{*,@sizes}", "$.books", "Book{!title, released}"))), result -> {
                    Map<String, Object> map = result.next();
                    assertEquals("Graph", ((Map) map.get("graph")).get("name"));
                    Collection<Node> nodes = (Collection<Node>) ((Map) map.get("graph")).get("nodes");
                    Collection<Relationship> rels = (Collection<Relationship>) ((Map) map.get("graph")).get("relationships");
                    assertEquals(3, nodes.size());
                    assertEquals(2, rels.size());
                    Iterator<Node> nodeIterator = nodes.iterator();
                    Iterator<Relationship> relationshipIterator = rels.iterator();

                    Node person = nodeIterator.next();
                    assertEquals(asList(label("Person"), label("Reader")), person.getLabels());
                    assertTrue(person.getId() < 0);
                    Map<String, Object> allProperties = new HashMap<>(person.getAllProperties());
                    allProperties.remove("sizes.array"); // we test only non-array properties
                    assertEquals(expectedMap, allProperties);
                    assertArrayEquals(strings, (String[]) person.getProperty("sizes.array"));

                    Node book1Node = nodeIterator.next();
                    assertEquals(asList(label("Book")), book1Node.getLabels());
                    assertTrue(book1Node.getId() < 0);
                    allProperties = book1Node.getAllProperties();
                    assertEquals(book1, allProperties);

                    Node book2Node = nodeIterator.next();
                    assertEquals(asList(label("Book")), book2Node.getLabels());
                    assertTrue(book2Node.getId() < 0);
                    allProperties = book2Node.getAllProperties();
                    assertEquals(book2, allProperties);

                    Relationship rel = relationshipIterator.next();
                    assertEquals(RelationshipType.withName("BOOKS"), rel.getType());
                    assertTrue(rel.getId() < 0);
                    assertEquals(person, rel.getStartNode());
                    assertEquals(book1Node, rel.getEndNode());

                    rel = relationshipIterator.next();
                    assertEquals(RelationshipType.withName("BOOKS"), rel.getType());
                    assertTrue(rel.getId() < 0);
                    assertEquals(person, rel.getStartNode());
                    assertEquals(book2Node, rel.getEndNode());

                    assertFalse("should not have next", result.hasNext());
                });
    }

    @Test
    public void testDeeplyNestedStructures() throws IOException {
        String json = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream("deeplyNestedObject.json"), Charset.forName("UTF-8"));
        TestUtil.testResult(db, "CALL apoc.graph.fromDocument($json, $config) yield graph",
                map("json", json, "config", map("idField", "name")), result -> {
                    Map<String, Object> map = result.next();
                    assertEquals("Graph", ((Map) map.get("graph")).get("name"));
                    Collection<Node> nodes = (Collection<Node>) ((Map) map.get("graph")).get("nodes");
                    Map<String, List<Node>> nodeMap = nodes.stream()
                            .collect(Collectors.groupingBy(e -> e.getLabels().iterator().next().name()));

                    assertEquals(1, nodeMap.get("Project").size());
                    assertEquals(3, nodeMap.get("Company").size());
                    assertEquals(5, nodeMap.get("Worker").size());
                    assertEquals(19, nodeMap.get("Task").size());


                    Collection<Relationship> rels = (Collection<Relationship>) ((Map) map.get("graph")).get("relationships");
                    Map<String, List<Relationship>> relMap = rels.stream()
                            .collect(Collectors.groupingBy(e -> String.format("(%s)-[%s]-(%s)",
                                    e.getStartNode().getLabels().iterator().next().name(),
                                    e.getType().name(),
                                    e.getEndNode().getLabels().iterator().next().name())));
                    assertEquals(5, relMap.get("(Project)-[TASKS]-(Task)").size());
                    assertEquals(19, relMap.get("(Task)-[WORKER]-(Worker)").size());
                    assertEquals(14, relMap.get("(Task)-[SUBTASKS]-(Task)").size());
                    assertEquals(5, relMap.get("(Worker)-[COMPANY]-(Company)").size());
                });
    }

    @Test
    public void testIncludeMappingsAsProperties() throws IOException {
        Map<String, Object> json = map("id", 1,
                "text", "Text",
                "data", "02-11-2019",
                "user", map("id", 1, "screenName", "conker84"),
                "geo", map("latitude", 11.45, "longitude", -12.3));
        TestUtil.testResult(db, "CALL apoc.graph.fromDocument($json, $config) yield graph",
                map("json", json, "config",
                        map("skipValidation", false, "mappings", map("$", "Tweet{!id, text}", "$.user", "User{!id, screenName}"))), result -> {
                    Map<String, Object> map = result.next();
                    assertEquals("Graph", ((Map) map.get("graph")).get("name"));
                    Collection<Node> nodes = (Collection<Node>) ((Map) map.get("graph")).get("nodes");
                    Map<String, List<Node>> nodeMap = nodes.stream()
                            .collect(Collectors.groupingBy(e -> e.getLabels().iterator().next().name()));

                    final List<Node> tweets = nodeMap.getOrDefault("Tweet", Collections.emptyList());
                    assertEquals(1, tweets.size());
                    assertEquals(new HashSet<>(Arrays.asList("id", "text")), tweets.get(0).getAllProperties().keySet());
                    final List<Node> users = nodeMap.getOrDefault("User", Collections.emptyList());
                    assertEquals(1, users.size());
                    assertEquals(new HashSet<>(Arrays.asList("id","screenName")), users.get(0).getAllProperties().keySet());

                    Collection<Relationship> rels = (Collection<Relationship>) ((Map) map.get("graph")).get("relationships");
                    Map<String, List<Relationship>> relMap = rels.stream()
                            .collect(Collectors.groupingBy(e -> String.format("(%s)-[%s]-(%s)",
                                    e.getStartNode().getLabels().iterator().next().name(),
                                    e.getType().name(),
                                    e.getEndNode().getLabels().iterator().next().name())));
                    assertEquals(1, relMap.get("(Tweet)-[USER]-(User)").size());
                });
    }

    @Test
    public void testValidationCustomIdAsProperties() throws IOException {
        Map<String, Object> json = map("id", 1,
                "text", "Text",
                "data", "02-11-2019",
                "user", map("id", 1, "screenName", "conker84"),
                "geo", map("latitude", 11.45, "longitude", -12.3));
        TestUtil.testResult(db, "CALL apoc.graph.fromDocument($json, $config) yield graph",
                map("json", json, "config",
                        map("skipValidation", false, "idField", "foo", "mappings", map("$", "Tweet{!id, text}", "$.user", "User{!screenName}"))), result -> {
                    Map<String, Object> map = result.next();
                    assertEquals("Graph", ((Map) map.get("graph")).get("name"));
                    Collection<Node> nodes = (Collection<Node>) ((Map) map.get("graph")).get("nodes");
                    Map<String, List<Node>> nodeMap = nodes.stream()
                            .collect(Collectors.groupingBy(e -> e.getLabels().iterator().next().name()));

                    final List<Node> tweets = nodeMap.getOrDefault("Tweet", Collections.emptyList());
                    assertEquals(1, tweets.size());
                    assertEquals(new HashSet<>(Arrays.asList("id", "text")), tweets.get(0).getAllProperties().keySet());
                    final List<Node> users = nodeMap.getOrDefault("User", Collections.emptyList());
                    assertEquals(1, users.size());
                    assertEquals(new HashSet<>(Arrays.asList("screenName")), users.get(0).getAllProperties().keySet());

                    Collection<Relationship> rels = (Collection<Relationship>) ((Map) map.get("graph")).get("relationships");
                    Map<String, List<Relationship>> relMap = rels.stream()
                            .collect(Collectors.groupingBy(e -> String.format("(%s)-[%s]-(%s)",
                                    e.getStartNode().getLabels().iterator().next().name(),
                                    e.getType().name(),
                                    e.getEndNode().getLabels().iterator().next().name())));
                    assertEquals(1, relMap.get("(Tweet)-[USER]-(User)").size());
                });
    }
}
