package apoc.neo4j.docker;

import apoc.bolt.Bolt;
import apoc.cypher.Cypher;
import apoc.export.cypher.ExportCypher;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestContainerUtil.ApocPackage;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.time.LocalTime;
import java.time.OffsetTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.Values.isoDuration;
import static org.neo4j.driver.Values.point;

/**
 * @author AgileLARUS
 * @since 29.08.17
 */
public class BoltTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static Neo4jContainerExtension neo4jContainer;

    @BeforeClass
    public static void setUp() throws Exception {
        neo4jContainer = createEnterpriseDB(List.of(ApocPackage.EXTENDED), true).withInitScript("init_neo4j_bolt.cypher");
        neo4jContainer.start();
        TestUtil.registerProcedure(db, Bolt.class, ExportCypher.class, Cypher.class);
    }

    @AfterClass
    public static void tearDown() {
        neo4jContainer.close();
    }
    
    @Test
    public void testNeo4jBolt() {
        final String uriDbBefore4 = System.getenv("URI_DB_BEFORE_4");
        Assume.assumeNotNull(uriDbBefore4);
        TestUtil.testCall(db, "CALL apoc.bolt.load($uri, 'RETURN 1', {}, {databaseName: null})", 
                Map.of("uri", uriDbBefore4),
                r -> assertEquals(Map.of("1", 1L), r.get("row")));
    }

    @Test
    public void testLoadNodeVirtual() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + getBoltUrl() + ",'match(p:Person {name:$name}) return p', {name:'Michael'}, {virtual:true})", r -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                Node node = (Node) row.get("p");
                assertEquals(true, node.hasLabel(Label.label("Person")));
                assertEquals("Michael", node.getProperty("name"));
                assertEquals("Jordan", node.getProperty("surname"));
                assertEquals(true, node.getProperty("state"));
                assertEquals(54L, node.getProperty("age"));
            });
    }

    @Test
    public void testLoadNodesVirtual() throws Exception {
            TestUtil.testResult(db, "call apoc.bolt.load(" + getBoltUrl() + ",'match(n) return n', {}, {virtual:true})", r -> {
                assertNotNull(r);
                Map<String, Object> row = r.next();
                Map result = (Map) row.get("row");
                Node node = (Node) result.get("n");
                assertEquals(true, node.hasLabel(Label.label("Person")));
                assertEquals("Michael", node.getProperty("name"));
                assertEquals("Jordan", node.getProperty("surname"));
                assertEquals(true, node.getProperty("state"));
                assertEquals(54L, node.getProperty("age"));
                row = r.next();
                result = (Map) row.get("row");
                node = (Node) result.get("n");
                assertEquals(true, node.hasLabel(Label.label("Person")));
                assertEquals("Tom", node.getProperty("name"));
                assertEquals("Burton", node.getProperty("surname"));
                assertEquals(23L, node.getProperty("age"));
                row = r.next();
                result = (Map) row.get("row");
                node = (Node) result.get("n");
                assertEquals(true, node.hasLabel(Label.label("Person")));
                assertEquals("John", node.getProperty("name"));
                assertEquals("William", node.getProperty("surname"));
                assertEquals(22L, node.getProperty("age"));

                r.close();
            });
    }

    @Test
    public void testLoadPathVirtual() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + getBoltUrl() + ",'MATCH (neo) WHERE neo.surname = $surnameNode  MATCH path= (neo)-[r:KNOWS*..3]->(other) return path', {surnameNode:'Burton'}, {virtual:true})", r -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                List<Object> path = (List<Object>) row.get("path");
                Node start = (Node) path.get(0);
                assertEquals(true, start.hasLabel(Label.label("Person")));
                assertEquals("Tom", start.getProperty("name"));
                assertEquals("Burton", start.getProperty("surname"));
                Node end = (Node) path.get(2);
                assertEquals(true, end.hasLabel(Label.label("Person")));
                assertEquals("John", end.getProperty("name"));
                assertEquals("William", end.getProperty("surname"));
                Relationship rel = (Relationship) path.get(1);
                assertEquals("KNOWS", rel.getType().name());
                assertEquals(2016L, rel.getProperty("since"));
                assertEquals(OffsetTime.parse("12:50:35.556+01:00"), rel.getProperty("time"));
            });
    }

    @Test
    public void testLoadRel() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + getBoltUrl() + ",'match(p)-[r]->(c) return r limit 1', {}, {virtual:true})", r -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                Relationship rel = (Relationship) row.get("r");
                assertEquals("KNOWS", rel.getType().name());
                assertEquals(2016L, rel.getProperty("since"));
                assertEquals(OffsetTime.parse("12:50:35.556+01:00"), rel.getProperty("time"));
            });
    }

    @Test
    public void testLoadRelsAndNodes() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + getBoltUrl() + ",'match(p:Person {surname:$surnameP})-[r]->(c:Person {surname:$surnameC}) return *', {surnameP:\"Burton\", surnameC:\"William\"}, {virtual:true})", r -> {
                Map result = (Map) r.get("row");
                Node node = (Node) result.get("p");
                assertEquals(true, node.hasLabel(Label.label("Person")));
                assertEquals("Tom", node.getProperty("name"));
                assertEquals("Burton", node.getProperty("surname"));
                assertEquals(23L, node.getProperty("age"));
                node = (Node) result.get("c");
                assertEquals(true, node.hasLabel(Label.label("Person")));
                assertEquals("John", node.getProperty("name"));
                assertEquals("William", node.getProperty("surname"));
                assertEquals(22L, node.getProperty("age"));
            });
    }

    @Test
    public void testLoadNullParams() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load("+getBoltUrl()+",\"match(p:Person {name:'Michael'}) return p\")", r -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                Map<String, Object> node = (Map<String, Object>) row.get("p");
                assertTrue(node.containsKey("entityType"));
                assertEquals("NODE", node.get("entityType"));
                assertTrue(node.containsKey("properties"));
                Map<String, Object> properties = (Map<String, Object>) node.get("properties");
                assertEquals("Michael", properties.get("name"));
                assertEquals("Jordan", properties.get("surname"));
                assertEquals(54L, properties.get("age"));
                assertEquals(true, properties.get("state"));
            });
    }

    @Test
    public void testLoadNode() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + getBoltUrl() + ",'match (p:Person {name:$name}) return p', {name:'Michael'})", r -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                Map<String, Object> node = (Map<String, Object>) row.get("p");
                assertTrue(node.containsKey("entityType"));
                assertEquals("NODE", node.get("entityType"));
                assertTrue(node.containsKey("properties"));
                Map<String, Object> properties = (Map<String, Object>) node.get("properties");
                assertEquals("Michael", properties.get("name"));
                assertEquals("Jordan", properties.get("surname"));
                assertEquals(54L, properties.get("age"));
                assertEquals(true, properties.get("state"));
            });
    }

    @Test
    public void testLoadScalarSingleReusult() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + getBoltUrl() + ",'match (n:Person {name:$name}) return n.age as Age', {name:'Michael'})", (r) -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                assertTrue(row.containsKey("Age"));
                assertEquals(54L, row.get("Age"));
            });
    }

    @Test
    public void testLoadMixedContent() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + getBoltUrl() + ",'match (n:Person {name:$name}) return n.age, n.name, n.state', {name:'Michael'})",
                    r -> {
                        assertNotNull(r);
                        Map<String, Object> row = (Map<String, Object>) r.get("row");
                        assertTrue(row.containsKey("n.age"));
                        assertEquals(54L, row.get("n.age"));
                        assertTrue(row.containsKey("n.name"));
                        assertEquals("Michael", row.get("n.name"));
                        assertTrue(row.containsKey("n.state"));
                        assertEquals(true, row.get("n.state"));
                    });
    }

    @Test
    public void testLoadList() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + getBoltUrl() + ",'match (p:Person {name:$name})  with collect({personName:p.name}) as rows return rows', {name:'Michael'})", r -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                List<Collections> p = (List<Collections>) row.get("rows");
                Map<String, Object> result = (Map<String, Object>) p.get(0);
                assertTrue(result.containsKey("personName"));
                assertEquals("Michael", result.get("personName"));
            });
    }

    @Test
    public void testLoadMap() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + getBoltUrl() + ",'match (p:Person {name:$name})  with p,collect({personName:p.name}) as rows return p{.*, rows:rows}', {name:'Michael'})", r -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                Map<String, Object> p = (Map<String, Object>) row.get("p");
                assertTrue(p.containsKey("name"));
                assertEquals("Michael", p.get("name"));
                assertTrue(p.containsKey("age"));
                assertEquals(54L, p.get("age"));
                assertTrue(p.containsKey("surname"));
                assertEquals("Jordan", p.get("surname"));
                assertTrue(p.containsKey("state"));
                assertEquals(true, p.get("state"));
            });
    }

    @Test
    public void testLoadPath() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + getBoltUrl() + ",'MATCH path= (neo)-[r:KNOWS*..3]->(other) where neo.surname = $surnameNode return path', {surnameNode: 'Burton'}, {})", r -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                List<Object> path = (List<Object>) row.get("path");
                Map<String, Object> startNode = (Map<String, Object>) path.get(0);
                assertEquals("NODE", startNode.get("entityType"));
                assertEquals(Arrays.asList("Person"), startNode.get("labels"));
                Map<String, Object> startNodeProperties = (Map<String, Object>) startNode.get("properties");
                assertEquals("Tom", startNodeProperties.get("name"));
                assertEquals("Burton", startNodeProperties.get("surname"));
                Map<String, Object> endNode = (Map<String, Object>) path.get(2);
                assertEquals("NODE", startNode.get("entityType"));
                assertEquals(Arrays.asList("Person"), startNode.get("labels"));
                Map<String, Object> endNodeProperties = (Map<String, Object>) endNode.get("properties");
                assertEquals("John", endNodeProperties.get("name"));
                assertEquals("William", endNodeProperties.get("surname"));
                Map<String, Object> rel = (Map<String, Object>) path.get(1);
                assertEquals("RELATIONSHIP", rel.get("entityType"));
                assertEquals("KNOWS", rel.get("type"));
                Map<String, Object> relProperties = (Map<String, Object>) rel.get("properties");
                assertEquals(2016L, relProperties.get("since"));
                assertEquals(OffsetTime.parse("12:50:35.556+01:00"), relProperties.get("time"));
            });
    }

    @Test
    public void testLoadRels() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + getBoltUrl() + ",'match (n)-[r]->(c) return r as rel limit 1', {})", (r) -> {
                assertNotNull(r);
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                Map<String, Object> rel = (Map<String, Object>) row.get("rel");
                assertEquals(1L, rel.get("start"));
                assertEquals(2L, rel.get("end"));
                assertEquals("RELATIONSHIP", rel.get("entityType"));
                assertEquals("KNOWS", rel.get("type"));
                Map<String, Object> properties = (Map<String, Object>) rel.get("properties");
                assertEquals(2016L, properties.get("since"));
                assertEquals(OffsetTime.parse("12:50:35.556+01:00"), properties.get("time"));
            });
    }

    @Test
    public void testExecuteCreateNodeStatistic() throws Exception {
            TestUtil.testResult(db, "call apoc.bolt.execute(" + getBoltUrl() + ",'create(n:Node {name:$name})', {name:'Node1'}, {statistics:true})", Collections.emptyMap(),
                    r -> {
                        assertNotNull(r);
                        Map<String, Object> row = r.next();
                        Map result = (Map) row.get("row");
                        assertEquals(1L, (long) Util.toLong(result.get("nodesCreated")));
                        assertEquals(1L, (long) Util.toLong(result.get("labelsAdded")));
                        assertEquals(1L, (long) Util.toLong(result.get("propertiesSet")));
                        assertEquals(false, r.hasNext());
                    });
    }

    @Test
    public void testExecuteCreateVirtualNode() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.execute(" + getBoltUrl() + ",'create(n:Node {name:$name}) return n', {name:'Node1'}, {virtual:true})",
                    r -> {
                        assertNotNull(r);
                        Map<String, Object> row = (Map<String, Object>) r.get("row");
                        Node node = (Node) row.get("n");
                        assertEquals(true, node.hasLabel(Label.label("Node")));
                        assertEquals("Node1", node.getProperty("name"));
                    });
    }

    @Test
    public void testLoadNoVirtual() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load("+getBoltUrl()+",\"match(p:Person {name:'Michael'}) return p\", {}, {virtual:false, test:false})",
                    r -> {
                        assertNotNull(r);
                        Map<String, Object> row = (Map<String, Object>) r.get("row");
                        Map<String, Object> node = (Map<String, Object>) row.get("p");
                        assertTrue(node.containsKey("entityType"));
                        assertEquals("NODE", node.get("entityType"));
                        assertTrue(node.containsKey("properties"));
                        Map<String, Object> properties = (Map<String, Object>) node.get("properties");
                        assertEquals("Michael", properties.get("name"));
                        assertEquals("Jordan", properties.get("surname"));
                        assertEquals(54L, properties.get("age"));
                        assertEquals(true, properties.get("state"));
                    });
    }

    @Test
    public void testLoadNodeWithDriverConfig() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + getBoltUrl() + ",\"match(p:Person {name:$nameP}) return p\", {nameP:'Michael'}, " +
                            "{driverConfig:{logging:'WARNING', encryption: false,logLeakedSessions:true, maxIdleConnectionPoolSize:10, idleTimeBeforeConnectionTest:-1," +
                            " routingFailureLimit: 1, routingRetryDelayMillis:500, connectionTimeoutMillis:500, maxRetryTimeMs:30000 , trustStrategy:'TRUST_ALL_CERTIFICATES'}})",
                    r -> {
                        assertNotNull(r);
                        Map<String, Object> row = (Map<String, Object>) r.get("row");
                        Map<String, Object> node = (Map<String, Object>) row.get("p");
                        assertTrue(node.containsKey("entityType"));
                        assertEquals("NODE", node.get("entityType"));
                        assertTrue(node.containsKey("properties"));
                        Map<String, Object> properties = (Map<String, Object>) node.get("properties");
                        assertEquals("Michael", properties.get("name"));
                        assertEquals("Jordan", properties.get("surname"));
                        assertEquals(54L, properties.get("age"));
                        assertEquals(true, properties.get("state"));
                    });
    }

    @Test
    public void testLoadBigPathVirtual() throws Exception {
            TestUtil.testCall(db, "call apoc.bolt.load(" + getBoltUrl() + ",'MATCH path= (neo)-[r:KNOWS*3]->(other) WHERE neo.surname = $surnameNode return path', {surnameNode: 'Loagan'}, {virtual:true})", r -> {
                Map<String, Object> row = (Map<String, Object>) r.get("row");
                List<Object> path = (List<Object>) row.get("path");
                Node start = (Node) path.get(0);
                assertEquals(true, start.hasLabel(Label.label("Person")));
                assertEquals("Tom", start.getProperty("name"));
                assertEquals("Loagan", start.getProperty("surname"));
                assertEquals(isoDuration(5, 1, 43200, 0).asIsoDuration(), start.getProperty("duration"));
                Relationship rel = (Relationship) path.get(1);
                assertEquals("KNOWS", rel.getType().name());
                assertEquals(LocalTime.parse("12:50:35.556"), rel.getProperty("since"));
                assertEquals(point(4326, 56.7, 12.78).asPoint(), rel.getProperty("born"));
                Node end = (Node) path.get(2);
                assertEquals(true, end.hasLabel(Label.label("Person")));
                assertEquals("John", end.getProperty("name"));
                assertEquals("Green", end.getProperty("surname"));
                assertEquals(point(7203, 2.3, 4.5).asPoint(), end.getProperty("born"));
                start = (Node) path.get(3);
                assertEquals(true, start.hasLabel(Label.label("Person")));
                assertEquals("John", start.getProperty("name"));
                assertEquals("Green", start.getProperty("surname"));
                assertEquals(point(7203, 2.3, 4.5).asPoint(), end.getProperty("born"));
                rel = (Relationship) path.get(4);
                assertEquals("KNOWS", rel.getType().name());
                assertEquals(OffsetTime.parse("12:50:35.556+01:00"), rel.getProperty("since"));
                assertEquals(point(4979, 56.7, 12.78, 100.0).asPoint(), rel.getProperty("born"));
                end = (Node) path.get(5);
                assertEquals(true, end.hasLabel(Label.label("Person")));
                assertEquals("Jim", end.getProperty("name"));
                assertEquals("Brown", end.getProperty("surname"));
                start = (Node) path.get(6);
                assertEquals(true, start.hasLabel(Label.label("Person")));
                assertEquals("Jim", start.getProperty("name"));
                assertEquals("Brown", start.getProperty("surname"));
                rel = (Relationship) path.get(7);
                assertEquals("KNOWS", rel.getType().name());
                assertEquals(2013L, rel.getProperty("since"));
                end = (Node) path.get(8);
                assertEquals(true, end.hasLabel(Label.label("Person")));
                assertEquals("Anne", end.getProperty("name"));
                assertEquals("Olsson", end.getProperty("surname"));
                assertEquals(point(9157,2.3, 4.5, 1.2).asPoint(), end.getProperty("born"));
            });
    }

    @Test
    public void testLoadFromLocal() {
        String localStatement = "RETURN 'foobar' AS foobar";
        String remoteStatement = "CREATE (n: TestLoadFromLocalNode { m: foobar })";
        final Map<String, Object> map = Util.map(
                "url", getBoltUrl().replaceAll("'", ""),
                "localStatement", localStatement,
                "remoteStatement", remoteStatement,
                "config", Util.map("readOnly", false));
        db.executeTransactionally("call apoc.bolt.load.fromLocal($url, $localStatement, $remoteStatement, $config) YIELD row return row", map);
        final long remoteCount = neo4jContainer.getSession().readTransaction(tx ->
                (long) tx.run("MATCH (n: TestLoadFromLocalNode { m: 'foobar' }) RETURN count(n) AS count").next().asMap().get("count"));
        assertEquals(1L, remoteCount);
    }

    @Test
    public void testLoadFromLocalStream() {
        String localStatement = "RETURN \"CREATE (n: TestLoadFromLocalStream)\" AS statement";
        final Map<String, Object> map = Util.map(
                "url", getBoltUrl().replaceAll("'", ""),
                "localStatement", localStatement,
                "remoteStatement", null,
                "config", Util.map("readOnly", false, "streamStatements", true));
        db.executeTransactionally("call apoc.bolt.load.fromLocal($url, $localStatement, $remoteStatement, $config)", map);
        final long remoteCount = neo4jContainer.getSession().readTransaction(tx ->
                (long) tx.run("MATCH (n: TestLoadFromLocalStream) RETURN count(n) AS count").next().asMap().get("count"));
        assertEquals(1L, remoteCount);
    }

    private String getBoltUrl() {
        return String.format("'bolt://neo4j:%s@%s:%s'",
                TestContainerUtil.password,
                neo4jContainer.getContainerIpAddress(),
                neo4jContainer.getMappedPort(7687));
    }
}