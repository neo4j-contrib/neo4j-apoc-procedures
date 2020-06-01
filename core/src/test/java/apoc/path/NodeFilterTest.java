package apoc.path;

import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test path expanders with node filters (where we already have the nodes that will be used for the whitelist, blacklist, endnodes, and terminator nodes
 */
public class NodeFilterTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    public NodeFilterTest() throws Exception {
    }

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, PathExplorer.class);
        String movies = Util.readResourceFile("movies.cypher");
        try (Transaction tx = db.beginTx()) {
            tx.execute(movies);
            tx.commit();
        }
    }

    @After
    public void removeOtherLabels() {
        db.executeTransactionally("OPTIONAL MATCH (c:Western) REMOVE c:Western WITH DISTINCT 1 as ignore OPTIONAL MATCH (c:Blacklist) REMOVE c:Blacklist");
    }

    @Test
    public void testTerminatorNodesPruneExpansion() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}) " +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', terminatorNodes:[gene, clint]}) yield node " +
                        "return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                });
    }

    @Test
    public void testEndNodesContinueTraversal() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western WITH c WHERE c.name = 'Clint Eastwood' SET c:Blacklist");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}) " +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', endNodes:[gene, clint]}) yield node " +
                        "return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(2, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                    node = (Node) maps.get(1).get("node");
                    assertEquals("Clint Eastwood", node.getProperty("name"));
                });
    }

    @Test
    public void testEndNodesAndTerminatorNodesReturnExpectedResults() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western WITH c WHERE c.name = 'Clint Eastwood' SET c:Blacklist");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}) " +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', endNodes:[gene], terminatorNodes:[clint]}) yield node " +
                        "return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(2, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                    node = (Node) maps.get(1).get("node");
                    assertEquals("Clint Eastwood", node.getProperty("name"));
                });
    }

    @Test
    public void testEndNodesAndTerminatorNodesReturnExpectedResultsReversed() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western WITH c WHERE c.name = 'Clint Eastwood' SET c:Blacklist");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}) " +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', terminatorNodes:[gene], endNodes:[clint]}) yield node " +
                        "return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                });
    }

    @Test
    public void testTerminatorNodesOverruleEndNodes1() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western WITH c WHERE c.name = 'Clint Eastwood' SET c:Blacklist");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}) " +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', terminatorNodes:[gene], endNodes:[clint, gene]}) yield node " +
                        "return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                });
    }

    @Test
    public void testTerminatorNodesOverruleEndNodes2() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western WITH c WHERE c.name = 'Clint Eastwood' SET c:Blacklist");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}) " +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', terminatorNodes:[gene, clint], endNodes:[clint, gene]}) yield node " +
                        "return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                });
    }

    @Test
    public void testEndNodesWithTerminationFilterPrunesExpansion() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}) " +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western', endNodes:[clint, gene]}) yield node " +
                        "return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                });
    }

    @Test
    public void testTerminatorNodesWithEndNodeFilterPrunesExpansion() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}) " +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'>Western', terminatorNodes" +
                        ":[clint, gene]}) yield node " +
                        "return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                });
    }

    @Test
    public void testBlacklistNodesInPathPrunesPath() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}), (unforgiven:Movie{title:'Unforgiven'}) " +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'>Western', blacklistNodes:[unforgiven]}) yield node " +
                        "return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                });
    }

    @Test
    public void testBlacklistNodesWithEndNodesPrunesPath() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}), (unforgiven:Movie{title:'Unforgiven'}) " +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', endNodes:[clint, gene], blacklistNodes:[unforgiven]}) yield node " +
                        "return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                });
    }

    @Test
    public void testBlacklistNodesOverridesAllOtherNodeFilters() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}), (unforgiven:Movie{title:'Unforgiven'}),  (replacements:Movie{title:'The Replacements'})\n" +
                        "WITH k, clint, gene, [k, gene, clint, unforgiven, replacements] as whitelist\n" +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', terminatorNodes:[clint], endNodes:[clint], whitelistNodes:whitelist, blacklistNodes:[clint]}) yield node return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(0, maps.size());
                });
    }

    @Test
    public void testWhitelistNodes() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}), (unforgiven:Movie{title:'Unforgiven'}),  (replacements:Movie{title:'The Replacements'})\n" +
                        "WITH k, clint, gene, [k, gene, clint, unforgiven, replacements] as whitelist\n" +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', endNodes:[clint, gene], whitelistNodes:whitelist}) yield node return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(2, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                    node = (Node) maps.get(1).get("node");
                    assertEquals("Clint Eastwood", node.getProperty("name"));
                });
    }

    @Test
    public void testWhitelistNodesIncludesEndNodes() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}), (unforgiven:Movie{title:'Unforgiven'}),  (replacements:Movie{title:'The Replacements'})\n" +
                        "WITH k, clint, gene, [k, gene, unforgiven, replacements] as whitelist\n" +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', endNodes:[clint, gene], whitelistNodes:whitelist}) yield node return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(2, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                    node = (Node) maps.get(1).get("node");
                    assertEquals("Clint Eastwood", node.getProperty("name"));
                });
    }

    @Test
    public void testWhitelistNodesIncludesTerminatorNodes() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}), (unforgiven:Movie{title:'Unforgiven'}),  (replacements:Movie{title:'The Replacements'}) \n" +
                        "WITH k, clint, gene, [k, gene, unforgiven, replacements] as whitelist \n" +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', terminatorNodes:[clint], whitelistNodes:whitelist}) yield node return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Clint Eastwood", node.getProperty("name"));
                });
    }

    @Test
    public void testWhitelistNodesAndLabelFiltersMustAgreeToInclude1() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (replacements:Movie{title:'The Replacements'}) \n" +
                        "WITH k, [k, replacements] as whitelist \n" +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'+Person', whitelistNodes:whitelist}) yield node return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Keanu Reeves", node.getProperty("name"));
                });
    }

    @Test
    public void testWhitelistNodesAndLabelFiltersMustAgreeToInclude2() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (replacements:Movie{title:'The Replacements'}) \n" +
                        "WITH k, [k] as whitelist \n" +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'+Person|+Movie', whitelistNodes:whitelist}) yield node return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Keanu Reeves", node.getProperty("name"));
                });
    }



    @Test
    public void testStartNodeWithFilterStartNodeFalseIgnoresBlacklistNodes() throws Throwable {
        String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expandConfig(m,{blacklistNodes:[m], maxLevel:2, filterStartNode:false}) yield path return count(*) as c";
        TestUtil.testCall(db, query, (row) -> assertEquals(44L,row.get("c")));
    }

    @Test
    public void testBlacklistNodesStillAppliesBelowMinLevel() throws Throwable {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (clint:Person {name:'Clint Eastwood'}), (unforgiven:Movie{title:'Unforgiven'}),  (replacements:Movie{title:'The Replacements'})\n" +
                        "WITH k, clint, unforgiven\n" +
                        "CALL apoc.path.expandConfig(clint, {uniqueness:'NODE_GLOBAL', relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', terminatorNodes:[k], blacklistNodes:[unforgiven], minLevel:3}) yield path return last(nodes(path))",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(0, maps.size());
                });
    }

    @Test
    public void testStartNodeWithFilterStartNodeFalseIgnoresWhitelistNodesFilter() throws Throwable {
        String query = "MATCH (m:Movie {title: 'The Matrix'}), (k:Person {name:'Keanu Reeves'}) CALL apoc.path.expandConfig(m,{whitelistNodes:[k], minLevel:1, maxLevel:1, filterStartNode:false}) yield path return count(*) as c";
        TestUtil.testCall(db, query, (row) -> assertEquals(1L,row.get("c")));
    }

    @Test
    public void testWhitelistNodesStillAppliesBelowMinLevel() throws Throwable {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (replacements:Movie{title:'The Replacements'}), (gene:Person {name:'Gene Hackman'}), (unforgiven:Movie{title:'Unforgiven'}), (clint:Person {name:'Clint Eastwood'})\n" +
                        "WITH clint, k, [k, replacements, gene, clint] as whitelist\n" +
                        "CALL apoc.path.expandConfig(clint, {uniqueness:'NODE_GLOBAL', relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', terminatorNodes:[k], whitelistNodes:whitelist, minLevel:3}) yield path return last(nodes(path))",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(0, maps.size());
                });
    }

    @Test
    public void testStartNodeWithFilterStartNodeFalseIgnoresTerminatorNodesFilter() throws Throwable {
        String query = "MATCH (m:Movie {title: 'The Matrix'}), (k:Person {name:'Keanu Reeves'}) CALL apoc.path.expandConfig(m,{terminatorNodes:[m,k], maxLevel:2, filterStartNode:false}) yield path WITH k, path WHERE last(nodes(path)) = k RETURN count(*) as c";
        TestUtil.testCall(db, query, (row) -> assertEquals(1L,row.get("c")));
    }

    @Test
    public void testTerminatorNodesDoesNotApplyBelowMinLevel() throws Throwable {
        String query = "MATCH (m:Movie {title: 'The Matrix'}), (k:Person {name:'Keanu Reeves'}) CALL apoc.path.expandConfig(m,{terminatorNodes:[m,k], minLevel:1, maxLevel:2, filterStartNode:true}) yield path WITH k, path WHERE last(nodes(path)) = k RETURN count(*) as c";
        TestUtil.testCall(db, query, (row) -> assertEquals(1L,row.get("c")));
    }

    @Test
    public void testStartNodeWithFilterStartNodeFalseIgnoresEndNodesFilter() throws Throwable {
        String query = "MATCH (m:Movie {title: 'The Matrix'}), (k:Person {name:'Keanu Reeves'}) CALL apoc.path.expandConfig(m,{endNodes:[m,k], maxLevel:2, filterStartNode:false}) yield path RETURN count(DISTINCT last(nodes(path))) as c";
        TestUtil.testCall(db, query, (row) -> assertEquals(1L,row.get("c")));
    }

    @Test
    public void testEndNodesDoesNotApplyBelowMinLevel() throws Throwable {
        String query = "MATCH (m:Movie {title: 'The Matrix'}), (k:Person {name:'Keanu Reeves'}) CALL apoc.path.expandConfig(m,{endNodes:[m,k], minLevel:1, maxLevel:2, filterStartNode:true}) yield path RETURN count(DISTINCT last(nodes(path))) as c";
        TestUtil.testCall(db, query, (row) -> assertEquals(1L,row.get("c")));
    }
}
