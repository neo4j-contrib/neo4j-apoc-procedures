package apoc.path;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;

public class LabelSequenceTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(newBuilder( "unsupported.dbms.debug.track_cursor_close", BOOL, false ).build(), false)
            .withSetting(newBuilder( "unsupported.dbms.debug.trace_cursors", BOOL, false ).build(), false);

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, PathExplorer.class);
        db.executeTransactionally("create (s:Start{name:'start'})-[:REL]->(:A{name:'a'})-[:REL]->(:B{name:'b'})-[:REL]->(:A:C{name:'ac'})-[:REL]->(:B:A{name:'ba'})-[:REL]->(:D:A{name:'da'})");
    }

    @Test
    public void testBasicSequence() throws Throwable {
        String query = "MATCH (s:Start {name: 'start'}) CALL apoc.path.subgraphNodes(s,{labelFilter:'A,B', beginSequenceAtStart:false}) yield node return collect(distinct node.name) as nodes";
        TestUtil.testCall(db, query, (row) -> {
            List<String> names = (List<String>) row.get("nodes");
            assertEquals(6L,names.size());
            assertTrue(names.containsAll(Arrays.asList("start", "a", "b", "ac", "ba", "da")));
        });
    }

    @Test
    public void testNoMatchWhenImproperlyStartingSequenceAtStart() throws Throwable {
        String query = "MATCH (s:Start {name: 'start'}) CALL apoc.path.subgraphNodes(s,{labelFilter:'A,B', beginSequenceAtStart:true, filterStartNode:true}) yield node return collect(distinct node.name) as nodes";
        TestUtil.testCall(db, query, (row) -> {
            List<String> names = (List<String>) row.get("nodes");
            assertEquals(0L,names.size());
        });
    }

    @Test
    public void testMatchWhenProperlyStartingSequenceAtStart() throws Throwable {
        String query = "MATCH (s:Start {name: 'start'}) CALL apoc.path.subgraphNodes(s,{labelFilter:'Start,A,B', beginSequenceAtStart:true, filterStartNode:true}) yield node return collect(distinct node.name) as nodes";
        TestUtil.testCall(db, query, (row) -> {
            List<String> names = (List<String>) row.get("nodes");
            assertEquals(3L,names.size());
            assertTrue(names.containsAll(Arrays.asList("start", "a", "b")));
        });
    }

    @Test
    public void testSequenceWithBlacklist() throws Throwable {
        String query = "MATCH (s:Start {name: 'start'}) CALL apoc.path.subgraphNodes(s,{labelFilter:'A|-C,B', beginSequenceAtStart:false}) yield node return collect(distinct node.name) as nodes";
        TestUtil.testCall(db, query, (row) -> {
            List<String> names = (List<String>) row.get("nodes");
            assertEquals(3L, names.size());
            assertTrue(names.containsAll(Arrays.asList("start", "a", "b")));
        });
    }

    @Test
    public void testSequenceWithTerminatorNode() throws Throwable {
        String query = "MATCH (s:Start {name: 'start'}) CALL apoc.path.subgraphNodes(s,{labelFilter:'/A|C,B', beginSequenceAtStart:false}) yield node return collect(distinct node.name) as nodes";
        TestUtil.testCall(db, query, (row) -> {
            List<String> names = (List<String>) row.get("nodes");
            assertEquals(1L, names.size());
            assertTrue(names.containsAll(Arrays.asList("a")));
        });
    }

    @Test
    public void testSequenceWithEndNode() throws Throwable {
        String query = "MATCH (s:Start {name: 'start'}) CALL apoc.path.subgraphNodes(s,{labelFilter:'>A|C,B', beginSequenceAtStart:false}) yield node return collect(distinct node.name) as nodes";
        TestUtil.testCall(db, query, (row) -> {
            List<String> names = (List<String>) row.get("nodes");
            assertEquals(3L, names.size());
            assertTrue(names.containsAll(Arrays.asList("a", "ac", "da")));
        });
    }

    @Test
    public void testSequenceWithEndNodeAndLimit() throws Throwable {
        String query = "MATCH (s:Start {name: 'start'}) CALL apoc.path.subgraphNodes(s,{labelFilter:'>A|C,B', beginSequenceAtStart:false, limit:2}) yield node return collect(distinct node.name) as nodes";
        TestUtil.testCall(db, query, (row) -> {
            List<String> names = (List<String>) row.get("nodes");
            assertEquals(2L, names.size());
            assertTrue(names.containsAll(Arrays.asList("a", "ac")));
        });
    }

    @Test
    public void testSequenceWithTerminatorNodeAsStartNodeWithoutFilteringStartNode() throws Throwable {
        String query = "MATCH (a:A {name: 'a'}) CALL apoc.path.subgraphNodes(a,{labelFilter:'/A, B', filterStartNode:false}) yield node return collect(distinct node.name) as nodes";
        TestUtil.testCall(db, query, (row) -> {
            List<String> names = (List<String>) row.get("nodes");
            assertEquals(1L, names.size());
            assertTrue(names.containsAll(Arrays.asList("ac")));
        });
    }

    @Test
    public void testSequenceWithTerminatorNodeAndEndNode() throws Throwable {
        String query = "MATCH (s:Start {name: 'start'}) CALL apoc.path.subgraphNodes(s,{labelFilter:'>A|C|/A:C, B', beginSequenceAtStart:false}) yield node return collect(distinct node.name) as nodes";
        TestUtil.testCall(db, query, (row) -> {
            List<String> names = (List<String>) row.get("nodes");
            assertEquals(2L, names.size());
            assertTrue(names.containsAll(Arrays.asList("a", "ac")));
        });
    }

    @Test
    public void testSequenceWithTerminatorNodeWhenUsingMinLevel() throws Throwable {
        String query = "MATCH (s:Start {name: 'start'}) CALL apoc.path.expandConfig(s,{labelFilter:'/A, B', beginSequenceAtStart:false, minLevel:3}) yield path return collect(distinct last(nodes(path)).name) as nodes";
        TestUtil.testCall(db, query, (row) -> {
            List<String> names = (List<String>) row.get("nodes");
            assertEquals(1L, names.size());
            assertTrue(names.containsAll(Arrays.asList("ac")));
        });
    }
}
