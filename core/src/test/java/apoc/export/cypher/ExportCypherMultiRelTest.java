package apoc.export.cypher;

import apoc.cypher.Cypher;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.export.cypher.ExportCypherTestUtils.CLEANUP_EMPTY;
import static apoc.export.cypher.ExportCypherTestUtils.CLEANUP_SMALL_BATCH;
import static apoc.export.cypher.ExportCypherTestUtils.NODES_MULTI_RELS;
import static apoc.export.cypher.ExportCypherTestUtils.NODES_MULTI_RELS_ADD_STRUCTURE;
import static apoc.export.cypher.ExportCypherTestUtils.NODES_MULTI_REL_CREATE;
import static apoc.export.cypher.ExportCypherTestUtils.NODES_UNWIND;
import static apoc.export.cypher.ExportCypherTestUtils.NODES_UNWIND_ADD_STRUCTURE;
import static apoc.export.cypher.ExportCypherTestUtils.NODES_UNWIND_UPDATE_STRUCTURE;
import static apoc.export.cypher.ExportCypherTestUtils.RELSUPDATE_STRUCTURE_2;
import static apoc.export.cypher.ExportCypherTestUtils.RELS_ADD_STRUCTURE_MULTI_RELS;
import static apoc.export.cypher.ExportCypherTestUtils.RELS_MULTI_RELS;
import static apoc.export.cypher.ExportCypherTestUtils.RELS_UNWIND_MULTI_RELS;
import static apoc.export.cypher.ExportCypherTestUtils.RELS_UNWIND_UPDATE_ALL_MULTI_RELS;
import static apoc.export.cypher.ExportCypherTestUtils.SCHEMA_UPDATE_STRUCTURE_MULTI_REL;
import static apoc.export.cypher.ExportCypherTestUtils.SCHEMA_WITH_UNIQUE_IMPORT_ID;
import static apoc.export.cypher.formatter.CypherFormatterUtils.UNIQUE_ID_REL;
import static apoc.export.util.ExportConfig.RELS_WITH_TYPE_KEY;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;

public class ExportCypherMultiRelTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            // as for the GraphRefactoringTest, we need to set unsupported.dbms.debug.trace_cursors=false 
            // because, with DbmsRule, using structures like StreamSupport.stream(node.getRelationships(dir, type).spliterator(), false)..etc.. an 'IllegalStateException: Closeable RelationshipTraversalCursor[..] was not closed!' is thrown. 
            // With a non-test instance it works without this config
            .withSetting(newBuilder("unsupported.dbms.debug.track_cursor_close", BOOL, false).build(), false);

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, ExportCypher.class, Cypher.class);
        db.executeTransactionally("create (pers:Person {name: 'MyName'})-[:WORKS_FOR {id: 1}]->(proj:Project {a: 1}), \n" +
                "(pers)-[:WORKS_FOR {id: 2}]->(proj), " +
                "(pers)-[:WORKS_FOR {id: 2}]->(proj), \n" +
                "(pers)-[:WORKS_FOR {id: 3}]->(proj), \n" +
                "(pers)-[:WORKS_FOR {id: 4}]->(proj), \n" +
                "(pers)-[:WORKS_FOR {id: 5}]->(proj), \n" +
                "(pers)-[:IS_TEAM_MEMBER_OF {name: 'aaa'}]->(:Team {name: 'one'}),\n" +
                "(pers)-[:IS_TEAM_MEMBER_OF {name: 'eee'}]->(:Team {name: 'two'})");
        
    }
    
    @Test
    public void updateAllOptimizationNone() {
        String expectedCypherStatement = NODES_MULTI_RELS + SCHEMA_WITH_UNIQUE_IMPORT_ID + RELS_MULTI_RELS + CLEANUP_SMALL_BATCH;
        final Map<String, Object> map = withoutOptimization(
                map("cypherFormat", "updateAll"));

        testsCommon(expectedCypherStatement, map);
    }

    @Test
    public void createOptimizationNone() {
        String expectedCypherStatement = NODES_MULTI_REL_CREATE + SCHEMA_WITH_UNIQUE_IMPORT_ID + RELS_ADD_STRUCTURE_MULTI_RELS + CLEANUP_SMALL_BATCH;
        final Map<String, Object> map = withoutOptimization(
                map("cypherFormat", "create"));

        testsCommon(expectedCypherStatement, map);
    }

    @Test
    public void addStructureOptimizationNone() {
        String expectedCypherStatement = NODES_MULTI_RELS_ADD_STRUCTURE + SCHEMA_UPDATE_STRUCTURE_MULTI_REL + RELS_ADD_STRUCTURE_MULTI_RELS + CLEANUP_EMPTY;
        final Map<String, Object> map = withoutOptimization(
                map( "cypherFormat", "addStructure"));
        testsCommon(expectedCypherStatement, map);
    }

    @Test
    public void updateStructureOptimizationNone() {
        String expectedCypherStatement = ":begin\n:commit\n" + SCHEMA_UPDATE_STRUCTURE_MULTI_REL + RELSUPDATE_STRUCTURE_2 + CLEANUP_EMPTY;
        final Map<String, Object> map = withoutOptimization(
                map("cypherFormat", "updateStructure"));

        testsCommon(expectedCypherStatement, map, true, true);
    }


    @Test
    public void updateAllWithOptimization() {
        String expectedCypherStatement = SCHEMA_WITH_UNIQUE_IMPORT_ID + NODES_UNWIND_UPDATE_STRUCTURE + RELS_UNWIND_UPDATE_ALL_MULTI_RELS + CLEANUP_SMALL_BATCH;
        final Map<String, Object> map = withOptimizationSmallBatch(
                map("cypherFormat", "updateAll"));

        testsCommon(expectedCypherStatement, map);
    }

    @Test
    public void createWithOptimization() {
        String expectedCypherStatement = SCHEMA_WITH_UNIQUE_IMPORT_ID + NODES_UNWIND + RELS_UNWIND_MULTI_RELS + CLEANUP_SMALL_BATCH;
        final Map<String, Object> map = withOptimizationSmallBatch(
                map("cypherFormat", "create"));

        testsCommon(expectedCypherStatement, map);
    }

    @Test
    public void addStructureWithOptimization() {
        String expectedCypherStatement = SCHEMA_UPDATE_STRUCTURE_MULTI_REL + NODES_UNWIND_ADD_STRUCTURE + RELS_UNWIND_MULTI_RELS + CLEANUP_EMPTY;
        final Map<String, Object> map = withOptimizationSmallBatch
                (map("cypherFormat", "addStructure"));

        testsCommon(expectedCypherStatement, map);
    }

    @Test
    public void addStructureWithOptimizationAndWithoutCleanup() {
        String expectedCypherStatement = SCHEMA_UPDATE_STRUCTURE_MULTI_REL + NODES_UNWIND_ADD_STRUCTURE + RELS_UNWIND_MULTI_RELS + CLEANUP_EMPTY;
        final Map<String, Object> map = withOptimizationSmallBatch
                (map("cypherFormat", "addStructure", "cleanup", false));

        testsCommon(expectedCypherStatement, map, false, true);
    }

    @Test
    public void updateStructureWithOptimization() {
        String expectedCypherStatement = SCHEMA_UPDATE_STRUCTURE_MULTI_REL + RELS_UNWIND_UPDATE_ALL_MULTI_RELS + CLEANUP_EMPTY;
        final Map<String, Object> map = withOptimizationSmallBatch(
                map("cypherFormat", "updateStructure"));

        testsCommon(expectedCypherStatement, map, true, true);
    }

    private Map<String, Object> withoutOptimization(Map<String, Object> map) {
        map.put("useOptimizations", map("type", "none"));
        return map;
    }

    private Map<String, Object> withOptimizationSmallBatch(Map<String, Object> map) {
        map.put("useOptimizations", map("type", "unwind_batch", "unwindBatchSize", 5L));
        return map;
    }

    private void testsCommon(String expectedCypherStatement, Map<String, Object> otherConfigs) {
        testsCommon(expectedCypherStatement, otherConfigs, false, false);
    }

    private void testsCommon(String expectedCypherStatement, Map<String, Object> otherConfigs, boolean recreateNodes, boolean withRelId) {
        consistencyCheck(false);

        // all test with batch size, to ensure it works correctly and with multipleRelationshipsWithType: true
        final Map<String, Object> config = map("stream", true, RELS_WITH_TYPE_KEY, true, "batchSize", 5);
        config.putAll(otherConfigs);
        final String cypherStatements = db.executeTransactionally("CALL apoc.export.cypher.all(null, $config)",
                map("config",  config),
                r -> Iterators.stream(r.<String>columnAs("cypherStatements")).collect(Collectors.joining("\n")));

        // check cypherStatements result
        assertEquals(expectedCypherStatement, cypherStatements.replace("  ", " "));

        // delete and recreate using export nodeStatements, relationshipStatements
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        // re-create all
        if (recreateNodes) {
            // for 'cypherFormat: updateStructure', because doesn't create nodes, only match them
            db.executeTransactionally("CREATE (:Person:`UNIQUE IMPORT LABEL`{name: 'MyName', `UNIQUE IMPORT ID`:0}), " +
                    "(:Project:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1})," +
                    "(:Team:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:2, name: 'one'})," +
                    "(:Team:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3, name: 'two'})");
        }
        db.executeTransactionally("call apoc.cypher.runMany($statements, {})", map("statements", cypherStatements));

        // check that before and after import the results are equivalents
        consistencyCheck(withRelId);
    }

    private void consistencyCheck(boolean withRelId) {
        TestUtil.testResult(db, "match p=(start:Person {name: 'MyName'})-[rel:WORKS_FOR]->(end:Project) return rel ORDER BY rel.id", r -> {
            final ResourceIterator<Relationship> rels = r.columnAs("rel");
            Relationship next = rels.next();
            final Node startNode = next.getStartNode();
            final Node endNode = next.getEndNode();
            if (withRelId) {
                assertTrue(next.hasProperty(UNIQUE_ID_REL));
            }
            assertEquals(1L, next.getProperty("id"));
            commonAssertions(rels.next(), startNode, endNode, 2L, withRelId);
            commonAssertions(rels.next(), startNode, endNode, 2L, withRelId);
            commonAssertions(rels.next(), startNode, endNode, 3L, withRelId);
            commonAssertions(rels.next(), startNode, endNode, 4L, withRelId);
            commonAssertions(rels.next(), startNode, endNode, 5L, withRelId);
            assertFalse(rels.hasNext());
        });

        final List<Object> teams = TestUtil.firstColumn(db, "match p=(start {name: 'MyName'})-[rel:IS_TEAM_MEMBER_OF]->(end:Team) return end.name order by end.name");
        assertEquals(List.of("one", "two"), teams);
    }

    private void commonAssertions(Relationship next, Node startNode, Node endNode, long id, boolean withRelId) {
        assertEquals(startNode, next.getStartNode());
        assertEquals(endNode, next.getEndNode());
        assertEquals(id, next.getProperty("id"));
        if (withRelId) {
            assertTrue(next.hasProperty(UNIQUE_ID_REL));
        }
    }

}
