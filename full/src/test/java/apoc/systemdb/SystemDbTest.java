package apoc.systemdb;

import apoc.util.TestUtil;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SystemDbTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, SystemDb.class);
    }

    @Test
    public void testGetGraph() throws Exception {
        TestUtil.testResult(db, "CALL apoc.systemdb.graph() YIELD nodes, relationships RETURN nodes, relationships", result -> {
            Map<String, Object> map = Iterators.single(result);
            List<Node> nodes = (List<Node>) map.get("nodes");
            List<Relationship> relationships = (List<Relationship>) map.get("relationships");
            assertEquals(5, nodes.size());
            assertEquals(2, nodes.stream().filter( node -> "Database".equals(Iterables.single(node.getLabels()).name())).count());
            assertEquals(1, nodes.stream().filter( node -> "User".equals(Iterables.single(node.getLabels()).name())).count());
            assertEquals(1, nodes.stream().filter( node -> "Version".equals(Iterables.single(node.getLabels()).name())).count());
            assertEquals(1, nodes.stream().filter( node -> "DbmsRuntime".equals(Iterables.single(node.getLabels()).name())).count());
            Set<String> names = nodes.stream().map(node -> (String)node.getProperty("name")).filter(Objects::nonNull).collect(Collectors.toSet());
            org.hamcrest.MatcherAssert.assertThat( names, Matchers.containsInAnyOrder("neo4j", "system"));

            assertTrue(relationships.isEmpty());
        });
    }

    @Test
    public void testExecute() {
        TestUtil.testResult(db, "CALL apoc.systemdb.execute('SHOW DATABASES') YIELD row RETURN row", result -> {
            List<Map<String, Object>> rows = Iterators.asList(result.columnAs("row"));
            // removed key "systemDefault"
            org.hamcrest.MatcherAssert.assertThat(rows, Matchers.containsInAnyOrder(
                    MapUtil.map("name", "system", "default", false, "currentStatus", "online", "role", "standalone", "requestedStatus", "online", "error", "", "address", "localhost:7687"),
                    MapUtil.map("name", "neo4j", "default", true, "currentStatus", "online", "role", "standalone", "requestedStatus", "online", "error", "", "address", "localhost:7687")
            ));
        });
    }

    @Test
    public void testExecuteMultipleStatements() {
        // we have two databases, so asking twice returns 4
        assertEquals(4, TestUtil.count(db, "CALL apoc.systemdb.execute(['SHOW DATABASES','SHOW DATABASES'])"));
    }

    @Test
    public void testWriteStatements() {
        // count exhaust the result - this is important here
        TestUtil.count(db, "CALL apoc.systemdb.execute([\"CREATE USER dummy SET PASSWORD '123'\"])");

        assertEquals(2l, TestUtil.count(db, "CALL apoc.systemdb.execute('SHOW USERS')"));
    }

}
