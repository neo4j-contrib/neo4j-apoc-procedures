package apoc.schemas;

import apoc.schema.SchemasExtended;
import org.junit.Before;
import org.junit.Rule;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Collections;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.registerProcedure;
import static apoc.util.TestUtil.testResult;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SchemasExtendedTest {
    
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.procedure_unrestricted, Collections.singletonList("apoc.*"));
    
    @Before
    public void setUp() throws Exception {
        registerProcedure(db, SchemasExtended.class);
    }
    
    @Test
    public void testCompareIndexesAndConstraints() {
        db.executeTransactionally("CREATE INDEX FOR (n:Person) ON (n.surname)");
        db.executeTransactionally("CREATE CONSTRAINT FOR (n:Person) REQUIRE (n.address) IS UNIQUE");
        db.executeTransactionally("CREATE INDEX FOR (n:Movie) ON (n.name)");
        db.executeTransactionally("CALL db.index.fulltext.createNodeIndex('fullSecondIdx', ['Person', 'Another'], ['weightProp'])");

        testResult(db, "CALL apoc.schema.node.compareIndexesAndConstraints()", (res) -> {
            Map<String, Object> row = res.next();
            assertEquals(Map.of(":<any-labels>()", emptyList()), row.get("onlyIdxProps"));
            assertEquals("<any-labels>", row.get("label"));
            assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
            assertEquals(emptyList(), row.get("commonProps"));
            row = res.next();
            assertEquals(Map.of(":[Another, Person],(weightProp)", List.of("weightProp")), row.get("onlyIdxProps"));
            assertEquals("Another", row.get("label"));
            assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
            assertEquals(emptyList(), row.get("commonProps"));
            row = res.next();
            assertEquals(Map.of(":Movie(name)", List.of("name")), row.get("onlyIdxProps"));
            assertEquals("Movie", row.get("label"));
            assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
            assertEquals(emptyList(), row.get("commonProps"));
            row = res.next();
            assertEquals(Map.of(":[Another, Person],(weightProp)", List.of("weightProp"), ":Person(surname)", List.of("surname")), row.get("onlyIdxProps"));
            assertEquals("Person", row.get("label"));
            assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
            assertEquals(List.of("address"), row.get("commonProps"));
            assertFalse(res.hasNext());
        });
    }

    @Test
    public void testCompareIndexesAndConstraintsRel() {
        db.executeTransactionally("CALL db.index.fulltext.createRelationshipIndex('fullIdxRel', ['TYPE_1', 'TYPE_2'], ['alpha', 'beta'])");
        db.executeTransactionally("CREATE INDEX rel_index_name FOR ()-[r:KNOWS]-() ON (r.since)");

        testResult(db, "CALL apoc.schema.relationship.compareIndexesAndConstraints()", (res) -> {
            Map<String, Object> row = res.next();
            assertEquals("<any-types>", row.get("type"));
            assertEquals(Map.of(":<any-types>()", emptyList()), row.get("onlyIdxProps"));
            assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
            assertEquals(emptyList(), row.get("commonProps"));
            row = res.next();
            assertEquals("KNOWS", row.get("type"));
            assertEquals(Map.of(":KNOWS(since)", List.of("since")), row.get("onlyIdxProps"));
            assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
            assertEquals(emptyList(), row.get("commonProps"));
            row = res.next();
            assertEquals("TYPE_1", row.get("type"));
            assertEquals(Map.of(":[TYPE_1, TYPE_2],(alpha,beta)", List.of("alpha", "beta")), row.get("onlyIdxProps"));
            assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
            assertEquals(emptyList(), row.get("commonProps"));
            row = res.next();
            assertEquals(Map.of(":[TYPE_1, TYPE_2],(alpha,beta)", List.of("alpha", "beta")), row.get("onlyIdxProps"));
            assertEquals("TYPE_2", row.get("type"));
            assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
            assertEquals(emptyList(), row.get("commonProps"));
            assertFalse(res.hasNext());
        });
    }
}
