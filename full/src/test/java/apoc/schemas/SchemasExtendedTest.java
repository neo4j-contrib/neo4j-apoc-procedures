package apoc.schemas;

import apoc.schema.SchemasExtended;
import org.junit.Before;
import org.junit.Rule;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Result;
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
        db.executeTransactionally("CREATE TEXT INDEX FOR (n:Foo) ON (n.text)");
        db.executeTransactionally("CREATE POINT INDEX FOR (n:Foo) ON (n.point)");
        db.executeTransactionally("CREATE RANGE INDEX FOR (n:Foo) ON (n.range)");
        db.executeTransactionally("CREATE CONSTRAINT FOR (n:Foo) REQUIRE (n.cons) IS UNIQUE");

        db.executeTransactionally("CREATE INDEX FOR (n:Person) ON (n.surname)");
        db.executeTransactionally("CREATE CONSTRAINT FOR (n:Person) REQUIRE (n.address) IS UNIQUE");
        db.executeTransactionally("CREATE FULLTEXT INDEX fullSecondIdx FOR (n:Person|Another) ON EACH [n.weightProp]");

        db.executeTransactionally("CREATE INDEX FOR (n:Movie) ON (n.name)");

        testResult(db, "CALL apoc.schema.node.compareIndexesAndConstraints()", (res) -> {
            Map<String, Object> row = res.next();
            assertAnyLabels(row);
            row = res.next();
            assertEquals("Another", row.get("label"));
            assertEquals(Map.of(":[Another, Person],(weightProp)", List.of("weightProp")), row.get("onlyIdxProps"));
            assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
            assertEquals(emptyList(), row.get("commonProps"));

            row = res.next();
            assertFooLabel(row);

            row = res.next();
            assertMovieLabel(row);

            row = res.next();
            assertEquals("Person", row.get("label"));
            Map<String, List<String>> expectedOnlyIdxProps = Map.of(":[Another, Person],(weightProp)", List.of("weightProp"), ":Person(surname)", List.of("surname"));
            assertEquals(expectedOnlyIdxProps, row.get("onlyIdxProps"));
            assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
            assertEquals(List.of("address"), row.get("commonProps"));
            assertFalse(res.hasNext());
        });

        testResult(db, "CALL apoc.schema.node.compareIndexesAndConstraints({labels: ['Movie', 'NotExistent']})", (res) -> {
            Map<String, Object> row = res.next();
            assertMovieLabel(row);

            assertFalse(res.hasNext());
        });

        testResult(db, "CALL apoc.schema.node.compareIndexesAndConstraints({excludeLabels: ['Person', 'NotExistent']})", (res) -> {
            Map<String, Object> row = res.next();
            assertAnyLabels(row);

            row = res.next();
            assertFooLabel(row);

            row = res.next();
            assertMovieLabel(row);

            assertFalse(res.hasNext());
        });
    }

    @Test
    public void testCompareIndexesAndConstraintsRel() {
        db.executeTransactionally("CALL db.index.fulltext.createRelationshipIndex('fullIdxRel', ['TYPE_1', 'TYPE_2'], ['alpha', 'beta'])");
        db.executeTransactionally("CREATE INDEX rel_index_name FOR ()-[r:KNOWS]-() ON (r.since)");

        testResult(db, "CALL apoc.schema.relationship.compareIndexesAndConstraints()", (res) -> {
            Map<String, Object> row = res.next();
            assertAnyTypes(row);
            row = res.next();
            assertKnowsType(row);
            row = res.next();
            assertType1AndType2(res, row);
            assertFalse(res.hasNext());
        });

        testResult(db, "CALL apoc.schema.relationship.compareIndexesAndConstraints({relationships: ['TYPE_1', 'NOT_EXISTENT']})", (res) -> {
            Map<String, Object> row = res.next();
            assertType1AndType2(res, row);
            assertFalse(res.hasNext());
        });

        testResult(db, "CALL apoc.schema.relationship.compareIndexesAndConstraints({excludeRelationships: ['TYPE_2', 'NOT_EXISTENT']})", (res) -> {
            Map<String, Object> row = res.next();
            assertAnyTypes(row);
            row = res.next();
            assertKnowsType(row);
            assertFalse(res.hasNext());
        });
    }

    private void assertAnyLabels(Map<String, Object> row) {
        assertEquals(Map.of(":<any-labels>()", emptyList()), row.get("onlyIdxProps"));
        assertEquals("<any-labels>", row.get("label"));
        assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
        assertEquals(emptyList(), row.get("commonProps"));
    }

    private void assertFooLabel(Map<String, Object> row) {
        assertEquals("Foo", row.get("label"));
        Map<String, List<String>> expectedFooOnlyIdxProps = Map.of(":Foo(text)", List.of("text"),
                ":Foo(point)", List.of("point"),
                ":Foo(range)", List.of("range"));
        assertEquals(expectedFooOnlyIdxProps, row.get("onlyIdxProps"));
        assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
        assertEquals(List.of("cons"), row.get("commonProps"));
    }

    private void assertMovieLabel(Map<String, Object> row) {
        assertEquals(Map.of(":Movie(name)", List.of("name")), row.get("onlyIdxProps"));
        assertEquals("Movie", row.get("label"));
        assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
        assertEquals(emptyList(), row.get("commonProps"));
    }

    private void assertKnowsType(Map<String, Object> row) {
        assertEquals("KNOWS", row.get("relationshipType"));
        assertEquals(Map.of(":KNOWS(since)", List.of("since")), row.get("onlyIdxProps"));
        assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
        assertEquals(emptyList(), row.get("commonProps"));
    }

    private void assertAnyTypes(Map<String, Object> row) {
        assertEquals("<any-types>", row.get("relationshipType"));
        assertEquals(Map.of(":<any-types>()", emptyList()), row.get("onlyIdxProps"));
        assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
        assertEquals(emptyList(), row.get("commonProps"));
    }

    private void assertType1AndType2(Result res, Map<String, Object> row) {
        // TYPE_1 assertions
        assertEquals(Map.of(":[TYPE_1, TYPE_2],(alpha,beta)", List.of("alpha", "beta")), row.get("onlyIdxProps"));
        assertEquals("TYPE_1", row.get("relationshipType"));
        assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
        assertEquals(emptyList(), row.get("commonProps"));

        row = res.next();
        // TYPE_2 assertions
        assertEquals(Map.of(":[TYPE_1, TYPE_2],(alpha,beta)", List.of("alpha", "beta")), row.get("onlyIdxProps"));
        assertEquals("TYPE_2", row.get("relationshipType"));
        assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
        assertEquals(emptyList(), row.get("commonProps"));
    }
}
