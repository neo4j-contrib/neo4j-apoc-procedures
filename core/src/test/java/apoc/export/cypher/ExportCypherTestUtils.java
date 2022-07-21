package apoc.export.cypher;

public class ExportCypherTestUtils {
    protected final static String NODES_MULTI_RELS = ":begin\n" +
            "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}) SET n.name=\"MyName\", n:Person;\n" +
            "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) SET n.a=1, n:Project;\n" +
            "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:2}) SET n.name=\"one\", n:Team;\n" +
            "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3}) SET n.name=\"two\", n:Team;\n" +
            ":commit\n";

    protected final static String NODES_MULTI_RELS_ADD_STRUCTURE = ":begin\n" +
            "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}) ON CREATE SET n.name=\"MyName\", n:Person;\n" +
            "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) ON CREATE SET n.a=1, n:Project;\n" +
            "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:2}) ON CREATE SET n.name=\"one\", n:Team;\n" +
            "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3}) ON CREATE SET n.name=\"two\", n:Team;\n" +
            ":commit\n";

    protected final static String NODES_UNWIND = ":begin\n" +
            "UNWIND [{_id:1, properties:{a:1}}] AS row\n" +
            "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Project;\n" +
            "UNWIND [{_id:2, properties:{name:\"one\"}}, {_id:3, properties:{name:\"two\"}}] AS row\n" +
            "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Team;\n" +
            "UNWIND [{_id:0, properties:{name:\"MyName\"}}] AS row\n" +
            "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Person;\n" +
            ":commit\n";

    protected final static String NODES_UNWIND_ADD_STRUCTURE = ":begin\n" +
            "UNWIND [{_id:1, properties:{a:1}}] AS row\n" +
            "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) ON CREATE SET n += row.properties SET n:Project;\n" +
            "UNWIND [{_id:2, properties:{name:\"one\"}}, {_id:3, properties:{name:\"two\"}}] AS row\n" +
            "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) ON CREATE SET n += row.properties SET n:Team;\n" +
            "UNWIND [{_id:0, properties:{name:\"MyName\"}}] AS row\n" +
            "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) ON CREATE SET n += row.properties SET n:Person;\n" +
            ":commit\n";

    protected final static String NODES_UNWIND_UPDATE_STRUCTURE = ":begin\n" +
            "UNWIND [{_id:1, properties:{a:1}}] AS row\n" +
            "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Project;\n" +
            "UNWIND [{_id:2, properties:{name:\"one\"}}, {_id:3, properties:{name:\"two\"}}] AS row\n" +
            "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Team;\n" +
            "UNWIND [{_id:0, properties:{name:\"MyName\"}}] AS row\n" +
            "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Person;\n" +
            ":commit\n";

    protected final static String NODES_MULTI_REL_CREATE = ":begin\n" +
            "CREATE (:Person:`UNIQUE IMPORT LABEL` {name:\"MyName\", `UNIQUE IMPORT ID`:0});\n" +
            "CREATE (:Project:`UNIQUE IMPORT LABEL` {a:1, `UNIQUE IMPORT ID`:1});\n" +
            "CREATE (:Team:`UNIQUE IMPORT LABEL` {name:\"one\", `UNIQUE IMPORT ID`:2});\n" +
            "CREATE (:Team:`UNIQUE IMPORT LABEL` {name:\"two\", `UNIQUE IMPORT ID`:3});\n" +
            ":commit\n";

    protected final static String SCHEMA_WITH_UNIQUE_IMPORT_ID = ":begin\n" +
            "CREATE CONSTRAINT UNIQUE_IMPORT_NAME FOR (node:`UNIQUE IMPORT LABEL`) REQUIRE (node.`UNIQUE IMPORT ID`) IS UNIQUE;\n" +
            ":commit\n" +
            "CALL db.awaitIndexes(300);\n";

    protected final static String SCHEMA_UPDATE_STRUCTURE_MULTI_REL = ":begin\n" +
            ":commit\n" +
            "CALL db.awaitIndexes(300);\n";

    protected final static String RELS_MULTI_RELS = ":begin\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:0}]->(n2) SET r.id=1;\n" +
            "\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:1}]->(n2) SET r.id=2;\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:2}]->(n2) SET r.id=2;\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:3}]->(n2) SET r.id=3;\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:4}]->(n2) SET r.id=4;\n" +
            ":commit\n" +
            ":begin\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:5}]->(n2) SET r.id=5;\n" +
            "\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:2}) MERGE (n1)-[r:IS_TEAM_MEMBER_OF]->(n2) SET r.name=\"aaa\";\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3}) MERGE (n1)-[r:IS_TEAM_MEMBER_OF]->(n2) SET r.name=\"eee\";\n" +
            ":commit\n";

    protected final static String RELS_UNWIND_MULTI_RELS = ":begin\n" +
            "UNWIND [{start: {_id:0}, end: {_id:2}, properties:{name:\"aaa\"}}, {start: {_id:0}, end: {_id:3}, properties:{name:\"eee\"}}] AS row\n" +
            "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})\n" +
            "MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})\n" +
            "CREATE (start)-[r:IS_TEAM_MEMBER_OF]->(end) SET r += row.properties;\n" +
            "UNWIND [{start: {_id:0}, id: 0, end: {_id:1}, properties:{id:1}}, {start: {_id:0}, id: 1, end: {_id:1}, properties:{id:2}}, {start: {_id:0}, id: 2, end: {_id:1}, properties:{id:2}}] AS row\n" +
            "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})\n" +
            "MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})\n" +
            "CREATE (start)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:row.id}]->(end) SET r += row.properties;\n" +
            ":commit\n" +
            ":begin\n" +
            "UNWIND [{start: {_id:0}, id: 3, end: {_id:1}, properties:{id:3}}, {start: {_id:0}, id: 4, end: {_id:1}, properties:{id:4}}, {start: {_id:0}, id: 5, end: {_id:1}, properties:{id:5}}] AS row\n" +
            "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})\n" +
            "MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})\n" +
            "CREATE (start)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:row.id}]->(end) SET r += row.properties;\n" +
            ":commit\n" +
            "\n";

    protected final static String RELS_UNWIND_UPDATE_ALL_MULTI_RELS = ":begin\n" +
            "UNWIND [{start: {_id:0}, end: {_id:2}, properties:{name:\"aaa\"}}, {start: {_id:0}, end: {_id:3}, properties:{name:\"eee\"}}] AS row\n" +
            "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})\n" +
            "MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})\n" +
            "MERGE (start)-[r:IS_TEAM_MEMBER_OF]->(end) SET r += row.properties;\n" +
            "UNWIND [{start: {_id:0}, id: 0, end: {_id:1}, properties:{id:1}}, {start: {_id:0}, id: 1, end: {_id:1}, properties:{id:2}}, {start: {_id:0}, id: 2, end: {_id:1}, properties:{id:2}}] AS row\n" +
            "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})\n" +
            "MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})\n" +
            "MERGE (start)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:row.id}]->(end) SET r += row.properties;\n" +
            ":commit\n" +
            ":begin\n" +
            "UNWIND [{start: {_id:0}, id: 3, end: {_id:1}, properties:{id:3}}, {start: {_id:0}, id: 4, end: {_id:1}, properties:{id:4}}, {start: {_id:0}, id: 5, end: {_id:1}, properties:{id:5}}] AS row\n" +
            "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})\n" +
            "MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})\n" +
            "MERGE (start)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:row.id}]->(end) SET r += row.properties;\n" +
            ":commit\n\n";

    protected final static String RELS_ADD_STRUCTURE_MULTI_RELS = ":begin\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) CREATE (n1)-[r:WORKS_FOR {id:1}]->(n2);\n" +
            "\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) CREATE (n1)-[r:WORKS_FOR {id:2}]->(n2);\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) CREATE (n1)-[r:WORKS_FOR {id:2}]->(n2);\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) CREATE (n1)-[r:WORKS_FOR {id:3}]->(n2);\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) CREATE (n1)-[r:WORKS_FOR {id:4}]->(n2);\n" +
            ":commit\n" +
            ":begin\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) CREATE (n1)-[r:WORKS_FOR {id:5}]->(n2);\n" +
            "\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:2}) CREATE (n1)-[r:IS_TEAM_MEMBER_OF {name:\"aaa\"}]->(n2);\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3}) CREATE (n1)-[r:IS_TEAM_MEMBER_OF {name:\"eee\"}]->(n2);\n" +
            ":commit\n";

    protected final static String RELSUPDATE_STRUCTURE_2 = ":begin\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:0}]->(n2) ON CREATE SET r.id=1;\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:1}]->(n2) ON CREATE SET r.id=2;\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:2}]->(n2) ON CREATE SET r.id=2;\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:3}]->(n2) ON CREATE SET r.id=3;\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:4}]->(n2) ON CREATE SET r.id=4;\n" +
            "\n" +
            ":commit\n" +
            ":begin\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:5}]->(n2) ON CREATE SET r.id=5;\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:2}) MERGE (n1)-[r:IS_TEAM_MEMBER_OF]->(n2) ON CREATE SET r.name=\"aaa\";\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3}) MERGE (n1)-[r:IS_TEAM_MEMBER_OF]->(n2) ON CREATE SET r.name=\"eee\";\n" +
            ":commit\n";

    protected final static String CLEANUP_SMALL_BATCH = ":begin\n" +
            "MATCH (n:`UNIQUE IMPORT LABEL`) WITH n LIMIT 5 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;\n" +
            ":commit\n" +
            ":begin\n" +
            "DROP CONSTRAINT UNIQUE_IMPORT_NAME;\n" +
            ":commit\n";

    protected final static String CLEANUP_EMPTY = ":begin\n:commit\n:begin\n:commit\n";
}
