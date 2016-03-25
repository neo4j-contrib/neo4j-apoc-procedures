package apoc.index;

import apoc.result.NodeResult;
import apoc.result.RelationshipResult;
import org.neo4j.graphdb.*;
import org.neo4j.index.impl.lucene.legacy.LuceneIndexImplementation;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 25.03.16
 */
public class FulltextIndex {
    private static final Map<String, String> FULL_TEXT = LuceneIndexImplementation.FULLTEXT_CONFIG;

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    // CALL apoc.index.nodes('Person','name:jo*')
    @Procedure @PerformsWrites
    public Stream<NodeResult> nodes(@Name("label") String label, @Name("query") String query) {
        if (!db.index().existsForNodes(label)) return Stream.empty();

        return db.index()
                .forNodes(label)
                .query(query)
                .stream()
                .map(NodeResult::new);
    }

    // CALL apoc.index.relationships('CHECKIN','on:2010-*')
    @Procedure @PerformsWrites
    public Stream<RelationshipResult> relationships(@Name("type") String type, @Name("query") String query) {
        if (!db.index().existsForRelationships(type)) return Stream.empty();

        return db.index()
                .forRelationships(type)
                .query(query)
                .stream()
                .map(RelationshipResult::new);
    }

    // CALL apoc.index.between(joe, 'KNOWS', null, 'since:2010-*')
    // CALL apoc.index.between(joe, 'CHECKIN', philz, 'on:2016-01-*')
    @Procedure @PerformsWrites
    public Stream<RelationshipResult> between(@Name("from") Node from, @Name("type") String type, @Name("to") Node to, @Name("query") String query) {
        if (!db.index().existsForRelationships(type)) return Stream.empty();

        return db.index()
                .forRelationships(type)
                .query(query, from, to)
                .stream()
                .map(RelationshipResult::new);
    }

    // CALL apoc.index.out(joe, 'CHECKIN', 'on:2010-*')
    @Procedure @PerformsWrites
    public Stream<NodeResult> out(@Name("from") Node from, @Name("type") String type, @Name("query") String query) {
        if (!db.index().existsForRelationships(type)) return Stream.empty();

        return db.index()
                .forRelationships(type)
                .query(query, from, null)
                .stream()
                .map((r) -> new NodeResult(r.getEndNode()));
    }

    // CALL apoc.index.in(philz, 'CHECKIN', 'on:2010-*')
    @Procedure @PerformsWrites
    public Stream<NodeResult> in(@Name("to") Node to, @Name("type") String type, @Name("query") String query) {
        if (!db.index().existsForRelationships(type)) return Stream.empty();

        return db.index()
                .forRelationships(type)
                .query(query, null, to)
                .stream()
                .map((r) -> new NodeResult(r.getStartNode()));
    }

    // CALL apoc.index.addNode(joe, ['name','age','city'])
    @Procedure
    @PerformsWrites
    public void addNode(@Name("node") Node node, @Name("properties") List<String> propKeys) {
        for (Label label : node.getLabels()) {
            addNodeByLabel(label.name(),node,propKeys);
        }
    }

    // CALL apoc.index.addNode(joe, 'Person', ['name','age','city'])
    @Procedure
    @PerformsWrites
    public void addNodeByLabel(@Name("label") String label, @Name("node") Node node, @Name("properties") List<String> propKeys) {
        org.neo4j.graphdb.index.Index<Node> index = db.index().forNodes(label, FULL_TEXT);
        indexContainer(node, propKeys, index);
    }

    // CALL apoc.index.addRelationship(checkin, ['on'])
    @Procedure
    @PerformsWrites
    public void addRelationship(@Name("relationship") Relationship rel, @Name("properties") List<String> propKeys) {
        String indexName = rel.getType().name();
        org.neo4j.graphdb.index.Index<Relationship> index = db.index().forRelationships(indexName, FULL_TEXT);
        indexContainer(rel, propKeys, index);
    }

    private <T extends PropertyContainer> void indexContainer(T pc, @Name("properties") List<String> propKeys, org.neo4j.graphdb.index.Index<T> index) {
        index.remove(pc);
        for (String key : propKeys) {
            Object value = pc.getProperty(key, null);
            if (value == null) continue;
            index.add(pc, key, value);
        }
    }

    /* WIP
    private TermQuery query(Map<String,Object> params) {
        Object sort = params.remove("sort");
        if (sort != null) {
            if (sort instanceof String) {
                new SortField(sort,null);
            }
        }
        Object top = params.remove("top");
        params.remove("score");
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            Object value = entry.getValue();
            if (value == null) continue;
            if (value instanceof String) {
                Query.class
            }
            if (value instanceof Number) {
                Number num = (Number) value;
                if (value instanceof Double || value instanceof Float) {
                    builder.add(NumericRangeQuery.newDoubleRange(entry.getKey(), num.doubleValue(), num.doubleValue(), true, true));
                }
                builder.add(NumericRangeQuery.newLongRange(entry.getKey(), num.longValue(), num.longValue(), true, true));
            }
            if (value.getClass().isArray()) {

            }
        }
        NumericRangeQuery.newDoubleRange(field,min, max, minInclusive,maxInclusive);
        NumericRangeQuery.newLongRange(field,min, max, minInclusive,maxInclusive);
        new Sort(new SortField())

    }
    */

}
