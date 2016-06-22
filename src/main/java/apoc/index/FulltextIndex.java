package apoc.index;

import apoc.Description;
import apoc.meta.Meta;
import apoc.result.NodeResult;
import apoc.result.WeightedNodeResult;
import apoc.result.WeightedRelationshipResult;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.index.impl.lucene.legacy.LuceneIndexImplementation;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 25.03.16
 */
public class FulltextIndex {
    private static final Map<String, String> FULL_TEXT = LuceneIndexImplementation.FULLTEXT_CONFIG;
    public static final String NODE = Meta.Types.NODE.name();
    public static final String RELATIONSHIP = Meta.Types.RELATIONSHIP.name();

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    // CALL apoc.index.nodes('Person','name:jo*')
    @Description("apoc.index.nodes('Label','prop:value*') YIELD node - lucene query on node index with the given label name")
    @Procedure @PerformsWrites
    public Stream<WeightedNodeResult> nodes(@Name("label") String label, @Name("query") String query) {
        if (!db.index().existsForNodes(label)) return Stream.empty();

        IndexHits<Node> hits = db.index().forNodes(label).query(query);
        return toWeightedNodeResult(hits).stream();
    }

    public static class IndexInfo {
        public final String type;
        public final String name;
        public final Map<String,String> config;

        public IndexInfo(String type, String name, Map<String, String> config) {
            this.type = type;
            this.name = name;
            this.config = config;
        }
    }

    @Description("apoc.index.forNodes('name',{config}) YIELD type,name,config - gets or creates node index")
    @Procedure @PerformsWrites
    public Stream<IndexInfo> forNodes(@Name("name") String name, @Name("config") Map<String,String> config) {
        Index<Node> index = getNodeIndex(name, config);
        return Stream.of(new IndexInfo(NODE, name, db.index().getConfiguration(index)));
    }

    private Index<Node> getNodeIndex(@Name("name") String name, @Name("config") Map<String, String> config) {
        IndexManager mgr = db.index();
        return mgr.existsForNodes(name) || config == null ? mgr.forNodes(name) : mgr.forNodes(name, config);
    }

    @Description("apoc.index.forRelationships('name',{config}) YIELD type,name,config - gets or creates relationship index")
    @Procedure @PerformsWrites
    public Stream<IndexInfo> forRelationships(@Name("name") String name, @Name("config") Map<String,String> config) {
        Index<Relationship> index = getRelationshipIndex(name, config);
        return Stream.of(new IndexInfo(RELATIONSHIP, name, db.index().getConfiguration(index)));
    }

    private Index<Relationship> getRelationshipIndex(@Name("name") String name, @Name("config") Map<String, String> config) {
        IndexManager mgr = db.index();
        return mgr.existsForRelationships(name) || config == null ? mgr.forRelationships(name) : mgr.forRelationships(name, config);
    }

    @Description("apoc.index.remove('name') YIELD type,name,config - removes an manual index")
    @Procedure @PerformsWrites
    public Stream<IndexInfo> remove(@Name("name") String name) {
        IndexManager mgr = db.index();
        List<IndexInfo> indexInfos = new ArrayList<>(2);
        if (mgr.existsForNodes(name)) {
            Index<Node> index = mgr.forNodes(name);
            indexInfos.add(new IndexInfo(NODE, name, mgr.getConfiguration(index)));
            index.delete();
        }
        if (mgr.existsForRelationships(name)) {
            Index<Relationship> index = mgr.forRelationships(name);
            indexInfos.add(new IndexInfo(RELATIONSHIP, name, mgr.getConfiguration(index)));
            index.delete();
        }
        return indexInfos.stream();
    }

    @Description("apoc.index.list() - YIELD type,name,config - lists all manual indexes")
    @Procedure @PerformsWrites
    public Stream<IndexInfo> list() {
        IndexManager mgr = db.index();
        List<IndexInfo> indexInfos = new ArrayList<>(100);
        for (String name : mgr.nodeIndexNames()) {
            Index<Node> index = mgr.forNodes(name);
            indexInfos.add(new IndexInfo(NODE,name,mgr.getConfiguration(index)));
        }
        for (String name : mgr.relationshipIndexNames()) {
            RelationshipIndex index = mgr.forRelationships(name);
            indexInfos.add(new IndexInfo(RELATIONSHIP,name,mgr.getConfiguration(index)));
        }
        return indexInfos.stream();
    }

    private List<WeightedNodeResult> toWeightedNodeResult(IndexHits<Node> hits) {
        List<WeightedNodeResult> results = new ArrayList<>(hits.size());
        while (hits.hasNext()) {
            results.add(new WeightedNodeResult(hits.next(),(double)hits.currentScore()));
        }
        return results;
    }
    private List<WeightedRelationshipResult> toWeightedRelationshipResult(IndexHits<Relationship> hits) {
        List<WeightedRelationshipResult> results = new ArrayList<>(hits.size());
        while (hits.hasNext()) {
            results.add(new WeightedRelationshipResult(hits.next(),(double)hits.currentScore()));
        }
        return results;
    }

    // CALL apoc.index.relationships('CHECKIN','on:2010-*')
    @Description("apoc.index.relationships('TYPE','prop:value*') YIELD rel - lucene query on relationship index with the given type name")
    @Procedure @PerformsWrites
    public Stream<WeightedRelationshipResult> relationships(@Name("type") String type, @Name("query") String query) {
        if (!db.index().existsForRelationships(type)) return Stream.empty();

        return toWeightedRelationshipResult(db.index().forRelationships(type).query(query)).stream();
    }

    // CALL apoc.index.between(joe, 'KNOWS', null, 'since:2010-*')
    // CALL apoc.index.between(joe, 'CHECKIN', philz, 'on:2016-01-*')
    @Description("apoc.index.between(node1,'TYPE',node2,'prop:value*') YIELD rel - lucene query on relationship index with the given type name bound by either or both sides (each node parameter can be null)")
    @Procedure @PerformsWrites
    public Stream<WeightedRelationshipResult> between(@Name("from") Node from, @Name("type") String type, @Name("to") Node to, @Name("query") String query) {
        if (!db.index().existsForRelationships(type)) return Stream.empty();

        return toWeightedRelationshipResult(db.index().forRelationships(type).query(query, from, to)).stream();
    }

    // CALL apoc.index.out(joe, 'CHECKIN', 'on:2010-*')
    @Procedure @PerformsWrites
    @Description("apoc.index.out(node,'TYPE','prop:value*') YIELD node - lucene query on relationship index with the given type name for *outgoing* relationship of the given node, *returns end-nodes*")
    public Stream<WeightedNodeResult> out(@Name("from") Node from, @Name("type") String type, @Name("query") String query) {
        if (!db.index().existsForRelationships(type)) return Stream.empty();

        IndexHits<Relationship> hits = db.index().forRelationships(type).query(query, from, null);
        List<WeightedNodeResult> results = new ArrayList<>(hits.size());
        while (hits.hasNext()) {
            results.add(new WeightedNodeResult(hits.next().getEndNode(),(double)hits.currentScore()));
        }
        return results.stream();
    }

    // CALL apoc.index.in(philz, 'CHECKIN', 'on:2010-*')
    @Procedure @PerformsWrites
    @Description("apoc.index.in(node,'TYPE','prop:value*') YIELD node lucene query on relationship index with the given type name for *incoming* relationship of the given node, *returns start-nodes*")
    public Stream<WeightedNodeResult> in(@Name("to") Node to, @Name("type") String type, @Name("query") String query) {
        if (!db.index().existsForRelationships(type)) return Stream.empty();

        IndexHits<Relationship> hits = db.index().forRelationships(type).query(query, null, to);
        List<WeightedNodeResult> results = new ArrayList<>(hits.size());
        while (hits.hasNext()) {
            results.add(new WeightedNodeResult(hits.next().getStartNode(),(double)hits.currentScore()));
        }
        return results.stream();
    }

    // CALL apoc.index.addNode(joe, ['name','age','city'])
    @Procedure
    @PerformsWrites
    @Description("apoc.index.addNode(node,['prop1',...]) add node to an index for each label it has")
    public void addNode(@Name("node") Node node, @Name("properties") List<String> propKeys) {
        for (Label label : node.getLabels()) {
            addNodeByLabel(label.name(),node,propKeys);
        }
    }

    // CALL apoc.index.addNode(joe, 'Person', ['name','age','city'])
    @Procedure
    @PerformsWrites
    @Description("apoc.index.addNodeByLabel(node,'Label',['prop1',...]) add node to an index for the given label")
    public void addNodeByLabel(@Name("label") String label, @Name("node") Node node, @Name("properties") List<String> propKeys) {
        indexContainer(node, propKeys, getNodeIndex(label,FULL_TEXT));
    }

    // CALL apoc.index.addRelationship(checkin, ['on'])
    @Procedure
    @PerformsWrites
    @Description("apoc.index.addRelationship(rel,['prop1',...]) add relationship to an index for its type")
    public void addRelationship(@Name("relationship") Relationship rel, @Name("properties") List<String> propKeys) {
        indexContainer(rel, propKeys, getRelationshipIndex(rel.getType().name(),FULL_TEXT));
    }

    private <T extends PropertyContainer> void indexContainer(T pc, @Name("properties") List<String> propKeys, org.neo4j.graphdb.index.Index<T> index) {
        index.remove(pc);
        for (String key : propKeys) {
            Object value = pc.getProperty(key, null);
            if (value == null) continue;
            index.remove(pc,key);
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
