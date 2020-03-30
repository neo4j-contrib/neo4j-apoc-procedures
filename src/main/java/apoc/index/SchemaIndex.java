package apoc.index;

import apoc.result.ListResult;
import apoc.result.NodeResult;
import apoc.util.MapUtil;
import apoc.util.QueueBasedSpliterator;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.path.RelationshipTypeAndDirections.parse;
import static apoc.util.MapUtil.map;

/**
 * @author mh
 * @since 23.05.16
 */
public class SchemaIndex {

    private static final PropertyValueCount POISON = new PropertyValueCount("poison", "poison", "poison", -1);

    @Context
    public GraphDatabaseAPI db;

    @Context
    public KernelTransaction tx;

    @Context
    public TerminationGuard terminationGuard;

    @Procedure
    @Deprecated
    @Description("apoc.index.relatedNodes([nodes],label,key,'<TYPE'/'TYPE>'/'TYPE',limit) yield node - schema range scan which keeps index order and adds limit and checks opposite node of relationship against the given set of nodes")
    public Stream<NodeResult> related(@Name("nodes") List<Node> nodes,
                                      @Name("label") String label, @Name("key") String key,
                                      @Name("relationship") String relationship,
                                      @Name("limit") long limit) {
        Set<Node> nodeSet = new HashSet<>(nodes);
        Pair<RelationshipType, Direction> relTypeDirection = parse(relationship).get(0);
        RelationshipType type = relTypeDirection.first();
        Direction dir = relTypeDirection.other();

        return queryForRange(label,key,Long.MIN_VALUE,Long.MAX_VALUE,0).filter((node) -> {
            for (Relationship rel : node.getRelationships(dir, type)) {
                Node other = rel.getOtherNode(node);
                if (nodeSet.contains(other)) {
                    return true;
                }
            }
            return false;
        }).map(NodeResult::new).limit(limit);
    }

    @Procedure
    @Deprecated
    @Description("deprecated, just there for compatibility. Use plain cypher instead")
    public Stream<NodeResult> orderedRange(@Name("label") String label, @Name("key") String key, @Name("min") Object min, @Name("max") Object max, @Name("relevance") boolean relevance, @Name("limit") long limit)  {
        Map<String, Object> params = MapUtil.map("min", min, "max", max, "limit", limit);
        List<Object> formatValues = new ArrayList<>();
        formatValues.add(label);
        String query = "MATCH (n:`%s`) WHERE ";
        if (min!=null) {
            query += "n.`%s` >= $min ";
            formatValues.add(key);
        }
        if (max!=null) {
            if (min!=null) {
                query+= "AND ";
            }
            query += "n.`%s` <= $max ";
            formatValues.add(key);
        }
        // we need order by id(n) to be consistent with previous versions (lucene based)
        query += "RETURN n ORDER by id(n) LIMIT $limit";
        Result result = db.execute(String.format(query, formatValues.toArray()), params);
        return result.columnAs("n").stream().map(o -> new NodeResult((Node)o));
    }

    public Stream<Node> queryForRange(@Name("label") String label, @Name("key") String key, @Name("min") Object min, @Name("max") Object max, @Name("limit") long limit) {
        Map<String, Object> params = map("min", min, "max", max, "limit", limit);
        String query = "MATCH (n:`" + label + "`)";
        if (min != null || max != null) {
            query += " WHERE ";
            if (min != null) query += "{min} <=";
            query += " n.`" + key + "` ";
            if (max != null) query += "<= {max}";
        }
        query += " RETURN n ";
        if (limit > 0) query+="LIMIT {limit}";
        ResourceIterator<Node> it = db.execute(query, params).columnAs("n");
        return it.stream().onClose(it::close);
    }

    @Procedure
    @Deprecated
    @Description("just use a cypher query with a range predicate on an indexed field and wait for index backed order by in 3.5")
    public Stream<NodeResult> orderedByText(@Name("label") String label, @Name("key") String key, @Name("operator") String operator, @Name("value") String value, @Name("relevance") boolean relevance, @Name("limit") long limit)  {
        Map<String, Object> params = MapUtil.map("value", value, "limit", limit);
        Result result = db.execute(String.format("MATCH (n:`%s`) WHERE n.`%s` %s $value RETURN n ORDER by id(n) LIMIT $limit", label, key, operator), params); // we need order by id(n) to be consistent with previous versions (lucene based)
        return result.columnAs("n").stream().map(o -> new NodeResult((Node)o));
    }

    @Procedure("apoc.schema.properties.distinct")
    @Description("apoc.schema.properties.distinct(label, key) - quickly returns all distinct values for a given key")
    public Stream<ListResult> distinct(@Name("label") String label, @Name("key")  String key) {
        List<Object> values = distinctCount(label, key).map(propertyValueCount -> propertyValueCount.value).collect(Collectors.toList());
        return Stream.of(new ListResult(values));
    }

    @Procedure("apoc.schema.properties.distinctCount")
    @Description("apoc.schema.properties.distinctCount([label], [key]) YIELD label, key, value, count - quickly returns all distinct values and counts for a given key")
    public Stream<PropertyValueCount> distinctCount(@Name(value = "label", defaultValue = "") String labelName, @Name(value = "key", defaultValue = "") String keyName) {

        ThreadToStatementContextBridge ctx = db.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class, DependencyResolver.SelectionStrategy.FIRST);
        BlockingQueue<PropertyValueCount> queue = new LinkedBlockingDeque<>(100);
        Iterable<IndexDefinition> indexDefinitions = (labelName.isEmpty()) ? db.schema().getIndexes() : db.schema().getIndexes(Label.label(labelName));

        new Thread(() ->
                StreamSupport.stream(indexDefinitions.spliterator(), true)
                .filter(indexDefinition -> isIndexCoveringProperty(indexDefinition, keyName))
                .map(indexDefinition -> scanIndexDefinitionForKeys(indexDefinition, keyName, ctx, queue))
                .collect(new QueuePoisoningCollector(queue, POISON))
        ).start();

        return StreamSupport.stream(new QueueBasedSpliterator<>(queue, POISON, terminationGuard, Integer.MAX_VALUE),false);
    }

    private Object scanIndexDefinitionForKeys(IndexDefinition indexDefinition, @Name(value = "key", defaultValue = "") String keyName, ThreadToStatementContextBridge ctx, BlockingQueue<PropertyValueCount> queue) {
        try (Transaction threadTx = db.beginTx()) {
            KernelTransaction ktx = ctx.getKernelTransactionBoundToThisThread(true);
            Iterable<String> keys = keyName.isEmpty() ? indexDefinition.getPropertyKeys() : Collections.singletonList(keyName);
            for (String key : keys) {
                try (KernelStatement ignored = (KernelStatement) ktx.acquireStatement()) {
                    SchemaRead schemaRead = ktx.schemaRead();
                    TokenRead tokenRead = ktx.tokenRead();
                    Read read = ktx.dataRead();
                    CursorFactory cursors = ktx.cursors();

                    int[] propertyKeyIds = StreamSupport.stream(indexDefinition.getPropertyKeys().spliterator(), false)
                            .mapToInt(name -> tokenRead.propertyKey(name))
                            .toArray();
                    LabelSchemaDescriptor schema = SchemaDescriptorFactory.forLabel(tokenRead.nodeLabel(indexDefinition.getLabel().name()), propertyKeyIds);
                    IndexReference indexReference = schemaRead.index(schema);
                    scanIndex(queue, indexDefinition, key, read, cursors, indexReference);
                }
            }
            threadTx.success();
            return null;
        }
    }

    private void scanIndex(BlockingQueue<PropertyValueCount> queue, IndexDefinition indexDefinition, String key, Read read, CursorFactory cursors, IndexReference indexReference) {
        try (NodeValueIndexCursor cursor = cursors.allocateNodeValueIndexCursor()) {
            // we need to using IndexOrder.NONE here to prevent an exception
            // however the index guarantees to be scanned in order unless
            // there are writes done in the same tx beforehand - which we don't do.
            read.nodeIndexScan(indexReference, cursor, IndexOrder.NONE, true);
            Value previousValue = NoValue.NO_VALUE;
            long count = 0;
            while (cursor.next()) {
                for (int i = 0; i < cursor.numberOfProperties(); i++) {
                    int k = cursor.propertyKey(i);
                    Value v = cursor.propertyValue(i);
                    if (v.equals(previousValue)) {
                        count++;
                    } else {
                        if (!previousValue.equals(NoValue.NO_VALUE)) {
                            putIntoQueue(queue, indexDefinition, key, previousValue, count);
                        }
                        previousValue = v;
                        count = 1;
                    }
                }
            }
            putIntoQueue(queue, indexDefinition, key, previousValue, count);
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
    }

    private void putIntoQueue(BlockingQueue<PropertyValueCount> queue, IndexDefinition indexDefinition, String key, Value value, long count) {
        try {
            queue.put(new PropertyValueCount(indexDefinition.getLabel().name(), key, value.asObject(), count));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isIndexCoveringProperty(IndexDefinition indexDefinition, String properttyKeyName) {
        try (Transaction threadTx = db.beginTx()) {
            threadTx.success();
            return properttyKeyName.isEmpty() || contains(indexDefinition.getPropertyKeys(), properttyKeyName);
        }
    }

    private boolean contains(Iterable<String> list, String search) {
        for (String element: list) {
            if (element.equals(search)) {
                return true;
            }
        }
        return false;
    }

    public static class PropertyValueCount {
        public String label;
        public String key;
        public Object value;
        public long count;

        public PropertyValueCount(String label, String key, Object value, long count) {
            this.label = label;
            this.key = key;
            this.value = value;
            this.count = count;
        }

        @Override
        public String toString() {
            return "PropertyValueCount{" +
                    "label='" + label + '\'' +
                    ", key='" + key + '\'' +
                    ", value='" + value + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
