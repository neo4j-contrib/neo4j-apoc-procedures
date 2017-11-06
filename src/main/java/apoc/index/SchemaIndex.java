package apoc.index;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.procedure.Description;
import apoc.result.ListResult;
import apoc.result.NodeResult;
import apoc.util.Util;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Sort;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.impl.schema.reader.SimpleIndexReader;
import org.neo4j.kernel.api.impl.schema.reader.SortedIndexReader;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.storageengine.api.schema.IndexReader;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.util.MapUtil.map;
import static apoc.path.RelationshipTypeAndDirections.parse;

/**
 * @author mh
 * @since 23.05.16
 */
public class SchemaIndex {

    @Context
    public GraphDatabaseService db;
    @Context
    public KernelTransaction tx;

    @Procedure
    @Description("apoc.index.relatedNodes([nodes],label,key,'<TYPE'/'TYPE>'/'TYPE',limit) yield node - schema range scan which keeps index order and adds limit and checks opposite node of relationship against the given set of nodes")
    public Stream<NodeResult> related(@Name("nodes") List<Node> nodes,
                                      @Name("label") String label, @Name("key") String key,
                                      @Name("relationship") String relationship,
                                      @Name("limit") long limit) throws SchemaRuleNotFoundException, IndexNotFoundKernelException, IOException, DuplicateSchemaRuleException, IndexNotApplicableKernelException {
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
    @Description("apoc.index.orderedRange(label,key,min,max,sort-relevance,limit) yield node - schema range scan which keeps index order and adds limit, values can be null, boundaries are inclusive")
    public Stream<NodeResult> orderedRange(@Name("label") String label, @Name("key") String key, @Name("min") Object min, @Name("max") Object max, @Name("relevance") boolean relevance, @Name("limit") long limit) throws SchemaRuleNotFoundException, IndexNotFoundKernelException, DuplicateSchemaRuleException {

        return queryForRange(label, key, min, max, limit).map(NodeResult::new);
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

    public Sort getSort(Object min, Object max, boolean relevance) {
        return Sort.RELEVANCE;
/*
        return relevance ? Sort.RELEVANCE :
                    (min instanceof Number || max instanceof  Number) ? new Sort(new SortedNumericSortField("number", SortField.Type.DOUBLE)) :
                            (min instanceof String || max instanceof String) ? new Sort(new SortedNumericSortField("string", SortField.Type.STRING)) :
                            (min instanceof Boolean || max instanceof Boolean) ? new Sort(new SortedNumericSortField("bool", SortField.Type.STRING)) :
                            Sort.INDEXORDER;
*/
    }

    private PrimitiveLongIterator queryForRange(SortedIndexReader sortedIndexReader, Object min, Object max) {
        if ((min == null || min instanceof Number) && (max == null || max instanceof Number)) {
            return sortedIndexReader.rangeSeekByNumberInclusive((Number) min, (Number) max);
        }

        String minValue = min == null ? null : min.toString();
        String maxValue = max == null ? null : max.toString();
        return sortedIndexReader.rangeSeekByString(minValue, true, maxValue, true);
    }

    @Procedure
    @Description("apoc.index.orderedByText(label,key,operator,value,sort-relevance,limit) yield node - schema string search which keeps index order and adds limit, operator is 'STARTS WITH' or 'CONTAINS'")
    public Stream<NodeResult> orderedByText(@Name("label") String label, @Name("key") String key, @Name("operator") String operator, @Name("value") String value, @Name("relevance") boolean relevance, @Name("limit") long limit) throws SchemaRuleNotFoundException, IndexNotFoundKernelException, DuplicateSchemaRuleException {
        SortedIndexReader sortedIndexReader = getSortedIndexReader(label, key, limit, getSort(value,value,relevance));
        PrimitiveLongIterator it = queryForString(sortedIndexReader, operator, value);
        return Util.toLongStream(it).mapToObj(id -> new NodeResult(db.getNodeById(id)));
    }

    private PrimitiveLongIterator queryForString(SortedIndexReader sortedIndexReader, String operator, String value) {
        switch (operator.trim().toUpperCase()) {
            case "CONTAINS":
                return sortedIndexReader.containsString(value);
            case "STARTS WITH":
                return sortedIndexReader.rangeSeekByPrefix(value);
            case "ENDS WITH" :
                return sortedIndexReader.containsString('*'+value);
            default:
                throw new IllegalArgumentException("Unknown Operator " + operator);
        }
    }

    private SortedIndexReader getSortedIndexReader(String label, String key, long limit, Sort sort) throws SchemaRuleNotFoundException, IndexNotFoundKernelException, DuplicateSchemaRuleException {
        // todo PartitionedIndexReader
        SimpleIndexReader reader = getLuceneIndexReader(label, key);
        return new SortedIndexReader(reader, limit, sort);
    }

    private SimpleIndexReader getLuceneIndexReader(String label, String key) throws SchemaRuleNotFoundException, IndexNotFoundKernelException, DuplicateSchemaRuleException {
        try (KernelStatement stmt = (KernelStatement) tx.acquireStatement()) {
            ReadOperations reads = stmt.readOperations();

            LabelSchemaDescriptor labelSchemaDescriptor = new LabelSchemaDescriptor(reads.labelGetForName(label), reads.propertyKeyGetForName(key));
            IndexDescriptor descriptor = reads.indexGetForSchema(labelSchemaDescriptor);
            IndexReader indexReader = stmt.getStoreStatement().getIndexReader(descriptor);
            if (indexReader.getClass().getName().endsWith("FusionIndexReader")) {
                try {
                    Field field = indexReader.getClass().getDeclaredField("luceneReader");
                    field.setAccessible(true);
                    return (SimpleIndexReader) field.get(indexReader);
                } catch (IllegalAccessException | NoSuchFieldException e) {
                    throw new RuntimeException("Error accessing index reader",e);
                }
            }
            return (SimpleIndexReader)indexReader;
        }
    }

    @Procedure("apoc.schema.properties.distinct")
    @Description("apoc.schema.properties.distinct(label, key) - quickly returns all distinct values for a given key")
    public Stream<ListResult> distinct(@Name("label") String label, @Name("key")  String key) throws SchemaRuleNotFoundException, IndexNotFoundKernelException, IOException, DuplicateSchemaRuleException {
        List<Object> values = distinctTerms(label, key);
        return Stream.of(new ListResult(values));
    }

    private List<Object> distinctTerms(@Name("label") String label, @Name("key") String key) throws SchemaRuleNotFoundException, IndexNotFoundKernelException, IOException, DuplicateSchemaRuleException {
        SimpleIndexReader reader = getLuceneIndexReader(label,key);
        SortedIndexReader sortedIndexReader = new SortedIndexReader(reader, 0, Sort.INDEXORDER);
        Set<Object> values = new LinkedHashSet<>(100);
        TermsEnum termsEnum;

        Fields fields = MultiFields.getFields(sortedIndexReader.getIndexSearcher().getIndexReader());

        Terms terms = fields.terms("string");
        if (terms != null) {
            termsEnum = terms.iterator();
            while ((termsEnum.next()) != null) {
                values.add(termsEnum.term().utf8ToString());
            }
        }
        return new ArrayList<>(values);
    }

    @Procedure("apoc.schema.properties.distinctCount")
    @Description("apoc.schema.properties.distinctCount([label], [key]) YIELD label, key, value, count - quickly returns all distinct values and counts for a given key")
    public Stream<PropertyValueCount> distinctCount(@Name(value = "label", defaultValue = "") String labelName, @Name(value = "key", defaultValue = "") String keyName) throws SchemaRuleNotFoundException, IndexNotFoundKernelException, IOException {
        Iterable<IndexDefinition> labels = (labelName.isEmpty()) ? db.schema().getIndexes() : db.schema().getIndexes(Label.label(labelName));
        return StreamSupport.stream(labels.spliterator(), false).filter(i -> keyName.isEmpty() || isKeyIndexed(i, keyName)).flatMap(
                index -> {
                    Iterable<String> keys = keyName.isEmpty() ? index.getPropertyKeys() : Collections.singletonList(keyName);
                    return StreamSupport.stream(keys.spliterator(), false).flatMap(key -> {
                        String label = index.getLabel().name();
                        return distinctTermsCount(label, key).
                                entrySet().stream().map(e -> new PropertyValueCount(label, key, e.getKey(), e.getValue()));
                    });
                }
        );
    }

    private boolean isKeyIndexed(@Name("index") IndexDefinition index, @Name("key") String key) {
        return StreamSupport.stream(index.getPropertyKeys().spliterator(), false).anyMatch(k -> k.equals(key));
    }

    private Map<String, Integer> distinctTermsCount(@Name("label") String label, @Name("key") String key) {
        try {
            SortedIndexReader sortedIndexReader = getSortedIndexReader(label, key, 0, Sort.INDEXORDER);

            Fields fields = MultiFields.getFields(sortedIndexReader.getIndexSearcher().getIndexReader());

            Map<String, Integer> values = new HashMap<>();
            TermsEnum termsEnum;
            Terms terms = fields.terms("string");
            if (terms != null) {
                termsEnum = terms.iterator();
                while ((termsEnum.next()) != null) {
                    values.put(termsEnum.term().utf8ToString(), termsEnum.docFreq());
                }
            }
            return values;
        } catch (Exception e) {
            throw new RuntimeException("Error collecting distinct terms of label: " + label + " and key: " + key, e);
        }
    }

    public static class PropertyValueCount {
        public String label;
        public String key;
        public String value;
        public long count;

        public PropertyValueCount(String label, String key, String value, long count) {
            this.label = label;
            this.key = key;
            this.value = value;
            this.count = count;
        }
    }
}
