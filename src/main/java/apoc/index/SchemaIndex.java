package apoc.index;

import com.hazelcast.util.IterableUtil;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.IndexDefinition;
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
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.IndexDefinition;
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
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.storageengine.api.schema.IndexReader;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
        Direction dir = Direction.BOTH;
        if (relationship.startsWith("<")) {
            dir = Direction.INCOMING;
            relationship = relationship.substring(1);
        }
        if (relationship.endsWith(">")) {
            dir = Direction.OUTGOING;
            relationship = relationship.substring(0, relationship.length() - 1);
        }
        RelationshipType type = RelationshipType.withName(relationship);
        List<Node> result = new ArrayList<>((int) limit);
        boolean reverse = false;
//        SortField sortField = new SortField("number" /*string*/, SortField.Type.STRING, reverse);
//        SortedIndexReader sortedIndexReader = getSortedIndexReader(label, key, Math.max(nodeSet.size(),limit), Sort.INDEXORDER); // new Sort(sortField));
        // PrimitiveLongIterator it = getIndexReader(label, key).rangeSeekByString("",true,String.valueOf((char)0xFF),true);
        int keyId;
        try (Statement stmt = tx.acquireStatement()) {
            keyId = stmt.readOperations().propertyKeyGetForName(key);
        }
        IndexQuery.NumberRangePredicate numberRangePredicate = IndexQuery.range(keyId, Long.MIN_VALUE, true, Long.MAX_VALUE, true);
        PrimitiveLongIterator it = getIndexReader(label, key).query(numberRangePredicate);

        while (it.hasNext() && result.size() < limit) {
            Node node = db.getNodeById(it.next());
//            System.out.println("id  " + node.getProperty("id") + " age " + node.getProperty("age"));
            for (Relationship rel : node.getRelationships(dir, type)) {
                Node other = rel.getOtherNode(node);
                if (nodeSet.contains(other)) {
                    result.add(node);
                    break;
                }
            }
        }
        return result.stream().map(NodeResult::new);
    }

    @Procedure
    @Description("apoc.index.orderedRange(label,key,min,max,sort-relevance,limit) yield node - schema range scan which keeps index order and adds limit, values can be null, boundaries are inclusive")
    public Stream<NodeResult> orderedRange(@Name("label") String label, @Name("key") String key, @Name("min") Object min, @Name("max") Object max, @Name("relevance") boolean relevance, @Name("limit") long limit) throws SchemaRuleNotFoundException, IndexNotFoundKernelException, DuplicateSchemaRuleException {

        SortedIndexReader sortedIndexReader = getSortedIndexReader(label, key, limit, getSort(min, max, relevance));

        PrimitiveLongIterator it = queryForRange(sortedIndexReader, min, max);
//        return Util.toLongStream(it).mapToObj(id -> new NodeResult(new VirtualNode(id, db)));
        return Util.toLongStream(it).mapToObj(id -> new NodeResult(db.getNodeById(id)));
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
//        return Util.toLongStream(it).mapToObj(id -> new NodeResult(new VirtualNode(id, db)));
        return Util.toLongStream(it).mapToObj(id -> new NodeResult(db.getNodeById(id)));
    }

    private PrimitiveLongIterator queryForString(SortedIndexReader sortedIndexReader, String operator, String value) {
        switch (operator.trim().toUpperCase()) {
            case "CONTAINS":
                return sortedIndexReader.containsString(value);
            case "STARTS WITH":
                return sortedIndexReader.rangeSeekByPrefix(value);
            // todo
            // case "ENDS WITH" : it = sortedIndexReader.rangeSeekByPrefix(value);
            default:
                throw new IllegalArgumentException("Unknown Operator " + operator);
        }
    }

    private SortedIndexReader getSortedIndexReader(String label, String key, long limit, Sort sort) throws SchemaRuleNotFoundException, IndexNotFoundKernelException, DuplicateSchemaRuleException {
        // todo PartitionedIndexReader
        SimpleIndexReader reader = (SimpleIndexReader) getIndexReader(label, key);
        return new SortedIndexReader(reader, limit, sort);
    }

    private IndexReader getIndexReader(String label, String key) throws SchemaRuleNotFoundException, IndexNotFoundKernelException, DuplicateSchemaRuleException {
        try (KernelStatement stmt = (KernelStatement) tx.acquireStatement()) {
            ReadOperations reads = stmt.readOperations();

            LabelSchemaDescriptor labelSchemaDescriptor = new LabelSchemaDescriptor(reads.labelGetForName(label), reads.propertyKeyGetForName(key));
            IndexDescriptor descriptor = reads.indexGetForSchema(labelSchemaDescriptor);
            return stmt.getStoreStatement().getIndexReader(descriptor);
        }
    }

    @Procedure("apoc.schema.properties.distinct")
    @Description("apoc.schema.properties.distinct(label, key) - quickly returns all distinct values for a given key")
    public Stream<ListResult> distinct(@Name("label") String label, @Name("key")  String key) throws SchemaRuleNotFoundException, IndexNotFoundKernelException, IOException, DuplicateSchemaRuleException {
        List<Object> values = distinctTerms(label, key);
        return Stream.of(new ListResult(values));
    }

    private List<Object> distinctTerms(@Name("label") String label, @Name("key") String key) throws SchemaRuleNotFoundException, IndexNotFoundKernelException, IOException, DuplicateSchemaRuleException {
        try (KernelStatement stmt = (KernelStatement) tx.acquireStatement()) {
            ReadOperations reads = stmt.readOperations();
    
            LabelSchemaDescriptor labelSchemaDescriptor = new LabelSchemaDescriptor(reads.labelGetForName(label), reads.propertyKeyGetForName(key));
            IndexDescriptor descriptor = reads.indexGetForSchema(labelSchemaDescriptor);
            SimpleIndexReader reader = (SimpleIndexReader) stmt.getStoreStatement().getIndexReader(descriptor);
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
    }

    @Procedure("apoc.schema.properties.distinctCount")
    @Description("apoc.schema.properties.distinctCount([label], [key]) YIELD label, key, value, count - quickly returns all distinct values and counts for a given key")
    public Stream<PropertyValueCount> distinctCount(@Name(value = "label", defaultValue = "") String labelName, @Name(value = "key", defaultValue = "") String keyName) throws SchemaRuleNotFoundException, IndexNotFoundKernelException, IOException {
        Iterable<IndexDefinition> labels = (labelName.isEmpty()) ? db.schema().getIndexes(Label.label(labelName)) : db.schema().getIndexes(Label.label(labelName));
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
            if (tx.isOpen()) {
                try {
                    tx.close();
                } catch (TransactionFailureException tfe) {
                    throw new RuntimeException("Error collecting distinct terms due to transaction failure", e);
                }
            }
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
