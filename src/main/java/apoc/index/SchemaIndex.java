package apoc.index;

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
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.impl.schema.reader.SimpleIndexReader;
import org.neo4j.kernel.api.impl.schema.reader.SortedIndexReader;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.storageengine.api.schema.IndexReader;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 23.05.16
 */
public class SchemaIndex {

    @Context
    public GraphDatabaseAPI db;
    @Context
    public KernelTransaction tx;

    @Procedure
    @Description("apoc.index.relatedNodes([nodes],label,key,'<TYPE'/'TYPE>'/'TYPE',limit) yield node - schema range scan which keeps index order and adds limit and checks opposite node of relationship against the given set of nodes")
    public Stream<NodeResult> related(@Name("nodes") List<Node> nodes,
                                           @Name("label") String label, @Name("key") String key,
                                           @Name("relationship") String relationship,
                                           @Name("limit") long limit) throws SchemaRuleNotFoundException, IndexNotFoundKernelException, IOException {
        Set<Node> nodeSet = new HashSet<>(nodes);
        Direction dir = Direction.BOTH;
        if (relationship.startsWith("<")) {dir = Direction.INCOMING; relationship = relationship.substring(1);}
        if (relationship.endsWith(">"))   {dir = Direction.OUTGOING; relationship = relationship.substring(0,relationship.length()-1);}
        RelationshipType type = RelationshipType.withName(relationship);
        System.out.println(distinctTerms(label,key));
        List<Node> result = new ArrayList<>((int)limit);
        boolean reverse = false;
//        SortField sortField = new SortField("number" /*string*/, SortField.Type.STRING, reverse);
//        SortedIndexReader sortedIndexReader = getSortedIndexReader(label, key, Math.max(nodeSet.size(),limit), Sort.INDEXORDER); // new Sort(sortField));
        // PrimitiveLongIterator it = getIndexReader(label, key).rangeSeekByString("",true,String.valueOf((char)0xFF),true);
        PrimitiveLongIterator it = getIndexReader(label, key).rangeSeekByNumberInclusive(Long.MIN_VALUE,Long.MAX_VALUE);
//        PrimitiveLongIterator it = sortedIndexReader.query(new MatchAllDocsQuery());

        while (it.hasNext() && result.size() < limit) {
            Node node = db.getNodeById(it.next());
            System.out.println("id  " + node.getProperty("id") + " age " + node.getProperty("age"));
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
    public Stream<NodeResult> orderedRange(@Name("label") String label, @Name("key") String key, @Name("min") Object min, @Name("max") Object max, @Name("relevance") boolean relevance, @Name("limit") long limit) throws SchemaRuleNotFoundException, IndexNotFoundKernelException {
        SortedIndexReader sortedIndexReader = getSortedIndexReader(label, key, limit, Sort.INDEXORDER);

        PrimitiveLongIterator it = queryForRange(sortedIndexReader, min, max);
//        return Util.toLongStream(it).mapToObj(id -> new NodeResult(new VirtualNode(id, db)));
        return Util.toLongStream(it).mapToObj(id -> new NodeResult(db.getNodeById(id)));
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
    public Stream<NodeResult> orderedByText(@Name("label") String label, @Name("key") String key, @Name("operator") String operator, @Name("value") String value, @Name("relevance") boolean relevance, @Name("limit") long limit) throws SchemaRuleNotFoundException, IndexNotFoundKernelException {
        SortedIndexReader sortedIndexReader = getSortedIndexReader(label, key, limit, Sort.INDEXORDER);
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

    private SortedIndexReader getSortedIndexReader(String label, String key, long limit, Sort sort) throws SchemaRuleNotFoundException, IndexNotFoundKernelException {
        // todo PartitionedIndexReader
        SimpleIndexReader reader = (SimpleIndexReader) getIndexReader(label, key);
        return new SortedIndexReader(reader, limit, sort);
    }

    private IndexReader getIndexReader(String label, String key) throws SchemaRuleNotFoundException, IndexNotFoundKernelException {
        KernelStatement stmt = (KernelStatement) tx.acquireStatement();
        ReadOperations reads = stmt.readOperations();

        IndexDescriptor descriptor = reads.indexGetForLabelAndPropertyKey(reads.labelGetForName(label), reads.propertyKeyGetForName(key));
        return stmt.getStoreStatement().getIndexReader(descriptor);
    }

    @Procedure("apoc.schema.properties.distinct")
    @Description("apoc.schema.properties.distinct(label, key) - quickly returns all distinct values for a given key")
    public Stream<ListResult> distinct(@Name("label") String label, @Name("key")  String key) throws SchemaRuleNotFoundException, IndexNotFoundKernelException, IOException {
        List<Object> values = distinctTerms(label, key);
        return Stream.of(new ListResult(values));
    }

    private List<Object> distinctTerms(@Name("label") String label, @Name("key") String key) throws SchemaRuleNotFoundException, IndexNotFoundKernelException, IOException {
        KernelStatement stmt = (KernelStatement) tx.acquireStatement();
        ReadOperations reads = stmt.readOperations();

        IndexDescriptor descriptor = reads.indexGetForLabelAndPropertyKey(reads.labelGetForName(label), reads.propertyKeyGetForName(key));
        SimpleIndexReader reader = (SimpleIndexReader) stmt.getStoreStatement().getIndexReader(descriptor);
        SortedIndexReader sortedIndexReader = new SortedIndexReader(reader, 0, Sort.INDEXORDER);

        Fields fields = MultiFields.getFields(sortedIndexReader.getIndexSearcher().getIndexReader());

        Set<Object> values = new LinkedHashSet<>(100);
        TermsEnum termsEnum;
        Terms terms = fields.terms("string");
        if (terms != null) {
            termsEnum = terms.iterator();
            while ((termsEnum.next()) != null) {
                values.add(termsEnum.term().utf8ToString());
            }
        }
        /*
        terms = fields.terms("number");
        if (terms != null) {
            termsEnum = terms.iterator();
            while ((termsEnum.next()) != null) {
                BytesRef value = termsEnum.term();
                System.out.println("value = " + NumericUtils.prefixCodedToLong(value));
//                values.add(value.isEmpty() ? null : Double.parseDouble(value));
            }
        }
        */
        return new ArrayList<>(values);
    }

}
