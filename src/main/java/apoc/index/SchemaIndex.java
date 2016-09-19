package apoc.index;

import org.neo4j.procedure.Description;
import apoc.result.NodeResult;
import apoc.result.VirtualNode;
import apoc.util.Util;
import org.apache.lucene.search.Sort;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
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
    @Description("apoc.index.orderedRange(label,key,min,max,sort-relevance,limit) yield node - schema range scan which keeps index order and adds limit, values can be null, boundaries are inclusive")
    public Stream<NodeResult> orderedRange(@Name("label") String label, @Name("key") String key, @Name("min") Object min, @Name("max") Object max, @Name("relevance") boolean relevance, @Name("limit") long limit) throws SchemaRuleNotFoundException, IndexNotFoundKernelException {
        SortedIndexReader sortedIndexReader = getSortedIndexReader(label, key, limit);

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
        SortedIndexReader sortedIndexReader = getSortedIndexReader(label, key, limit);
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

    private SortedIndexReader getSortedIndexReader(String label, String key, long limit) throws SchemaRuleNotFoundException, IndexNotFoundKernelException {
        KernelStatement stmt = (KernelStatement) tx.acquireStatement();
        ReadOperations reads = stmt.readOperations();

        IndexDescriptor descriptor = reads.indexGetForLabelAndPropertyKey(reads.labelGetForName(label), reads.propertyKeyGetForName(key));
        // todo PartitionedIndexReader
        SimpleIndexReader reader = (SimpleIndexReader) stmt.getStoreStatement().getIndexReader(descriptor);
        return new SortedIndexReader(reader, limit, Sort.INDEXORDER);
    }
}
