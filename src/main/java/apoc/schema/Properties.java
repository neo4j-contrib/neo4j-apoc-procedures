package apoc.schema;

import apoc.Description;
import apoc.result.ListResult;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Sort;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Stream;

public class Properties {

    @Context
    public GraphDatabaseAPI db;
    @Context
    public KernelTransaction tx;


    public Properties(GraphDatabaseAPI db) {
        this.db = db;
    }

    public Properties() {
    }

    @Procedure("apoc.schema.properties.distinct")
    @Description("apoc.schema.properties.distinct(label, key) - quickly returns all distinct values for a given key")
    public Stream<ListResult> distinct(@Name("label") String label, @Name("key")  String key) throws SchemaRuleNotFoundException, IndexNotFoundKernelException, IOException {
        KernelStatement stmt = (KernelStatement) tx.acquireStatement();
        ReadOperations reads = stmt.readOperations();

        IndexDescriptor descriptor = reads.indexGetForLabelAndPropertyKey(reads.labelGetForName(label), reads.propertyKeyGetForName(key));
        SimpleIndexReader reader = (SimpleIndexReader) stmt.getStoreStatement().getIndexReader(descriptor);
        SortedIndexReader sortedIndexReader = new SortedIndexReader(reader, 0, Sort.INDEXORDER);

        Fields fields = MultiFields.getFields(sortedIndexReader.getIndexSearcher().getIndexReader());

        Terms terms = fields.terms("string");
        TermsEnum termsEnum = terms.iterator();
        ArrayList<String> values = new ArrayList<>();
        while((termsEnum.next()) != null){
            values.add(termsEnum.term().utf8ToString());
        }

        return Stream.of(new ListResult(values));
    }

}
