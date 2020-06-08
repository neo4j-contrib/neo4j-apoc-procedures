package apoc.get;

import apoc.Extended;
import apoc.result.NodeResult;
import apoc.result.RelationshipResult;
import apoc.util.Util;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

@Extended
public class GetProcedures {

    @Context
    public Transaction tx;

    @Procedure
    @Description("apoc.get.nodes(node|id|[ids]) - quickly returns all nodes with these id's")
    public Stream<NodeResult> nodes(@Name("nodes") Object ids) {
        return new Get(tx).nodes(ids);
    }

    @Procedure
    @Description("apoc.get.rels(rel|id|[ids]) - quickly returns all relationships with these id's")
    public Stream<RelationshipResult> rels(@Name("relationships") Object ids) {
        return new Get(tx).rels(ids);
    }

}
