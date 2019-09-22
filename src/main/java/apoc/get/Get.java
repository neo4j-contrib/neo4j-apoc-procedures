package apoc.get;

import apoc.result.NodeResult;
import apoc.result.RelationshipResult;
import apoc.util.Util;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

public class Get {

    @Context
    public Transaction tx;

    public Get(Transaction tx) {
        this.tx = tx;
    }

    public Get() {
    }

    @Procedure
    @Description("apoc.get.nodes(node|id|[ids]) - quickly returns all nodes with these id's")
    public Stream<NodeResult> nodes(@Name("nodes") Object ids) {
        return Util.nodeStream(tx, ids).map(NodeResult::new);
    }

    @Procedure
    @Description("apoc.get.rels(rel|id|[ids]) - quickly returns all relationships with these id's")
    public Stream<RelationshipResult> rels(@Name("relationships") Object ids) {
        return Util.relsStream(tx, ids).map(RelationshipResult::new);
    }

}
