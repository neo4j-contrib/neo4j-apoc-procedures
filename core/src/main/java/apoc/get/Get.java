package apoc.get;

import apoc.result.NodeResult;
import apoc.result.RelationshipResult;
import apoc.util.Util;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Name;

import java.util.stream.Stream;

public class Get {

    public Transaction tx;

    public Get(Transaction tx) {
        this.tx = tx;
    }

    public Stream<NodeResult> nodes(@Name("nodes") Object ids) {
        return Util.nodeStream(tx, ids).map(NodeResult::new);
    }

    public Stream<RelationshipResult> rels(@Name("relationships") Object ids) {
        return Util.relsStream(tx, ids).map(RelationshipResult::new);
    }

}
