package apoc.nodes;

import apoc.Description;
import apoc.periodic.Periodic;
import apoc.result.LongResult;
import apoc.result.NodeResult;
import apoc.result.RelationshipResult;
import apoc.util.Util;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class Nodes {

    @Context public GraphDatabaseService db;
    @Context public GraphDatabaseAPI api;

    public Nodes(GraphDatabaseService db) {
        this.db = db;
    }

    public Nodes() {
    }

    @Procedure
    @PerformsWrites
    @Description("apoc.nodes.link([nodes],'REL_TYPE') - creates a linked list of nodes from first to last")
    public void link(@Name("nodes") List<Node> nodes, @Name("type") String type) {
        Iterator<Node> it = nodes.iterator();
        if (it.hasNext()) {
            RelationshipType relType = RelationshipType.withName(type);
            Node node = it.next();
            while (it.hasNext()) {
                Node next = it.next();
                node.createRelationshipTo(next, relType);
                node = next;
            }
        }
    }

    @Procedure
    @Description("apoc.nodes.get(node|nodes|id|[ids]) - quickly returns all nodes with these id's")
    public Stream<NodeResult> get(@Name("nodes") Object ids) {
        return Util.nodeStream(db, ids).map(NodeResult::new);
    }

    @Procedure
    @Description("apoc.nodes.delete(node|nodes|id|[ids]) - quickly delete all nodes with these id's")
    public Stream<LongResult> delete(@Name("nodes") Object ids, @Name("batchSize") long batchSize) {
        Iterator<Node> it = Util.nodeStream(db, ids).iterator();
        long count = 0;
        List<Node> batch = new ArrayList<>((int)batchSize);
        while (it.hasNext()) {
            Node node = it.next();
            batch.add(node);
            if (++count % batchSize == 0) {
                List<Node> submit = batch;
                batch = new ArrayList<>((int)batchSize);
                Periodic.submit("delete",() -> {
                    try (Transaction tx = api.beginTx()) {
                        for (Node n : submit) {
                            for (Relationship rel : node.getRelationships()) rel.delete();
                            n.delete();
                        }
                        tx.success();
                    };
                });
            }
        }
        return Stream.of(new LongResult(count));
    }

    @Procedure
    @Description("apoc.get.rels(rel|id|[ids]) - quickly returns all relationships with these id's")
    public Stream<RelationshipResult> rels(@Name("relationships") Object ids) {
        return Util.relsStream(db, ids).map(RelationshipResult::new);
    }

}
