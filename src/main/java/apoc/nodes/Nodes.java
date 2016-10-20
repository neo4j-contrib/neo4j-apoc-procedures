package apoc.nodes;

import apoc.Description;
import apoc.periodic.Periodic;
import apoc.result.LongResult;
import apoc.result.NodeResult;
import apoc.result.RelationshipResult;
import apoc.util.Util;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static apoc.util.Util.map;

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
    @PerformsWrites
    @Description("apoc.nodes.delete(node|nodes|id|[ids]) - quickly delete all nodes with these id's")
    public Stream<LongResult> delete(@Name("nodes") Object ids, @Name("batchSize") long batchSize) {
        Iterator<Node> it = Util.nodeStream(db, ids).iterator();
        long count = 0;
        while (it.hasNext()) {
            final List<Node> batch = Util.take(it, (int)batchSize);
//            count += Util.inTx(api,() -> batch.stream().peek( n -> {n.getRelationships().forEach(Relationship::delete);n.delete();}).count());
             count += Util.inTx(api,() -> {api.execute("FOREACH (n in {nodes} | DETACH DELETE n)",map("nodes",batch)).close();return batch.size();});
        }
        return Stream.of(new LongResult(count));
    }

    @Procedure
    @Description("apoc.get.rels(rel|id|[ids]) - quickly returns all relationships with these id's")
    public Stream<RelationshipResult> rels(@Name("relationships") Object ids) {
        return Util.relsStream(db, ids).map(RelationshipResult::new);
    }

    @Procedure
    @Description("apoc.nodes.isDense(node|nodes|id|[ids]) yield node, dense - returns each node and a 'dense' flag if it is a dense node")
    public Stream<DenseNodeResult> isDense(@Name("nodes") Object ids) {
        ThreadToStatementContextBridge ctx = api.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
        ReadOperations ops = ctx.get().readOperations();
        return Util.nodeStream(db, ids).map(n -> new DenseNodeResult(n, isDense(ops, n)));
    }

    public boolean isDense(ReadOperations ops, Node n) {
        try {
            return ops.nodeIsDense(n.getId());
        } catch (EntityNotFoundException e) {
            return false;
        }
    }

    public static class DenseNodeResult {
        public final Node node;
        public final boolean dense;

        public DenseNodeResult(Node node, boolean dense) {
            this.node = node;
            this.dense = dense;
        }
    }

}
