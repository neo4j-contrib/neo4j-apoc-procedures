package apoc.systemdb;

import apoc.ApocConfig;
import apoc.Extended;
import apoc.result.RowResult;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.Util;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Extended
public class SystemDb {

    @Context
    public ApocConfig apocConfig;
    
    @Context
    public SecurityContext securityContext;

    @Context
    public ProcedureCallContext callContext;

    public static class NodesAndRelationshipsResult {
        public List<Node> nodes;
        public List<Relationship> relationships;

        public NodesAndRelationshipsResult(List<Node> nodes, List<Relationship> relationships) {
            this.nodes = nodes;
            this.relationships = relationships;
        }
    }

    @Procedure
    public Stream<NodesAndRelationshipsResult> graph() {
        Util.checkAdmin(securityContext, callContext,"apoc.systemdb.graph");
        return withSystemDbTransaction(tx -> {
            Map<Long, Node> virtualNodes = new HashMap<>();
            for (Node node: tx.getAllNodes())  {
                virtualNodes.put(-node.getId(), new VirtualNode(-node.getId(), Iterables.asArray(Label.class, node.getLabels()), node.getAllProperties()));
            }

            List<Relationship> relationships = tx.getAllRelationships().stream().map(rel -> new VirtualRelationship(
                    -rel.getId(),
                    virtualNodes.get(-rel.getStartNodeId()),
                    virtualNodes.get(-rel.getEndNodeId()),
                    rel.getType(),
                    rel.getAllProperties())).collect(Collectors.toList()
            );
            return Stream.of(new NodesAndRelationshipsResult(Iterables.asList(virtualNodes.values()), relationships) );
        });
    }

    @Procedure
    public Stream<RowResult> execute(@Name("DDL command") String command, @Name(value="params", defaultValue = "{}") Map<String ,Object> params) {
        Util.checkAdmin(securityContext, callContext,"apoc.systemdb.execute");
        List<RowResult> rowResultStream = withSystemDbTransaction(tx -> tx.execute(command, params).stream().map(map -> new RowResult(map)).collect(Collectors.toList()));
        return rowResultStream.stream();
    }

    private <T> T withSystemDbTransaction(Function<Transaction, T> function) {
        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            T result = function.apply(tx);
            tx.commit();
            return result;
        }
    }
}
