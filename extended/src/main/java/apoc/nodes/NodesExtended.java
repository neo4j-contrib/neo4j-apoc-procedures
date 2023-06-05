package apoc.nodes;

import apoc.Extended;
import apoc.util.EntityUtil;
import apoc.util.Util;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

@Extended
public class NodesExtended {

    @Context
    public Transaction tx;
    
    @UserFunction("apoc.node.rebind")
    @Description("apoc.node.rebind(node - to rebind a node (i.e. executing a Transaction.getNodeById(node.getId())  ")
    public Node nodeRebind(@Name("node") Node node) {
        return Util.rebind(tx, node);
    }

    @UserFunction("apoc.rel.rebind")
    @Description("apoc.rel.rebind(rel) - to rebind a rel (i.e. executing a Transaction.getRelationshipById(rel.getId())  ")
    public Relationship relationshipRebind(@Name("rel") Relationship rel) {
        return Util.rebind(tx, rel);
    }

    @UserFunction("apoc.any.rebind")
    @Description("apoc.any.rebind(Object) - to rebind any rel, node, path, map, list or combination of them (i.e. executing a Transaction.getNodeById(node.getId()) / Transaction.getRelationshipById(rel.getId()))")
    public Object anyRebind(@Name("any") Object any) {
        return EntityUtil.anyRebind(tx, any);
    }
}
