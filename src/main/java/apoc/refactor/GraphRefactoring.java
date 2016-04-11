package apoc.refactor;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import apoc.Description;
import apoc.result.NodeResult;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

public class GraphRefactoring {
    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    private Stream<NodeRefactorResult> doCloneNodes(@Name("nodes") List<Node> nodes, @Name("withRelationships") boolean withRelationships) {
        return nodes.stream().map((node) -> {
            NodeRefactorResult result = new NodeRefactorResult(node.getId());
            try {
                Node copy = copyProperties(node, copyLabels(node, db.createNode()));
                if (withRelationships) {
                    copyRelationships(node, copy,false);
                }
                return result.withOther(copy);
            } catch (Exception e) {
                return result.withError(e);
            }
        });
    }

    /**
     * this procedure takes a list of nodes and clones them with their labels and properties
     */
    @Procedure
    @PerformsWrites
    @Description("apoc.refactor.cloneNodes([node1,node2,...]) clone nodes with their labels and properties")
    public Stream<NodeRefactorResult> cloneNodes(@Name("nodes") List<Node> nodes) {
        return doCloneNodes(nodes,false);
    }

    /**
     * this procedure takes a list of nodes and clones them with their labels, properties and relationships
     */
    @Procedure
    @PerformsWrites
    @Description("apoc.refactor.cloneNodesWithRelationships([node1,node2,...]) clone nodes with their labels, properties and relationships")
    public Stream<NodeRefactorResult> cloneNodesWithRelationships(@Name("nodes") List<Node> nodes) {
        return doCloneNodes(nodes,true);
    }

    /**
     * Merges the nodes onto the first node.
     * The other nodes and relationships are deleted.
     */
    @Procedure
    @PerformsWrites
    @Description("apoc.refactor.mergeNodes([node1,node2]) merge nodes onto first in list")
    public Stream<NodeResult> mergeNodes(@Name("nodes") List<Node> nodes) {
        if (nodes.isEmpty()) return Stream.empty();
        Iterator<Node> it = nodes.iterator();
        Node first = it.next();
        while (it.hasNext()) {
            Node other = it.next();
            mergeNodes(other, first, true);
        }
        return Stream.of(new NodeResult(first));
    }

    /**
     * Changes the relationship-type of a relationship by creating a new one between the two nodes
     * and deleting the old.
     */
    @Procedure
    @PerformsWrites
    @Description("apoc.refactor.setType(rel, 'NEW-TYPE') change relationship-type")
    public Stream<RelationshipRefactorResult> setType(@Name("relationship") Relationship rel, @Name("newType") String newType) {
        RelationshipRefactorResult result = new RelationshipRefactorResult(rel.getId());
        try {
            Relationship newRel = rel.getStartNode().createRelationshipTo(rel.getEndNode(), RelationshipType.withName(newType));
            copyProperties(rel, newRel);
            rel.delete();
            return Stream.of(result.withOther(newRel));
        } catch (Exception e) {
            return Stream.of(result.withError(e));
        }
    }

    /**
     * Redirects a relationships to a new target node.
     */
    @Procedure
    @PerformsWrites
    @Description("apoc.refactor.to(rel, endNode) redirect relationship to use new end-node")
    public Stream<RelationshipRefactorResult> to(@Name("relationship") Relationship rel, @Name("newNode") Node newNode) {
        RelationshipRefactorResult result = new RelationshipRefactorResult(rel.getId());
        try {
            Relationship newRel = rel.getStartNode().createRelationshipTo(newNode, rel.getType());
            copyProperties(rel,newRel);
            rel.delete();
            return Stream.of(result.withOther(newRel));
        } catch (Exception e) {
            return Stream.of(result.withError(e));
        }
    }

    /**
     * Redirects a relationships to a new target node.
     */
    @Procedure
    @PerformsWrites
    @Description("apoc.refactor.from(rel, startNode) redirect relationship to use new start-node")
    public Stream<RelationshipRefactorResult> from(@Name("relationship") Relationship rel, @Name("newNode") Node newNode) {
        RelationshipRefactorResult result = new RelationshipRefactorResult(rel.getId());
        try {
            Relationship newRel = newNode.createRelationshipTo(rel.getEndNode(), rel.getType());
            copyProperties(rel,newRel);
            rel.delete();
            return Stream.of(result.withOther(newRel));
        } catch (Exception e) {
            return Stream.of(result.withError(e));
        }
    }

    private Node mergeNodes(Node source, Node target, boolean delete) {
        copyRelationships(source, copyProperties(source, copyLabels(source, target)), delete);
        if (delete) source.delete();
        return target;
    }

    private Node copyRelationships(Node source, Node target, boolean delete) {
        for (Relationship rel : source.getRelationships()) {
            copyRelationship(rel, source, target);
            if (delete) rel.delete();
        }
        return target;
    }

    private Node copyLabels(Node source, Node target) {
        for (Label label : source.getLabels()) target.addLabel(label);
        return target;
    }

    private <T extends PropertyContainer> T copyProperties(T source, T target) {
        for (Map.Entry<String, Object> prop : source.getAllProperties().entrySet())
            target.setProperty(prop.getKey(), prop.getValue());
        return target;
    }

    private Relationship copyRelationship(Relationship rel, Node source, Node newSource) {
        return copyProperties(rel,newSource.createRelationshipTo(rel.getOtherNode(source), rel.getType()));
    }

}
