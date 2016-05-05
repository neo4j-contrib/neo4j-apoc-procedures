package apoc.refactor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import apoc.Description;
import apoc.result.BooleanResult;
import apoc.result.NodeResult;
import apoc.util.Util;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import static apoc.util.MapUtil.map;

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

    @Procedure
    @PerformsWrites
    @Description("apoc.refactor.extractNode([rel1,rel2,...], [labels],'OUT','IN') extract node from relationships")
    public Stream<NodeRefactorResult> extractNode(@Name("relationships") Object rels, @Name("labels") List<String> labels, @Name("outType") String outType, @Name("inType") String inType) {
        return Util.relsStream(db, rels).map((rel) -> {
            NodeRefactorResult result = new NodeRefactorResult(rel.getId());
            try {
                Node copy = copyProperties(rel, db.createNode(Util.labels(labels)));
                copy.createRelationshipTo(rel.getEndNode(),RelationshipType.withName(outType));
                rel.getStartNode().createRelationshipTo(copy,RelationshipType.withName(inType));
                rel.delete();
                return result.withOther(copy);
            } catch (Exception e) {
                return result.withError(e);
            }
        });
    }

    @Procedure
    @PerformsWrites
    @Description("apoc.refactor.collapseNode([node1,node2],'TYPE') collapse node to relationship, node with one rel becomes self-relationship")
    public Stream<RelationshipRefactorResult> collapseNode(@Name("nodes") Object nodes, @Name("type") String type) {
        return Util.nodeStream(db, nodes).map((node) -> {
            RelationshipRefactorResult result = new RelationshipRefactorResult(node.getId());
            try {
                Iterable<Relationship> outRels = node.getRelationships(Direction.OUTGOING);
                Iterable<Relationship> inRels = node.getRelationships(Direction.INCOMING);
                if (node.getDegree(Direction.OUTGOING) == 1 &&  node.getDegree(Direction.INCOMING) == 1) {
                    Relationship outRel = outRels.iterator().next();
                    Relationship inRel = inRels.iterator().next();
                    Relationship newRel = inRel.getStartNode().createRelationshipTo(outRel.getEndNode(), RelationshipType.withName(type));
                    newRel = copyProperties(node, copyProperties(inRel, copyProperties(outRel, newRel)));

                    for (Relationship r : inRels) r.delete();
                    for (Relationship r : outRels) r.delete();
                    node.delete();

                    return result.withOther(newRel);
                } else {
                    return result.withError(String.format("Node %d has more that 1 outgoing %d or incoming %d relationships",node.getId(),node.getDegree(Direction.OUTGOING),node.getDegree(Direction.INCOMING)));
                }
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

    /**
     * Make properties boolean
     */
    @Procedure
    @PerformsWrites
    @Description("apoc.refactor.normalizeAsBoolean(entity, propertyKey, true_values, false_values) normalize/convert a property to be boolean")
    public void normalizeAsBoolean(
            @Name("entity") Object entity,
            @Name("propertyKey") String propertyKey,
            @Name("true_values") List<Object> trueValues,
            @Name("false_values") List<Object> falseValues) {
        if (entity instanceof PropertyContainer) {
            PropertyContainer pc = (PropertyContainer) entity;
            Object value = pc.getProperty(propertyKey, null);
            if (value != null) {
                boolean isTrue  = trueValues.contains(value);
                boolean isFalse = falseValues.contains(value);
                if (isTrue && !isFalse) {
                    pc.setProperty(propertyKey, true );
                }
                if (!isTrue && isFalse) {
                    pc.setProperty(propertyKey, false );
                }
                if (!isTrue && !isFalse) {
                    pc.removeProperty(propertyKey);
                }
            }
        }
    }

    /**
     * Create category nodes from a string-valued property
     */
    @Procedure
    @PerformsWrites
    @Description("apoc.refactor.categorize(node, propertyKey, type, outgoing, label) turn each unique propertyKey into a category node and connect to it")
    public void categorize(
        @Name("node") Node node,
        @Name("propertyKey") String propertyKey,
        @Name("type") String relationshipType,
        @Name("outgoing") Boolean outgoing,
        @Name("label") String label
    ) {
        Object value = node.getProperty(propertyKey, null);
        if (value != null) {
            String q = "WITH {node} AS n " +
                       "MERGE (cat:`" + label + "` {name: {value}}) " +
           (outgoing ? "MERGE (n)-[:`" + relationshipType + "`]->(cat)"
                     : "MERGE (n)<-[:`" + relationshipType + "`]-(cat)");
            Map<String, Object> params = new HashMap<>(2);
            params.put("node", node);
            params.put("value", value);
            Result result = db.execute(q, params);
            while (result.hasNext()) result.next();
            result.close();
            node.removeProperty(propertyKey);
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

    private <T extends PropertyContainer> T copyProperties(PropertyContainer source, T target) {
        for (Map.Entry<String, Object> prop : source.getAllProperties().entrySet())
            target.setProperty(prop.getKey(), prop.getValue());
        return target;
    }

    private Relationship copyRelationship(Relationship rel, Node source, Node newSource) {
        return copyProperties(rel,newSource.createRelationshipTo(rel.getOtherNode(source), rel.getType()));
    }

}
