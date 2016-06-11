package apoc.refactor;

import apoc.Description;
import apoc.Pools;
import apoc.result.NodeResult;
import apoc.util.Util;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public class GraphRefactoring {
    @Context
    public GraphDatabaseService db;

    @Context
    public GraphDatabaseAPI dbAPI;

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
     * The other nodes are deleted and their relationships moved onto that first node.
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
     * Create category nodes from unique property values
     */
    @Procedure
    @PerformsWrites
    @Description("apoc.refactor.categorize(sourceKey, type, outgoing, label, targetKey, copiedKeys, batchSize) turn each unique propertyKey into a category node and connect to it")
    public void categorize(
            @Name("sourceKey") String sourceKey,
            @Name("type") String relationshipType,
            @Name("outgoing") Boolean outgoing,
            @Name("label") String label,
            @Name("targetKey") String targetKey,
            @Name("copiedKeys") List<String> copiedKeys,
            @Name("batchSize") long batchSize
    ) throws ExecutionException {
        // Verify and adjust arguments
        if (sourceKey == null)
            throw new IllegalArgumentException("Invalid (null) sourceKey");

        if (targetKey == null)
            throw new IllegalArgumentException("Invalid (null) targetKey");

        copiedKeys.remove(targetKey); // Just to be sure

        // Create batches of nodes
        List<Node> batch = null;
        List<Future<Void>> futures = new ArrayList<>();
        try(Transaction tx = dbAPI.beginTx()) {
            for (Node node : dbAPI.getAllNodes()) {
                if (batch == null) {
                    batch = new ArrayList<>((int) batchSize);
                }
                batch.add(node);
                if (batch.size() == batchSize) {
                    futures.add( categorizeNodes(batch, sourceKey, relationshipType, outgoing, label, targetKey, copiedKeys) );
                    batch = null;
                }
            }
            if (batch != null) {
                futures.add( categorizeNodes(batch, sourceKey, relationshipType, outgoing, label, targetKey, copiedKeys) );
            }

            // Await processing of node batches
            for (Future<Void> future : futures) {
                Pools.force(future);
            }
            tx.success();
        }
    }

    private Future<Void> categorizeNodes(List<Node> batch, String sourceKey, String relationshipType, Boolean outgoing, String label, String targetKey, List<String> copiedKeys) {
        return Pools.processBatch(batch, dbAPI, (Node node) -> {
            Object value = node.getProperty(sourceKey, null);
            if (value != null) {
                String q =
                        "WITH {node} AS n " +
                                "MERGE (cat:`" + label + "` {`" + targetKey + "`: {value}}) " +
                                (outgoing ? "MERGE (n)-[:`" + relationshipType + "`]->(cat) "
                                        : "MERGE (n)<-[:`" + relationshipType + "`]-(cat) ") +
                                "RETURN cat";
                Map<String, Object> params = new HashMap<>(2);
                params.put("node", node);
                params.put("value", value);
                Result result = dbAPI.execute(q, params);
                if (result.hasNext()) {
                    Node cat = (Node) result.next().get("cat");
                    for (String copiedKey : copiedKeys) {
                        Object copiedValue = node.getProperty(copiedKey, null);
                        if (copiedValue != null) {
                            Object catValue = cat.getProperty(copiedKey, null);
                            if (catValue == null) {
                                cat.setProperty(copiedKey, copiedValue);
                                node.removeProperty(copiedKey);
                            } else if (copiedValue.equals(catValue)) {
                                node.removeProperty(copiedKey);
                            }
                        }
                    }
                }
                assert (!result.hasNext());
                result.close();
                node.removeProperty(sourceKey);
            }
        });
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
