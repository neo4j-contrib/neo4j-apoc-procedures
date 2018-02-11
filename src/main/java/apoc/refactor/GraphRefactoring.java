package apoc.refactor;

import apoc.Pools;
import apoc.refactor.util.PropertiesManager;
import apoc.refactor.util.RefactorConfig;
import apoc.result.NodeResult;
import apoc.result.RelationshipResult;
import apoc.util.Util;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class GraphRefactoring {
    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    private Stream<NodeRefactorResult> doCloneNodes(@Name("nodes") List<Node> nodes, @Name("withRelationships") boolean withRelationships, List<String> skipProperties) {
        if (nodes == null) return Stream.empty();
        return nodes.stream().map((node) -> {
            NodeRefactorResult result = new NodeRefactorResult(node.getId());
            try {
                Node newNode = copyLabels(node, db.createNode());

                Map<String, Object> properties = node.getAllProperties();
                if (skipProperties!=null && !skipProperties.isEmpty())
                    for (String skip : skipProperties) properties.remove(skip);

                Node copy = copyProperties(properties, newNode);
                if (withRelationships) {
                    copyRelationships(node, copy,false);
                }
                return result.withOther(copy);
            } catch (Exception e) {
                return result.withError(e);
            }
        });
    }

    @Procedure(mode = Mode.WRITE)
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

    @Procedure(mode = Mode.WRITE)
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
    @Procedure(mode = Mode.WRITE)
    @Description("apoc.refactor.cloneNodes([node1,node2,...]) clone nodes with their labels and properties")
    public Stream<NodeRefactorResult> cloneNodes(@Name("nodes") List<Node> nodes,
                                                 @Name(value = "withRelationships",defaultValue = "false") boolean withRelationships,
                                                 @Name(value = "skipProperties",defaultValue = "[]") List<String> skipProperties) {
        return doCloneNodes(nodes,withRelationships,skipProperties);
    }

    /**
     * this procedure takes a list of nodes and clones them with their labels, properties and relationships
     */
    @Procedure(mode = Mode.WRITE)
    @Deprecated
    @Description("apoc.refactor.cloneNodesWithRelationships([node1,node2,...]) clone nodes with their labels, properties and relationships")
    public Stream<NodeRefactorResult> cloneNodesWithRelationships(@Name("nodes") List<Node> nodes) {
        return doCloneNodes(nodes,true, Collections.emptyList());
    }

    /**
     * Merges the nodes onto the first node.
     * The other nodes are deleted and their relationships moved onto that first node.
     */
    @Procedure(mode = Mode.WRITE)
    @Description("apoc.refactor.mergeNodes([node1,node2],[{properties:'override' or 'discard' or 'combine'}]) merge nodes onto first in list")
    public Stream<NodeResult> mergeNodes(@Name("nodes") List<Node> nodes, @Name(value= "config", defaultValue = "") Map<String, Object> config) {
        if (nodes == null || nodes.isEmpty()) return Stream.empty();
        // grab write locks upfront consistently ordered
        try (Transaction tx=db.beginTx()) {
	        nodes.stream().distinct().sorted(Comparator.comparingLong(Node::getId)).forEach( tx::acquireWriteLock );
            tx.success();
        }
        RefactorConfig conf = new RefactorConfig(config);
        final Node first = nodes.get(0);
        nodes.stream().skip(1).distinct().forEach(node -> mergeNodes(node, first, true, conf));
        return Stream.of(new NodeResult(first));
    }

    /**
     * Merges the relationships onto the first relationship and delete them.
     * All relationships must have the same starting node and ending node.
     */
    @Procedure(mode = Mode.WRITE)
    @Description("apoc.refactor.mergeRelationships([rel1,rel2]) merge relationships onto first in list")
    public Stream<RelationshipResult> mergeRelationships(@Name("rels") List<Relationship> relationships, @Name(value= "config", defaultValue = "") Map<String, Object> config) {
        if (relationships == null || relationships.isEmpty()) return Stream.empty();
        RefactorConfig conf = new RefactorConfig(config);
        Iterator<Relationship> it = relationships.iterator();
        Relationship first = it.next();
        while (it.hasNext()) {
            Relationship other = it.next();
            if(first.getStartNode().equals(other.getStartNode()) && first.getEndNode().equals(other.getEndNode()))
                mergeRels(other, first, true, conf);
            else
                throw new RuntimeException("All Relationships must have the same start and end nodes.");
        }
        return Stream.of(new RelationshipResult(first));
    }

    /**
     * Changes the relationship-type of a relationship by creating a new one between the two nodes
     * and deleting the old.
     */
    @Procedure(mode = Mode.WRITE)
    @Description("apoc.refactor.setType(rel, 'NEW-TYPE') change relationship-type")
    public Stream<RelationshipRefactorResult> setType(@Name("relationship") Relationship rel, @Name("newType") String newType) {
        if (rel == null) return Stream.empty();
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
    @Procedure(mode = Mode.WRITE)
    @Description("apoc.refactor.to(rel, endNode) redirect relationship to use new end-node")
    public Stream<RelationshipRefactorResult> to(@Name("relationship") Relationship rel, @Name("newNode") Node newNode) {
        if (rel == null || newNode == null) return Stream.empty();
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

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.refactor.invert(rel) inverts relationship direction")
    public Stream<RelationshipRefactorResult> invert(@Name("relationship") Relationship rel) {
        if (rel == null) return Stream.empty();
        RelationshipRefactorResult result = new RelationshipRefactorResult(rel.getId());
        try {
            Relationship newRel = rel.getEndNode().createRelationshipTo(rel.getStartNode(), rel.getType());
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
    @Procedure(mode = Mode.WRITE)
    @Description("apoc.refactor.from(rel, startNode) redirect relationship to use new start-node")
    public Stream<RelationshipRefactorResult> from(@Name("relationship") Relationship rel, @Name("newNode") Node newNode) {
        if (rel == null || newNode == null) return Stream.empty();
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
    @Procedure(mode = Mode.WRITE)
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
    @Procedure(mode = Mode.WRITE)
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
        try(Transaction tx = db.beginTx()) {
            for (Node node : db.getAllNodes()) {
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
        return Pools.processBatch(batch, db, (Node node) -> {
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
                Result result = db.execute(q, params);
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

    private Node mergeNodes(Node source, Node target, boolean delete, RefactorConfig conf) {
        try {
            Map<String, Object> properties = source.getAllProperties();
            copyRelationships(source, copyLabels(source, target), delete);
            if (delete) source.delete();
            PropertiesManager.mergeProperties(properties, target, conf);
        } catch (NotFoundException e) {
            log.warn("skipping a node for merging: " + e.getCause().getMessage());
        }
        return target;
    }

    private Relationship mergeRels(Relationship source, Relationship target, boolean delete, RefactorConfig conf) {
        Map<String, Object> properties = source.getAllProperties();
        if (delete) source.delete();
        PropertiesManager.mergeProperties(properties, target, conf);
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
        for (Label label : source.getLabels()) {
            if (!target.hasLabel(label)) {
                    target.addLabel(label);
            }
        }
        return target;
    }

    private <T extends PropertyContainer> T copyProperties(PropertyContainer source, T target) {
        return copyProperties(source.getAllProperties(),target);
    }

    private <T extends PropertyContainer> T copyProperties(Map<String,Object> source, T target) {
        for (Map.Entry<String, Object> prop : source.entrySet())
            target.setProperty(prop.getKey(), prop.getValue());
        return target;
    }

    private Relationship copyRelationship(Relationship rel, Node source, Node target) {
        Node startNode = rel.getStartNode();
        Node endNode = rel.getEndNode();

        if (startNode.getId() == source.getId()) {
            startNode = target;
        }

        if (endNode.getId() == source.getId()) {
            endNode = target;
        }

        Relationship newrel = startNode.createRelationshipTo(endNode, rel.getType());
        return copyProperties(rel, newrel);
    }
}
