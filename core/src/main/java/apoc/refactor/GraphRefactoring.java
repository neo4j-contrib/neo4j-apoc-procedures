package apoc.refactor;

import apoc.Pools;
import apoc.algo.Cover;
import apoc.refactor.util.PropertiesManager;
import apoc.refactor.util.RefactorConfig;
import apoc.result.GraphResult;
import apoc.result.NodeResult;
import apoc.result.RelationshipResult;
import apoc.util.Util;
import org.apache.commons.collections4.IterableUtils;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.refactor.util.PropertiesManager.mergeProperties;
import static apoc.refactor.util.RefactorConfig.RelationshipSelectionStrategy.MERGE;
import static apoc.refactor.util.RefactorUtil.*;

public class GraphRefactoring {
    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Context
    public Pools pools;

    private Stream<NodeRefactorResult> doCloneNodes(@Name("nodes") List<Node> nodes, @Name("withRelationships") boolean withRelationships, List<String> skipProperties) {
        if (nodes == null) return Stream.empty();
        return nodes.stream().map(node -> Util.rebind(tx, node)).map(node -> {
            NodeRefactorResult result = new NodeRefactorResult(node.getId());
            try {
                Node newNode = copyLabels(node, tx.createNode());

                Map<String, Object> properties = node.getAllProperties();
                if (skipProperties != null && !skipProperties.isEmpty())
                    for (String skip : skipProperties) properties.remove(skip);

                Node copy = copyProperties(properties, newNode);
                if (withRelationships) {
                    copyRelationships(node, copy, false);
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
        return Util.relsStream(tx, rels).map((rel) -> {
            NodeRefactorResult result = new NodeRefactorResult(rel.getId());
            try {
                Node copy = copyProperties(rel, tx.createNode(Util.labels(labels)));
                copy.createRelationshipTo(rel.getEndNode(), RelationshipType.withName(outType));
                rel.getStartNode().createRelationshipTo(copy, RelationshipType.withName(inType));
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
        return Util.nodeStream(tx, nodes).map((node) -> {
            RelationshipRefactorResult result = new RelationshipRefactorResult(node.getId());
            try {
                Iterable<Relationship> outRels = node.getRelationships(Direction.OUTGOING);
                Iterable<Relationship> inRels = node.getRelationships(Direction.INCOMING);
                if (node.getDegree(Direction.OUTGOING) == 1 && node.getDegree(Direction.INCOMING) == 1) {
                    Relationship outRel = outRels.iterator().next();
                    Relationship inRel = inRels.iterator().next();
                    Relationship newRel = inRel.getStartNode().createRelationshipTo(outRel.getEndNode(), RelationshipType.withName(type));
                    newRel = copyProperties(node, copyProperties(inRel, copyProperties(outRel, newRel)));

                    for (Relationship r : inRels) r.delete();
                    for (Relationship r : outRels) r.delete();
                    node.delete();

                    return result.withOther(newRel);
                } else {
                    return result.withError(String.format("Node %d has more that 1 outgoing %d or incoming %d relationships", node.getId(), node.getDegree(Direction.OUTGOING), node.getDegree(Direction.INCOMING)));
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
                                                 @Name(value = "withRelationships", defaultValue = "false") boolean withRelationships,
                                                 @Name(value = "skipProperties", defaultValue = "[]") List<String> skipProperties) {
        return doCloneNodes(nodes, withRelationships, skipProperties);
    }

    /**
     * this procedure takes a list of nodes and clones them with their labels, properties and relationships
     */
    @Procedure(mode = Mode.WRITE)
    @Deprecated
    @Description("apoc.refactor.cloneNodesWithRelationships([node1,node2,...]) clone nodes with their labels, properties and relationships")
    public Stream<NodeRefactorResult> cloneNodesWithRelationships(@Name("nodes") List<Node> nodes) {
        return doCloneNodes(nodes, true, Collections.emptyList());
    }

    /**
     * this procedure clones a subgraph defined by a list of nodes and relationships. The resulting clone is a disconnected subgraph,
     * with no relationships connecting with the original nodes, nor with any other node outside the subgraph clone.
     * This can be overridden by supplying a list of node pairings in the `standinNodes` config property, so any relationships that went to the old node, when cloned, will instead be redirected to the standin node.
     * This is useful when instead of cloning a certain node or set of nodes, you want to instead redirect relationships in the resulting clone
     * such that they point to some existing node in the graph.
     *
     * For example, this could be used to clone a branch from a tree structure (with none of the new relationships going
     * to the original nodes) and to redirect any relationships from an old root node (which will not be cloned) to a different existing root node, which acts as the standin.
     *
     */
    @Procedure(mode = Mode.WRITE)
    @Description("apoc.refactor.cloneSubgraphFromPaths([path1, path2, ...], {standinNodes:[], skipProperties:[]}) YIELD input, output, error | " +
            "from the subgraph formed from the given paths, clone nodes with their labels and properties (optionally skipping any properties in the skipProperties list via the config map), and clone the relationships (will exist between cloned nodes only). " +
            "Relationships can be optionally redirected according to standinNodes node pairings (this is a list of list-pairs of nodes), so given a node in the original subgraph (first of the pair), " +
            "an existing node (second of the pair) can act as a standin for it within the cloned subgraph. Cloned relationships will be redirected to the standin.")
    public Stream<NodeRefactorResult> cloneSubgraphFromPaths(@Name("paths") List<Path> paths,
                                                             @Name(value="config", defaultValue = "{}") Map<String, Object> config) {

        if (paths == null || paths.isEmpty()) return Stream.empty();

        Set<Node> nodes = new HashSet<>();
        Set<Relationship> rels = new HashSet<>();

        for (Path path : paths) {
            for (Relationship rel : path.relationships()) {
                rels.add(rel);
            }

            for (Node node : path.nodes()) {
                nodes.add(node);
            }
        }

        List<Node> nodesList = new ArrayList<>(nodes);
        List<Relationship> relsList = new ArrayList<>(rels);

        return cloneSubgraph(nodesList, relsList, config);
    }

    /**
     * this procedure clones a subgraph defined by a list of nodes and relationships. The resulting clone is a disconnected subgraph,
     * with no relationships connecting with the original nodes, nor with any other node outside the subgraph clone.
     * This can be overridden by supplying a list of node pairings in the `standinNodes` config property, so any relationships that went to the old node, when cloned, will instead be redirected to the standin node.
     * This is useful when instead of cloning a certain node or set of nodes, you want to instead redirect relationships in the resulting clone
     * such that they point to some existing node in the graph.
     *
     * For example, this could be used to clone a branch from a tree structure (with none of the new relationships going
     * to the original nodes) and to redirect any relationships from an old root node (which will not be cloned) to a different existing root node, which acts as the standin.
     *
     */
    @Procedure(mode = Mode.WRITE)
    @Description("apoc.refactor.cloneSubgraph([node1,node2,...], [rel1,rel2,...]:[], {standinNodes:[], skipProperties:[]}) YIELD input, output, error | " +
            "clone nodes with their labels and properties (optionally skipping any properties in the skipProperties list via the config map), and clone the given relationships (will exist between cloned nodes only). " +
            "If no relationships are provided, all relationships between the given nodes will be cloned. " +
            "Relationships can be optionally redirected according to standinNodes node pairings (this is a list of list-pairs of nodes), so given a node in the original subgraph (first of the pair), " +
            "an existing node (second of the pair) can act as a standin for it within the cloned subgraph. Cloned relationships will be redirected to the standin.")
    public Stream<NodeRefactorResult> cloneSubgraph(@Name("nodes") List<Node> nodes,
                                                    @Name(value="rels", defaultValue = "[]") List<Relationship> rels,
                                                    @Name(value="config", defaultValue = "{}") Map<String, Object> config) {

        if (nodes == null || nodes.isEmpty()) return Stream.empty();

        // empty or missing rels list means get all rels between nodes
        if (rels == null || rels.isEmpty()) {
            rels = Cover.coverNodes(nodes).collect(Collectors.toList());
        }

        Map<Node, Node> copyMap = new HashMap<>(nodes.size());
        List<NodeRefactorResult> resultStream = new ArrayList<>();

        Map<Node, Node> standinMap = generateStandinMap((List<List<Node>>) config.getOrDefault("standinNodes", Collections.emptyList()));
        List<String> skipProperties = (List<String>) config.getOrDefault("skipProperties", Collections.emptyList());

        // clone nodes and populate copy map
        for (Node node : nodes) {
            if (node == null || standinMap.containsKey(node)) continue;
            // standinNodes will NOT be cloned

            NodeRefactorResult result = new NodeRefactorResult(node.getId());
            try {
                Node copy = copyLabels(node, tx.createNode());

                Map<String, Object> properties = node.getAllProperties();
                if (skipProperties != null && !skipProperties.isEmpty()) {
                    for (String skip : skipProperties) properties.remove(skip);
                }
                copy = copyProperties(properties, copy);

                resultStream.add(result.withOther(copy));
                copyMap.put(node, copy);
            } catch (Exception e) {
                resultStream.add(result.withError(e));
            }
        }

        // clone relationships, will be between cloned nodes and/or standins
        for (Relationship rel : rels) {
            if (rel == null) continue;

            Node oldStart = rel.getStartNode();
            Node newStart = standinMap.getOrDefault(oldStart, copyMap.get(oldStart));

            Node oldEnd = rel.getEndNode();
            Node newEnd = standinMap.getOrDefault(oldEnd, copyMap.get(oldEnd));

            if (newStart != null && newEnd != null) {
                Relationship newrel = newStart.createRelationshipTo(newEnd, rel.getType());
                Map<String, Object> properties = rel.getAllProperties();
                if (skipProperties != null && !skipProperties.isEmpty()) {
                    for (String skip : skipProperties) properties.remove(skip);
                }
                copyProperties(properties, newrel);            }
        }

        return resultStream.stream();
    }

    private Map<Node, Node> generateStandinMap(List<List<Node>> standins) {
        Map<Node, Node> standinMap = standins.isEmpty() ? Collections.emptyMap() : new HashMap<>(standins.size());

        for (List<Node> pairing : standins) {
            if (pairing == null) continue;

            if (pairing.size() != 2) {
                throw new IllegalArgumentException("\'standinNodes\' must be a list of node pairs");
            }

            Node from = pairing.get(0);
            Node to = pairing.get(1);

            if (from == null || to == null) {
                throw new IllegalArgumentException("\'standinNodes\' must be a list of node pairs");
            }

            standinMap.put(from, to);
        }

        return standinMap;
    }

    /**
     * Merges the nodes onto the first node.
     * The other nodes are deleted and their relationships moved onto that first node.
     */
    @Procedure(mode = Mode.WRITE,eager = true)
    @Description("apoc.refactor.mergeNodes([node1,node2],[{properties:'overwrite' or 'discard' or 'combine'}]) merge nodes onto first in list")
    public Stream<NodeResult> mergeNodes(@Name("nodes") List<Node> nodes, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if (nodes == null || nodes.isEmpty()) return Stream.empty();
        RefactorConfig conf = new RefactorConfig(config);
        // grab write locks upfront consistently ordered
        nodes.stream().distinct().sorted(Comparator.comparingLong(Node::getId)).forEach(tx::acquireWriteLock);

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
    public Stream<RelationshipResult> mergeRelationships(@Name("rels") List<Relationship> relationships, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if (relationships == null || relationships.isEmpty()) return Stream.empty();
        RefactorConfig conf = new RefactorConfig(config);
        Iterator<Relationship> it = relationships.iterator();
        Relationship first = it.next();
        while (it.hasNext()) {
            Relationship other = it.next();
            if (first.getStartNode().equals(other.getStartNode()) && first.getEndNode().equals(other.getEndNode()))
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
    @Procedure(mode = Mode.WRITE,eager = true)
    @Description("  apoc.refactor.to(rel, endNode) redirect relationship to use new end-node")
    public Stream<RelationshipRefactorResult> to(@Name("relationship") Relationship rel, @Name("newNode") Node newNode) {
        if (rel == null || newNode == null) return Stream.empty();
        RelationshipRefactorResult result = new RelationshipRefactorResult(rel.getId());
        try {
            Relationship newRel = rel.getStartNode().createRelationshipTo(newNode, rel.getType());
            copyProperties(rel, newRel);
            rel.delete();
            return Stream.of(result.withOther(newRel));
        } catch (Exception e) {
            return Stream.of(result.withError(e));
        }
    }

    @Procedure(mode = Mode.WRITE,eager = true)
    @Description("apoc.refactor.invert(rel) inverts relationship direction")
    public Stream<RelationshipRefactorResult> invert(@Name("relationship") Relationship rel) {
        if (rel == null) return Stream.empty();
        RelationshipRefactorResult result = new RelationshipRefactorResult(rel.getId());
        try {
            Relationship newRel = rel.getEndNode().createRelationshipTo(rel.getStartNode(), rel.getType());
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
    @Procedure(mode = Mode.WRITE, eager = true)
    @Description("apoc.refactor.from(rel, startNode) redirect relationship to use new start-node")
    public Stream<RelationshipRefactorResult> from(@Name("relationship") Relationship rel, @Name("newNode") Node newNode) {
        if (rel == null || newNode == null) return Stream.empty();
        RelationshipRefactorResult result = new RelationshipRefactorResult(rel.getId());
        try {
            Relationship newRel = newNode.createRelationshipTo(rel.getEndNode(), rel.getType());
            copyProperties(rel, newRel);
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
        if (entity instanceof Entity) {
            Entity pc = (Entity) entity;
            Object value = pc.getProperty(propertyKey, null);
            if (value != null) {
                boolean isTrue = trueValues.contains(value);
                boolean isFalse = falseValues.contains(value);
                if (isTrue && !isFalse) {
                    pc.setProperty(propertyKey, true);
                }
                if (!isTrue && isFalse) {
                    pc.setProperty(propertyKey, false);
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

        if (!isUniqueConstraintDefinedFor(label, targetKey)) {
            throw new IllegalArgumentException("Before execute this procedure you must define an unique constraint for the label and the targetKey:\n"
                    + String.format("CREATE CONSTRAINT ON (n:`%s`) ASSERT n.`%s` IS UNIQUE", label, targetKey));
        }

        // Create batches of nodes
        List<Node> batch = null;
        List<Future<Void>> futures = new ArrayList<>();
        for (Node node : tx.getAllNodes()) {
            if (batch == null) {
                batch = new ArrayList<>((int) batchSize);
            }
            batch.add(node);
            if (batch.size() == batchSize) {
                futures.add(categorizeNodes(batch, sourceKey, relationshipType, outgoing, label, targetKey, copiedKeys));
                batch = null;
            }
        }
        if (batch != null) {
            futures.add(categorizeNodes(batch, sourceKey, relationshipType, outgoing, label, targetKey, copiedKeys));
        }

        // Await processing of node batches
        for (Future<Void> future : futures) {
            Pools.force(future);
        }
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.refactor.deleteAndReconnect([pathLinkedList], [nodesToRemove], {config}) - Removes some nodes from a linked list")
    public Stream<GraphResult> deleteAndReconnect(@Name("path") Path path, @Name("nodes") List<Node> nodesToRemove, @Name(value = "config", defaultValue = "{}") Map<String,Object> config) {

        RefactorConfig refactorConfig = new RefactorConfig(config);

        List<Node> nodes = IterableUtils.toList(path.nodes());
        Set<Relationship> rels = Iterables.asSet(path.relationships());

        if (!nodes.containsAll(nodesToRemove)) {
            return Stream.empty();
        }

        BiFunction<Node, Direction, Relationship> filterRel = (node, direction) -> StreamSupport
                .stream(node.getRelationships(direction).spliterator(), false)
                .filter(rels::contains)
                .findFirst()
                .orElse(null);

        nodesToRemove.forEach(node -> {

            Relationship relationshipIn = filterRel.apply(node, Direction.INCOMING);
            Relationship relationshipOut = filterRel.apply(node, Direction.OUTGOING);

            // if initial or terminal node
            if (relationshipIn == null || relationshipOut == null) {
                rels.remove(relationshipIn == null ? relationshipOut : relationshipIn);

            } else {
                Node nodeIncoming = relationshipIn.getStartNode();
                Node nodeOutgoing = relationshipOut.getEndNode();

                RelationshipType newRelType;
                Map<String, Object> newRelProps = new HashMap<>();

                final RefactorConfig.RelationshipSelectionStrategy strategy = refactorConfig.getRelationshipSelectionStrategy();
                switch (strategy) {
                    case INCOMING:
                        newRelType = relationshipIn.getType();
                        newRelProps.putAll(relationshipIn.getAllProperties());
                        break;

                    case OUTGOING:
                        newRelType = relationshipOut.getType();
                        newRelProps.putAll(relationshipOut.getAllProperties());
                        break;

                    default:
                        newRelType = RelationshipType.withName(relationshipIn.getType() + "_" + relationshipOut.getType());
                        newRelProps.putAll(relationshipIn.getAllProperties());
                }

                Relationship relCreated = nodeIncoming.createRelationshipTo(nodeOutgoing, newRelType);
                newRelProps.forEach(relCreated::setProperty);

                if (strategy == MERGE) {
                    mergeProperties(relationshipOut.getAllProperties(), relCreated, refactorConfig);
                }

                rels.add(relCreated);
                rels.removeAll(List.of(relationshipIn, relationshipOut));
            }

            tx.execute("WITH $node as n DETACH DELETE n", Map.of("node", node));
            nodes.remove(node);
        });

        return Stream.of(new GraphResult(nodes, List.copyOf(rels)));
    }

    private boolean isUniqueConstraintDefinedFor(String label, String key) {
        return StreamSupport.stream(tx.schema().getConstraints(Label.label(label)).spliterator(), false)
                .anyMatch(c ->  {
                    if (!c.isConstraintType(ConstraintType.UNIQUENESS)) {
                        return false;
                    }
                    return StreamSupport.stream(c.getPropertyKeys().spliterator(), false)
                            .allMatch(k -> k.equals(key));
                });
    }

    private Future<Void> categorizeNodes(List<Node> batch, String sourceKey, String relationshipType, Boolean outgoing, String label, String targetKey, List<String> copiedKeys) {

        return pools.processBatch(batch, db, (innerTx, node) -> {
            node = Util.rebind(innerTx, node);
            Object value = node.getProperty(sourceKey, null);
            if (value != null) {
                String q =
                        "WITH $node AS n " +
                                "MERGE (cat:`" + label + "` {`" + targetKey + "`: $value}) " +
                                (outgoing ? "MERGE (n)-[:`" + relationshipType + "`]->(cat) "
                                        : "MERGE (n)<-[:`" + relationshipType + "`]-(cat) ") +
                                "RETURN cat";
                Map<String, Object> params = new HashMap<>(2);
                params.put("node", node);
                params.put("value", value);
                Result result = innerTx.execute(q, params);
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
            if (conf.getMergeRelsAllowed()) {
                if(!conf.hasProperties()) {
                    Map<String, Object> map = Collections.singletonMap("properties", "combine");
                    conf = new RefactorConfig(map);
                }
                mergeRelsWithSameTypeAndDirectionInMergeNodes(target, conf, Direction.OUTGOING);
                mergeRelsWithSameTypeAndDirectionInMergeNodes(target, conf, Direction.INCOMING);
            }
            if (delete) source.delete();
            PropertiesManager.mergeProperties(properties, target, conf);
        } catch (NotFoundException e) {
            log.warn("skipping a node for merging: " + e.getCause().getMessage());
        }
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
