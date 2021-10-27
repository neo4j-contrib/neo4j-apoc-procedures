package apoc.result;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Paths;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.commons.collections4.IterableUtils.reversedIterable;


public class VirtualPath implements Path {

    private final Node start;
    private final List<Relationship> relationships;

    public VirtualPath(Node start) {
        this(start, new ArrayList<>());
    }

    private VirtualPath(Node start, List<Relationship> relationships) {
        Objects.requireNonNull(start);
        Objects.requireNonNull(relationships);
        this.start = start;
        this.relationships = relationships;
    }

    public void addRel(Relationship relationship) {
        Objects.requireNonNull(relationship);
        requireConnected(relationship);
        this.relationships.add(relationship);
    }

    @Override
    public Node startNode() {
        return start;
    }

    @Override
    public Node endNode() {
        return reverseNodes().iterator().next();
    }

    @Override
    public Relationship lastRelationship() {
        return relationships.isEmpty() ? null : relationships.get(relationships.size() - 1);
    }

    @Override
    public Iterable<Relationship> relationships() {
        return relationships;
    }

    @Override
    public Iterable<Relationship> reverseRelationships() {
        return reversedIterable(relationships());
    }

    @Override
    public Iterable<Node> nodes() {
        List<Node> nodes = new ArrayList<>();
        nodes.add(start);

        AtomicReference<Node> currNode = new AtomicReference<>(start);
        final List<Node> otherNodes = relationships.stream().map(rel -> {
            final Node otherNode = rel.getOtherNode(currNode.get());
            currNode.set(otherNode);
            return otherNode;
        }).collect(Collectors.toList());

        nodes.addAll(otherNodes);
        return nodes;
    }

    @Override
    public Iterable<Node> reverseNodes() {
        return reversedIterable(nodes());
    }

    @Override
    public int length() {
        return relationships.size();
    }

    @Override
    @Nonnull
    public Iterator<Entity> iterator() {
        return new Iterator<>() {
            Iterator<? extends Entity> current = nodes().iterator();
            Iterator<? extends Entity> next = relationships().iterator();

            @Override
            public boolean hasNext() {
                return current.hasNext();
            }

            @Override
            public Entity next() {
                try {
                    return current.next();
                }
                finally {
                    Iterator<? extends Entity> temp = current;
                    current = next;
                    next = temp;
                }
            }

            @Override
            public void remove() {
                next.remove();
            }
        };
    }

    @Override
    public String toString() {
        return Paths.defaultPathToString(this);
    }

    private void requireConnected(Relationship relationship) {
        Node previousStartNode;
        Node previousEndNode;
        Relationship previousRelationship = lastRelationship();
        if (previousRelationship != null) {
            previousStartNode = previousRelationship.getStartNode();
            previousEndNode = previousRelationship.getEndNode();
        } else {
            previousStartNode = startNode();
            previousEndNode = endNode();
        }
        if (relationship.getStartNode() != previousEndNode
                || relationship.getStartNode() != previousStartNode
                || relationship.getEndNode() != previousEndNode
                || relationship.getEndNode() != previousStartNode) {
            throw new IllegalArgumentException("Relationship is not part of current path.");
        }
    }
}
