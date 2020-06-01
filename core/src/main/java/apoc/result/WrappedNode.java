package apoc.result;

import org.neo4j.graphdb.*;
import org.neo4j.internal.helpers.collection.FilteringIterable;
import org.neo4j.internal.helpers.collection.Iterables;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Arrays.asList;

/**
 * @author mh
 * @since 16.03.16
 */
public class WrappedNode implements Node {
    private static AtomicLong MIN_ID = new AtomicLong(-1);
    private final List<Label> labels = new ArrayList<Label>();
    private final Map<String, Object> props = new HashMap<>();
    private final List<Relationship> rels = new ArrayList<>();
    private final GraphDatabaseService db;
    private final long id;

    public WrappedNode(Label[] labels, Map<String, Object> props, GraphDatabaseService db) {
        this.id = MIN_ID.getAndDecrement();
        this.db = db;
        this.labels.addAll(asList(labels));
        this.props.putAll(props);
    }

    public WrappedNode(long nodeId, GraphDatabaseService db) {
        this.id = nodeId;
        this.db = db;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public void delete() {
        for (Relationship rel : rels) {
            rel.delete();
        }
    }

    @Override
    public Iterable<Relationship> getRelationships() {
        return rels;
    }

    @Override
    public boolean hasRelationship() {
        return !rels.isEmpty();
    }

    @Override
    public Iterable<Relationship> getRelationships(RelationshipType... relationshipTypes) {
        return new FilteringIterable<>(rels, (r) -> isType(r, relationshipTypes));
    }

    private boolean isType(Relationship r, RelationshipType... relationshipTypes) {
        for (RelationshipType type : relationshipTypes) {
            if (r.isType(type)) return true;
        }
        return false;
    }

    @Override
    public Iterable<Relationship> getRelationships(Direction direction, RelationshipType... relationshipTypes) {
        return new FilteringIterable<>(rels, (r) -> isType(r, relationshipTypes) && isDirection(r, direction));
    }

    private boolean isDirection(Relationship r, Direction direction) {
        return direction == Direction.BOTH || direction == Direction.OUTGOING && r.getStartNode().equals(this) || direction == Direction.INCOMING && r.getEndNode().equals(this);
    }

    @Override
    public boolean hasRelationship(RelationshipType... relationshipTypes) {
        return getRelationships(relationshipTypes).iterator().hasNext();
    }

    @Override
    public boolean hasRelationship(Direction direction, RelationshipType... relationshipTypes) {
        return getRelationships(direction, relationshipTypes).iterator().hasNext();
    }

    @Override
    public Iterable<Relationship> getRelationships(Direction direction) {
        return new FilteringIterable<>(rels, (r) -> isDirection(r, direction));
    }

    @Override
    public boolean hasRelationship(Direction direction) {
        return getRelationships(direction).iterator().hasNext();
    }

    @Override
    public Relationship getSingleRelationship(RelationshipType relationshipType, Direction direction) {
        return null;
    }

    @Override
    public Relationship createRelationshipTo(Node node, RelationshipType relationshipType) {
        Relationship rel = new VirtualRelationship(this,node,relationshipType);

        return null;
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes() {
        return null;
    }

    @Override
    public int getDegree() {
        return (int) rels.size();
    }

    @Override
    public int getDegree(RelationshipType relationshipType) {
        return (int) Iterables.count(getRelationships(relationshipType));
    }

    @Override
    public int getDegree(Direction direction) {
        return (int) Iterables.count(getRelationships(direction));
    }

    @Override
    public int getDegree(RelationshipType relationshipType, Direction direction) {
        return (int) Iterables.count(getRelationships(direction, relationshipType));
    }

    @Override
    public void addLabel(Label label) {
        labels.add(label);
    }

    @Override
    public void removeLabel(Label label) {
        for (Iterator<Label> iterator = labels.iterator(); iterator.hasNext(); ) {
            Label next = iterator.next();
            if (next.name().equals(label.name())) iterator.remove();
        }
    }

    @Override
    public boolean hasLabel(Label label) {
        for (Label l : labels) {
            if (l.name().equals(label.name())) return true;
        }
        return false;
    }

    @Override
    public Iterable<Label> getLabels() {
        return labels;
    }

    @Override
    public boolean hasProperty(String s) {
        return props.containsKey(s);
    }

    @Override
    public Object getProperty(String s) {
        return props.get(s);
    }

    @Override
    public Object getProperty(String s, Object o) {
        Object value = props.get(s);
        return value == null ? o : value;
    }

    @Override
    public void setProperty(String s, Object o) {
        props.put(s,o);
    }

    @Override
    public Object removeProperty(String s) {
        return props.remove(s);
    }

    @Override
    public Iterable<String> getPropertyKeys() {
        return props.keySet();
    }

    @Override
    public Map<String, Object> getProperties(String... strings) {
        HashMap<String, Object> res = new HashMap<>(props);
        res.entrySet().retainAll(asList(strings));
        return res;
    }

    @Override
    public Map<String, Object> getAllProperties() {
        return props;
    }

    void delete(Relationship rel) {
        rels.remove(rel);
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof Node && id == ((Node) o).getId();

    }

    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }
}
