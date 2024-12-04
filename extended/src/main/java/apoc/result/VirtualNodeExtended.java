/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.result;

import apoc.util.UtilExtended;
import apoc.util.collection.FilteringIterableExtended;
import apoc.util.collection.IterablesExtended;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

/**
 * @author mh
 * @since 16.03.16
 */
public class VirtualNodeExtended implements Node {
    public static final String ERROR_NODE_NULL = "The inserted Node is null";

    private static AtomicLong MIN_ID = new AtomicLong(-1);
    private final Set<String> labels = new LinkedHashSet<>();
    private final Map<String, Object> props = new HashMap<>();
    private final List<Relationship> rels = new ArrayList<>();
    private final long id;
    private final String elementId;

    public VirtualNodeExtended(Label[] labels, Map<String, Object> props) {
        // to not overlap this ids with ids from VirtualNode(Node node, List<String> propertyNames)
        this.id = MIN_ID.decrementAndGet();
        addLabels(asList(labels));
        this.props.putAll(props);
        this.elementId = null;
    }

    @SuppressWarnings("unused") // used from extended
    public VirtualNodeExtended(long nodeId, Label[] labels, Map<String, Object> props) {
        this.id = nodeId;
        addLabels(asList(labels));
        this.props.putAll(props);
        this.elementId = null;
    }

    public VirtualNodeExtended(long nodeId) {
        this.id = nodeId;
        this.elementId = null;
    }

    public VirtualNodeExtended(Node node, List<String> propertyNames) {
        Objects.requireNonNull(node, ERROR_NODE_NULL);
        final long id = node.getId();
        // if node is already virtual, we return the same id
        this.id = id < 0 ? id : -id - 1;
        // to not overlap this ids with ids from VirtualNode(Label[] labels, Map<String, Object> props)
        MIN_ID.updateAndGet(x -> Math.min(x, this.id));
        this.labels.addAll(UtilExtended.labelStrings(node));
        String[] keys = propertyNames.toArray(new String[propertyNames.size()]);
        this.props.putAll(node.getProperties(keys));
        this.elementId = node.getElementId();
    }

    public static VirtualNodeExtended from(Node node) {
        return new VirtualNodeExtended(node, IterablesExtended.asList(node.getPropertyKeys()));
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public String getElementId() {
        return elementId != null ? elementId : String.valueOf(id);
    }

    @Override
    public void delete() {
        for (Relationship rel : rels) {
            rel.delete();
        }
    }

    @Override
    public ResourceIterable<Relationship> getRelationships() {
        return IterablesExtended.asResourceIterable(rels);
    }

    @Override
    public boolean hasRelationship() {
        return !rels.isEmpty();
    }

    @Override
    public ResourceIterable<Relationship> getRelationships(RelationshipType... relationshipTypes) {
        return IterablesExtended.asResourceIterable(new FilteringIterableExtended<>(rels, (r) -> isType(r, relationshipTypes)));
    }

    private boolean isType(Relationship r, RelationshipType... relationshipTypes) {
        for (RelationshipType type : relationshipTypes) {
            if (r.isType(type)) return true;
        }
        return false;
    }

    @Override
    public ResourceIterable<Relationship> getRelationships(Direction direction, RelationshipType... relationshipTypes) {
        return IterablesExtended.asResourceIterable(
                new FilteringIterableExtended<>(rels, (r) -> isType(r, relationshipTypes) && isDirection(r, direction)));
    }

    private boolean isDirection(Relationship r, Direction direction) {
        return direction == Direction.BOTH
                || direction == Direction.OUTGOING && r.getStartNode().equals(this)
                || direction == Direction.INCOMING && r.getEndNode().equals(this);
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
    public ResourceIterable<Relationship> getRelationships(Direction direction) {
        return IterablesExtended.asResourceIterable(new FilteringIterableExtended<>(rels, (r) -> isDirection(r, direction)));
    }

    @Override
    public boolean hasRelationship(Direction direction) {
        return getRelationships(direction).iterator().hasNext();
    }

    @Override
    public Relationship getSingleRelationship(RelationshipType relationshipType, Direction direction) {
        return IterablesExtended.single(getRelationships(direction, relationshipType));
    }

    @Override
    public VirtualRelationshipExtended createRelationshipTo(Node node, RelationshipType relationshipType) {
        VirtualRelationshipExtended rel = new VirtualRelationshipExtended(this, node, relationshipType);
        rels.add(rel);
        if (node
                instanceof
                VirtualNodeExtended) { // register the inverse relationship into the target virtual node only if it is not a
            // self relationship
            VirtualNodeExtended target = (VirtualNodeExtended) node;
            if (!target.rels.contains(rel)) {
                target.rels.add(rel);
            }
        }
        return rel;
    }

    public VirtualRelationshipExtended createRelationshipFrom(Node start, RelationshipType relationshipType) {
        VirtualRelationshipExtended rel = new VirtualRelationshipExtended(start, this, relationshipType);
        rels.add(rel);
        if (start
                instanceof
                VirtualNodeExtended) { // register the inverse relationship into the start virtual node only if it is not a self
            // relationship
            VirtualNodeExtended startVirtual = (VirtualNodeExtended) start;
            if (!startVirtual.rels.contains(rel)) {
                startVirtual.rels.add(rel);
            }
        }
        return rel;
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes() {
        return rels.stream().map(Relationship::getType).collect(Collectors.toList());
    }

    @Override
    public int getDegree() {
        return rels.size();
    }

    @Override
    public int getDegree(RelationshipType relationshipType) {
        return (int) IterablesExtended.count(getRelationships(relationshipType));
    }

    @Override
    public int getDegree(Direction direction) {
        return (int) IterablesExtended.count(getRelationships(direction));
    }

    @Override
    public int getDegree(RelationshipType relationshipType, Direction direction) {
        return (int) IterablesExtended.count(getRelationships(direction, relationshipType));
    }

    @Override
    public void addLabel(Label label) {
        labels.add(label.name());
    }

    public void addLabels(Iterable<Label> labels) {
        for (Label label : labels) {
            addLabel(label);
        }
    }

    @Override
    public void removeLabel(Label label) {
        labels.remove(label.name());
    }

    @Override
    public boolean hasLabel(Label label) {
        return labels.contains(label.name());
    }

    @Override
    public Iterable<Label> getLabels() {
        return labels.stream().map(Label::label).collect(Collectors.toList());
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
        props.put(s, o);
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
        res.keySet().retainAll(asList(strings));
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
        return this == o || o instanceof Node && Objects.equals(getElementId(), ((Node) o).getElementId());
    }

    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }

    @Override
    public String toString() {
        return "VirtualNode{" + "id=" + id + ", labels=" + labels + ", props=" + props + ", rels=" + rels + '}';
    }
}
