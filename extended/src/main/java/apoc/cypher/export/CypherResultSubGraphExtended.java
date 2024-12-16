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
package apoc.cypher.export;

import apoc.util.Util;
import apoc.util.collection.Iterables;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.graphdb.security.AuthorizationViolationException;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static apoc.util.Util.INVALID_QUERY_MODE_ERROR;

public class CypherResultSubGraphExtended implements SubGraph {

    private final SortedMap<String, Node> nodes = new TreeMap<>();
    private final SortedMap<String, Relationship> relationships = new TreeMap<>();
    private final Collection<Label> labels = new HashSet<>();
    private final Collection<RelationshipType> types = new HashSet<>();
    private final Collection<IndexDefinition> indexes = new HashSet<>();
    private final Collection<ConstraintDefinition> constraints = new HashSet<>();

    public void add(Node node) {
        final String id = node.getElementId();
        if (!nodes.containsKey(id)) {
            addNode(id, node);
        }
    }

    void addNode(String id, Node data) {
        nodes.put(id, data);
        labels.addAll(Iterables.asList(data.getLabels()));
    }

    public void add(Relationship rel, boolean addNodes) {
        final String id = rel.getElementId();
        if (!relationships.containsKey(id)) {
            addRel(id, rel);
            // start and end nodes will be added only with the `apoc.meta.*` procedures,
            // not with the `apoc.export.*.query` ones
            if (addNodes) {
                add(rel.getStartNode());
                add(rel.getEndNode());
            }
        }
    }

    public static SubGraph from(Transaction tx, Result result, boolean addBetween) {
        return from(tx, result, addBetween, true);
    }

    public static SubGraph from(Transaction tx, Result result, boolean addBetween, boolean addRelNodes) {
        final CypherResultSubGraphExtended graph = new CypherResultSubGraphExtended();
        final List<String> columns = result.columns();
        try {
            result.forEachRemaining(row -> {
                for (String column : columns) {
                    final Object value = row.get(column);
                    graph.addToGraph(value, addRelNodes);
                }
            });
        } catch (AuthorizationViolationException e) {
            throw new RuntimeException(INVALID_QUERY_MODE_ERROR);
        }
        for (IndexDefinition def : Util.getIndexes(tx)) {
            if (def.getIndexType() != IndexType.LOOKUP) {
                if (def.isNodeIndex()) {
                    for (Label label : def.getLabels()) {
                        if (graph.getLabels().contains(label)) {
                            graph.addIndex(def);
                            break;
                        }
                    }
                } else {
                    for (RelationshipType type : def.getRelationshipTypes()) {
                        if (graph.getTypes().contains(type)) {
                            graph.addIndex(def);
                            break;
                        }
                    }
                }
            }
        }
        for (ConstraintDefinition def : tx.schema().getConstraints()) {
            if (Util.isNodeCategory(def.getConstraintType())
                    && graph.getLabels().contains(def.getLabel())) {
                graph.addConstraint(def);
            } else if (Util.isRelationshipCategory(def.getConstraintType())
                    && graph.getTypes().contains(def.getRelationshipType())) {
                graph.addConstraint(def);
            }
        }

        if (addBetween) {
            graph.addRelationshipsBetweenNodes();
        }
        return graph;
    }

    private void addIndex(IndexDefinition def) {
        indexes.add(def);
    }

    private void addConstraint(ConstraintDefinition def) {
        constraints.add(def);
    }

    private void addRelationshipsBetweenNodes() {
        Set<Node> newNodes = new HashSet<>();
        for (Node node : nodes.values()) {
            for (Relationship relationship : node.getRelationships()) {
                if (!relationships.containsKey(relationship.getElementId())) {
                    continue;
                }

                final Node other = relationship.getOtherNode(node);
                if (nodes.containsKey(other.getElementId()) || newNodes.contains(other)) {
                    continue;
                }
                newNodes.add(other);
            }
        }
        for (Node node : newNodes) {
            add(node);
        }
    }

    private void addToGraph(Object value, boolean addRelNodes) {
        if (value instanceof Node) {
            add((Node) value);
        }
        if (value instanceof Relationship) {
            add((Relationship) value, addRelNodes);
        }
        if (value instanceof Iterable) {
            for (Object inner : (Iterable) value) {
                addToGraph(inner, addRelNodes);
            }
        }
    }

    @Override
    public Iterable<Node> getNodes() {
        return nodes.values();
    }

    @Override
    public Iterable<Relationship> getRelationships() {
        return relationships.values();
    }

    public Collection<Label> getLabels() {
        return labels;
    }

    public Collection<RelationshipType> getTypes() {
        return types;
    }

    void addRel(String id, Relationship rel) {
        relationships.put(id, rel);
        types.add(rel.getType());
    }

    @Override
    public Iterable<IndexDefinition> getIndexes() {
        return indexes;
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints() {
        return constraints;
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints(Label label) {
        return constraints.stream()
                .filter(c -> Util.isNodeCategory(c.getConstraintType()))
                .filter(c -> c.getLabel().equals(label))
                .collect(Collectors.toSet());
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints(RelationshipType type) {
        return constraints.stream()
                .filter(c -> Util.isRelationshipCategory(c.getConstraintType()))
                .filter(c -> c.getRelationshipType().equals(type))
                .collect(Collectors.toSet());
    }

    @Override
    public Iterable<IndexDefinition> getIndexes(Label label) {
        return indexes.stream()
                .filter(IndexDefinition::isNodeIndex)
                .filter(idx -> StreamSupport.stream(idx.getLabels().spliterator(), false)
                        .anyMatch(lb -> lb.equals(label)))
                .collect(Collectors.toSet());
    }

    @Override
    public Iterable<IndexDefinition> getIndexes(RelationshipType type) {
        return indexes.stream()
                .filter(IndexDefinition::isRelationshipIndex)
                .filter(idx -> StreamSupport.stream(idx.getRelationshipTypes().spliterator(), false)
                        .anyMatch(relType -> relType.name().equals(type.name())))
                .collect(Collectors.toSet());
    }

    @Override
    public Iterable<RelationshipType> getAllRelationshipTypesInUse() {
        return Collections.unmodifiableCollection(types);
    }

    @Override
    public Iterable<Label> getAllLabelsInUse() {
        return Collections.unmodifiableCollection(labels);
    }

    @Override
    public long countsForRelationship(Label start, RelationshipType type, Label end) {
        return relationships.values().stream()
                .filter(r -> {
                    boolean matchType = r.getType().equals(type);
                    boolean matchStart = start == null || r.getStartNode().hasLabel(start);
                    boolean matchEnd = end == null || r.getEndNode().hasLabel(end);
                    return matchType && matchStart && matchEnd;
                })
                .count();
    }

    @Override
    public long countsForNode(Label label) {
        return nodes.values().stream().filter(n -> n.hasLabel(label)).count();
    }

    @Override
    public Iterator<Node> findNodes(Label label) {
        return nodes.values().stream().filter(n -> n.hasLabel(label)).iterator();
    }
}
