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
package apoc.export.util;

import apoc.util.Util;
import apoc.util.collection.Iterables;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.graphdb.schema.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author mh
 * @since 22.05.16
 */
public class NodesAndRelsSubGraphExtended implements SubGraph {
    private final Collection<Node> nodes;
    private final Collection<Relationship> rels;
    private final Transaction tx;
    private final Set<String> labels = new HashSet<>(20);
    private final Set<String> types = new HashSet<>(20);

    public NodesAndRelsSubGraphExtended(Transaction tx, Collection<Node> nodes, Collection<Relationship> rels) {
        this(tx, nodes, rels, false);
    }

    public NodesAndRelsSubGraphExtended(Transaction tx, Collection<Node> nodes, Collection<Relationship> rels, boolean rebind) {
        this.tx = tx;
        this.nodes = new ArrayList<>(nodes.size());
        for (Node node : nodes) {
            if (rebind) node = Util.rebind(tx, node);
            for (Label label : node.getLabels()) labels.add(label.name());
            this.nodes.add(node);
        }
        this.rels = new HashSet<>(rels.size());
        for (Relationship rel : rels) {
            if (rebind) rel = Util.rebind(tx, rel);
            this.rels.add(rel);
            this.types.add(rel.getType().name());
        }
    }

    @Override
    public Iterable<Node> getNodes() {
        return nodes;
    }

    @Override
    public Iterable<Relationship> getRelationships() {
        return rels;
    }

    @Override
    public Iterable<IndexDefinition> getIndexes() {
        return getDefinitions(
                (schema, label) -> StreamSupport.stream(schema.getIndexes(label).spliterator(), false)
                        .filter(indexDefinition -> indexDefinition.getIndexType() != IndexType.VECTOR)
                        .toList(),
                (schema, type) -> StreamSupport.stream(schema.getIndexes(type).spliterator(), false)
                        .filter(indexDefinition -> indexDefinition.getIndexType() != IndexType.VECTOR)
                        .toList());
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints() {
        Comparator<ConstraintDefinition> comp = Comparator.comparing(ConstraintDefinition::getName);
        ArrayList<ConstraintDefinition> definitions = getDefinitions(Schema::getConstraints, Schema::getConstraints);
        definitions.sort(comp);
        return definitions;
    }

    private <T> ArrayList<T> getDefinitions(
            BiFunction<Schema, Label, Iterable<T>> nodeFunction,
            BiFunction<Schema, RelationshipType, Iterable<T>> relFunction) {
        Schema schema = tx.schema();
        ArrayList<T> definitions = new ArrayList<>(labels.size() * 2);
        for (String label : labels) {
            Iterables.addAll(definitions, nodeFunction.apply(schema, Label.label(label)));
        }
        for (String type : types) {
            Iterables.addAll(definitions, relFunction.apply(schema, RelationshipType.withName(type)));
        }
        return definitions;
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints(Label label) {
        if (!labels.contains(label.name())) {
            return Collections.emptyList();
        }
        return tx.schema().getConstraints(label);
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints(RelationshipType type) {
        if (!types.contains(type.name())) {
            return Collections.emptyList();
        }
        return tx.schema().getConstraints(type);
    }

    @Override
    public Iterable<IndexDefinition> getIndexes(Label label) {
        if (!labels.contains(label.name())) {
            return Collections.emptyList();
        }
        return Util.getIndexes(tx, label);
    }

    @Override
    public Iterable<IndexDefinition> getIndexes(RelationshipType type) {
        if (!types.contains(type.name())) {
            return Collections.emptyList();
        }
        return Util.getIndexes(tx, type);
    }

    @Override
    public Iterable<RelationshipType> getAllRelationshipTypesInUse() {
        return types.stream().map(RelationshipType::withName).collect(Collectors.toSet());
    }

    @Override
    public Iterable<Label> getAllLabelsInUse() {
        return labels.stream().map(Label::label).collect(Collectors.toSet());
    }

    @Override
    public long countsForRelationship(Label start, RelationshipType type, Label end) {
        return rels.stream()
                .filter(r -> {
                    boolean matchType = r.getType().equals(type);
                    boolean matchStart = start != null ? r.getStartNode().hasLabel(start) : true;
                    boolean matchEnd = end != null ? r.getEndNode().hasLabel(end) : true;
                    return matchType && matchStart && matchEnd;
                })
                .count();
    }

    @Override
    public long countsForNode(Label label) {
        return nodes.stream().filter(n -> n.hasLabel(label)).count();
    }

    @Override
    public Iterator<Node> findNodes(Label label) {
        return nodes.stream().filter(n -> n.hasLabel(label)).iterator();
    }
}
