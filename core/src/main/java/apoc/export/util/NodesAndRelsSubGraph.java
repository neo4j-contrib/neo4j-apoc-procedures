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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.helpers.collection.Iterables;

/**
 * @author mh
 * @since 22.05.16
 */
public class NodesAndRelsSubGraph implements SubGraph {
    private final Collection<Node> nodes;
    private final Collection<Relationship> rels;
    private final Transaction tx;
    private final Set<String> labels = new HashSet<>(20);
    private final Set<String> types = new HashSet<>(20);

    public NodesAndRelsSubGraph(Transaction tx, Collection<Node> nodes, Collection<Relationship> rels) {
        this.tx = tx;
        this.nodes = new ArrayList<>(nodes.size());
        for (Node node : nodes) {
            for (Label label : node.getLabels()) labels.add(label.name());
            this.nodes.add(node);
        }
        this.rels = new HashSet<>(rels);
        for (Relationship rel : rels) {
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
    public boolean contains(Relationship relationship) {
        return rels.contains(relationship);
    }

    @Override
    public Iterable<IndexDefinition> getIndexes() {
        return getDefinitions(Schema::getIndexes);
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints() {
        Comparator<ConstraintDefinition> comp = Comparator.comparing(ConstraintDefinition::getName);
        ArrayList<ConstraintDefinition> definitions = getDefinitions(Schema::getConstraints);
        definitions.sort(comp);
        return definitions;
    }

    private <T> ArrayList<T> getDefinitions(BiFunction<Schema, Label, Iterable<T>> biFunction) {
        Schema schema = tx.schema();
        ArrayList<T> definitions = new ArrayList<>(labels.size() * 2);
        for (String label : labels) {
            Iterables.addAll(definitions, biFunction.apply(schema, Label.label(label)));
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
        return tx.schema().getIndexes(label);
    }

    @Override
    public Iterable<IndexDefinition> getIndexes(RelationshipType type) {
        if (!types.contains(type.name())) {
            return Collections.emptyList();
        }
        return tx.schema().getIndexes(type);
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
