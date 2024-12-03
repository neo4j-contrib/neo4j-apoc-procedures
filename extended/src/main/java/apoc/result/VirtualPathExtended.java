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

import apoc.util.CollectionUtilsExtended;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Paths;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class VirtualPathExtended implements Path {

    private final Node start;
    private final List<Relationship> relationships;

    public VirtualPathExtended(Node start) {
        this(start, new ArrayList<>());
    }

    private VirtualPathExtended(Node start, List<Relationship> relationships) {
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
        return () -> new ReverseIterator<>(relationships);
    }

    @Override
    public Iterable<Node> nodes() {
        return nodeList();
    }

    @Override
    public Iterable<Node> reverseNodes() {
        return () -> new ReverseIterator<>(nodeList());
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
                } finally {
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
        final List<Node> previousNodes = getPreviousNodes();
        boolean isRelConnectedToPrevious = CollectionUtilsExtended.containsAny(previousNodes, relationship.getNodes());
        if (!isRelConnectedToPrevious) {
            throw new IllegalArgumentException("Relationship is not part of current path.");
        }
    }

    private List<Node> nodeList() {
        List<Node> nodes = new ArrayList<>();
        nodes.add(start);

        AtomicReference<Node> currNode = new AtomicReference<>(start);
        final List<Node> otherNodes = relationships.stream()
                .map(rel -> {
                    final Node otherNode = rel.getOtherNode(currNode.get());
                    currNode.set(otherNode);
                    return otherNode;
                })
                .toList();

        nodes.addAll(otherNodes);
        return nodes;
    }

    private List<Node> getPreviousNodes() {
        Relationship previousRelationship = lastRelationship();
        if (previousRelationship != null) {
            return Arrays.asList(previousRelationship.getNodes());
        }
        return List.of(endNode());
    }

    private static class ReverseIterator<T> implements Iterator<T> {
        private final List<T> list;
        private int index;

        private ReverseIterator(List<T> list) {
            this.list = list;
            index = list.size() - 1;
        }

        @Override
        public boolean hasNext() {
            return index >= 0;
        }

        @Override
        public T next() {
            return list.get(index--);
        }
    }
}
