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
package apoc.generate.node;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

/**
 * A {@link NodeCreator} that assigns every {@link Node} a {@link Label} passed to it in the constructor and a UUID as
 * a property.
 */
public class DefaultNodeCreator implements NodeCreator {

    private static final String UUID = "uuid";

    private final Label label;

    public DefaultNodeCreator(String label) {
        this.label = Label.label(label);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node createNode(Transaction tx) {
        Node node = tx.createNode(label);
        node.setProperty(UUID, java.util.UUID.randomUUID().toString());
        return node;
    }
}
