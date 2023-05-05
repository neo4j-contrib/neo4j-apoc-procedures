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
package apoc.generate;

import apoc.generate.config.GeneratorConfiguration;
import apoc.generate.relationship.RelationshipGenerator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link GraphGenerator} for Neo4j.
 */
public class Neo4jGraphGenerator extends BaseGraphGenerator {

    private final GraphDatabaseService database;

    public Neo4jGraphGenerator(GraphDatabaseService database) {
        this.database = database;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<Long> generateNodes(final GeneratorConfiguration config) {
        final List<Long> nodes = new ArrayList<>();

        int numberOfNodes = config.getNumberOfNodes();

        Transaction tx = database.beginTx();
        try {
            for (int i = 1; i <= numberOfNodes; i++) {
                nodes.add(config.getNodeCreator().createNode(tx).getId());

                if (i % config.getBatchSize() == 0) {
                    tx.commit();
                    tx.close();
                    tx = database.beginTx();
                }
            }
            tx.commit();
        } finally {
            tx.close();
        }

        return nodes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void generateRelationships(final GeneratorConfiguration config, final List<Long> nodes) {
        RelationshipGenerator relationshipGenerator = config.getRelationshipGenerator();
        List<Pair<Integer, Integer>> relationships = relationshipGenerator.generateEdges();

        Transaction tx = database.beginTx();
        try {
            int i = 0;
            for (Pair<Integer, Integer> input : relationships) {
                Node first = tx.getNodeById(nodes.get(input.first()));
                Node second = tx.getNodeById(nodes.get(input.other()));
                config.getRelationshipCreator().createRelationship(first, second);

                if (++i % config.getBatchSize() == 0) {
                    tx.commit();
                    tx.close();
                    tx = database.beginTx();
                }
            }

            tx.commit();
        } finally {
            tx.close();
        }
    }
}
