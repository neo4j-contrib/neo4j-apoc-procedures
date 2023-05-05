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

import apoc.generate.config.BasicGeneratorConfig;
import apoc.generate.config.ErdosRenyiConfig;
import apoc.generate.node.SocialNetworkNodeCreator;
import apoc.generate.relationship.ErdosRenyiRelationshipGenerator;
import apoc.generate.relationship.SocialNetworkRelationshipCreator;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.Parameters;

/**
 * Integration test for {@link Neo4jGraphGenerator} with
 * {@link ErdosRenyiRelationshipGenerator}.
 */
@RunWith(Parameterized.class)
public class ErdosRenyiGeneratorTest {

    @Parameters
    public static Collection<Integer[]> data() {
        return Arrays.asList(new Integer[][]{
                {100,200},
                {100,300},
                {100,1000},
                {100,5},
                {10,11},
                {10,23},
                {200,190},
        });
    }

    @Parameter(0)
    public int numberOfNodes;

    @Parameter(1)
    public int numberOfEdges;

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Test
    public void shouldGenerateCorrectNumberOfNodesAndRelationships() throws Exception {
        new Neo4jGraphGenerator(db).generateGraph(
                new BasicGeneratorConfig(
                    new ErdosRenyiRelationshipGenerator(new ErdosRenyiConfig(numberOfNodes, numberOfEdges)),
                new SocialNetworkNodeCreator(),
                new SocialNetworkRelationshipCreator()
        ));

        try (Transaction tx = db.beginTx()) {
            assertEquals(numberOfNodes, Iterables.count( tx.getAllNodes()));
            assertEquals(numberOfEdges, Iterables.count( tx.getAllRelationships()));
            tx.commit();
        }
    }

}
