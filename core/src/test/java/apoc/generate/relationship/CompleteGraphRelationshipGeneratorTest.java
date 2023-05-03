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
package apoc.generate.relationship;

import apoc.generate.config.NumberOfNodesBasedConfig;
import org.junit.Test;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CompleteGraphRelationshipGeneratorTest {

    @Test
    public void testCompleteGraphGenerator() {
        int numberOfNodes = 6;

        NumberOfNodesBasedConfig num = new NumberOfNodesBasedConfig(numberOfNodes);
        CompleteGraphRelationshipGenerator cg = new CompleteGraphRelationshipGenerator(num);

        List<Pair<Integer, Integer>> edges = cg.doGenerateEdges();

        assertIsComplete(edges, numberOfNodes);
    }

    private void assertIsComplete(List<Pair<Integer, Integer>> edges, int numberOfNodes) {
        assertEquals(edges.size(), (int) (.5 * numberOfNodes * (numberOfNodes - 1)));

        for (Integer i = 0; i < numberOfNodes; i++) {
            for (Integer j = i + 1; j < numberOfNodes; j++) {
                assertTrue(edges.contains(Pair.of(i, j)));
            }
        }
    }
}
