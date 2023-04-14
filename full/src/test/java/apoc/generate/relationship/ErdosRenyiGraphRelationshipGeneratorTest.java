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

import apoc.generate.Generate;
import apoc.generate.config.ErdosRenyiConfig;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;

import static org.junit.Assert.assertEquals;


public class ErdosRenyiGraphRelationshipGeneratorTest  {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, Generate.class);
    }

    @Test
    public void testErdosRenyiGeneratorValidity() {
        doGenerateEdges(20, 190); // Uses simple generator
        doGenerateEdges(10, 15); // Uses index <-> edge mapping
    }

    private void doGenerateEdges(int numberOfNodes, int numberOfEdges) {
        ErdosRenyiConfig config = new ErdosRenyiConfig(numberOfNodes, numberOfEdges);
        ErdosRenyiRelationshipGenerator er = new ErdosRenyiRelationshipGenerator(config);
        List<Pair<Integer, Integer>> edges = er.doGenerateEdges(); // Integer may not be enough here!

        assertCorrectNumberOfEdgesGenerated(numberOfEdges, edges);
    }

    /**
     * Checks the length of edgeList and compares to the expected number of edges to be generated
     *
     * @param numberOfEdges number of edges in the graph
     * @param edges list of edges as SameTypePair<Integer>
     */
    private void assertCorrectNumberOfEdgesGenerated(long numberOfEdges, List<Pair<Integer, Integer>> edges) {
        assertEquals(numberOfEdges, edges.size());
    }

}
