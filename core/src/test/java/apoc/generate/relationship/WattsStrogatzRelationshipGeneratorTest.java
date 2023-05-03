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

import apoc.generate.config.WattsStrogatzConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WattsStrogatzRelationshipGeneratorTest {

    @Test
    public void testDoGenerateEdgesValidity() throws Exception {
        int meanDegree = 4;
        int numberOfNodes = 10;
        double betaCoefficient = 0.5;

        WattsStrogatzRelationshipGenerator generator = new WattsStrogatzRelationshipGenerator(new WattsStrogatzConfig(numberOfNodes, meanDegree, betaCoefficient));

        assertEquals((int) (meanDegree * numberOfNodes * .5), generator.doGenerateEdges().size());
    }

    @Test
    public void testDoGenerateEdgesPerformance() throws Exception {
        int meanDegree = 4;
        int numberOfNodes = 2_000;
        double betaCoefficient = 0.5;

        WattsStrogatzRelationshipGenerator generator = new WattsStrogatzRelationshipGenerator(new WattsStrogatzConfig(numberOfNodes, meanDegree, betaCoefficient));
        assertEquals((int) (meanDegree * numberOfNodes * .5), generator.doGenerateEdges().size());
    }
}