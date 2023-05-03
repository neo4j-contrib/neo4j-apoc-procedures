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

import apoc.generate.config.ErdosRenyiConfig;
import apoc.generate.relationship.ErdosRenyiRelationshipGenerator;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

/**
 * Large integration test for {@link Neo4jGraphGenerator} with
 * {@link ErdosRenyiRelationshipGenerator}.
 */
public class ErdosRenyiGeneratorLargeTest {


    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Test(timeout = 60 * 1000)
    @Ignore("very long running test")
    public void shouldGenerateRelationshipsForLargeGraphInAReasonableAmountOfTime() {
        new ErdosRenyiRelationshipGenerator(new ErdosRenyiConfig(500_000, 10_000_000)).generateEdges();
    }

    @Test(timeout = 60 * 1000)
    @Ignore("very long running test")
    public void shouldGenerateRelationshipsForLargeGraphInAReasonableAmountOfTime2() {
        new ErdosRenyiRelationshipGenerator(new ErdosRenyiConfig(10000, 25_000_000)).generateEdges();
    }

}
