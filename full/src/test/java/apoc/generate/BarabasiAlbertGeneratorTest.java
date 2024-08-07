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

import apoc.generate.config.BarabasiAlbertConfig;
import apoc.generate.config.BasicGeneratorConfig;
import apoc.generate.node.SocialNetworkNodeCreator;
import apoc.generate.relationship.BarabasiAlbertRelationshipGenerator;
import apoc.generate.relationship.SocialNetworkRelationshipCreator;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

/**
 * Integration test for {@link Neo4jGraphGenerator} with {@link BarabasiAlbertRelationshipGenerator}.
 */
public class BarabasiAlbertGeneratorTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @After
    public void teardown() {
        db.shutdown();
    }

    @Test
    public void shouldGeneratePowerLawDistribution() {
        new Neo4jGraphGenerator(db)
                .generateGraph(new BasicGeneratorConfig(
                        new BarabasiAlbertRelationshipGenerator(new BarabasiAlbertConfig(100, 2)),
                        new SocialNetworkNodeCreator(),
                        new SocialNetworkRelationshipCreator()));

        List<Integer> degrees = new LinkedList<>();

        try (Transaction tx = db.beginTx()) {
            for (Node node : tx.getAllNodes()) {
                degrees.add(node.getDegree());
            }
            tx.commit();
        }

        Collections.sort(degrees, Collections.reverseOrder());

        // TODO make this an automated test
        // System.out.println(ArrayUtils.toString(degrees.toArray(new Integer[degrees.size()])));
    }

    @Test(timeout = 10 * 1000)
    @Ignore
    public void shouldGenerateRelationshipsForLargeGraphInAReasonableAmountOfTime() {
        new BarabasiAlbertRelationshipGenerator(new BarabasiAlbertConfig(1_000_000, 3)).generateEdges();
    }
}
