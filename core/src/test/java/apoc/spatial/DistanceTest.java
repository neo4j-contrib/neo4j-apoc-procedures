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
package apoc.spatial;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import apoc.result.DistancePathResult;
import apoc.util.TestUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class DistanceTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static final String LABEL = "Point";
    private static final String LAT = "latitude";
    private static final String LONG = "longitude";
    private static final String NAME = "name";
    private static final String RELATES = "RELATES";

    @BeforeClass
    public static void setup() {
        TestUtil.registerProcedure(db, Distance.class);
    }

    /* This test fails due to a bug in neo with Path objects returned in scala
    @Test
    public void testCalculateShortestDistanceAndReturnPath() {
        createPoints();
        int i = 0;
        testCall(db, "MATCH (a:Point {name: \"bruges\"}), (b:Point {name: \"dresden\"}) " +
                "MATCH p=(a)-[*]->(b) " +
                "WITH collect(p) as paths " +
                "CALL apoc.spatial.shortestDistancePath(paths) YIELD path RETURN path", (row) -> {
            PathImpl path = (PathImpl) row.get("path");
            List<Node> nodes = new ArrayList<>();
            for (Node node : path.nodes()) {
                nodes.add(node);
            }
            assertEquals(3, nodes.size());
            assertEquals("brussels", nodes.get(1).getProperty(NAME));

        });
    }
    */

    @AfterClass
    public static void teardown() {
        db.shutdown();
    }

    @Test
    public void testSortingPathsOnDistance() {
        Distance distanceProc = new Distance();
        distanceProc.db = db;
        createPoints();
        int i = 0;
        try (Transaction tx = db.beginTx()) {
            List<Path> paths = new ArrayList<>();
            Result result = tx.execute(
                    "MATCH (a:Point {name:'bruges'}), (b:Point {name:'dresden'}) " + "MATCH p=(a)-[*]->(b) RETURN p");
            while (result.hasNext()) {
                Map<String, Object> record = result.next();
                ++i;
                paths.add((Path) record.get("p"));
            }
            int z = 0;
            SortedSet<DistancePathResult> sorted = distanceProc.sortPaths(paths);
            double lastDistance = 0.0;
            for (DistancePathResult distancePathResult : sorted) {
                ++z;
                assertTrue(distancePathResult.distance > lastDistance);
                lastDistance = distancePathResult.distance;
            }
            assertEquals(3, z);
            tx.commit();
        }
        assertEquals(3, i);
    }

    private void createPoints() {
        try (Transaction tx = db.beginTx()) {
            Node bruges = tx.createNode(Label.label(LABEL));
            bruges.setProperty(NAME, "bruges");
            bruges.setProperty(LAT, 51.2605829);
            bruges.setProperty(LONG, 3.0817189);

            Node brussels = tx.createNode(Label.label(LABEL));
            brussels.setProperty(NAME, "brussels");
            brussels.setProperty(LAT, 50.854954);
            brussels.setProperty(LONG, 4.3051786);

            Node paris = tx.createNode(Label.label(LABEL));
            paris.setProperty(NAME, "paris");
            paris.setProperty(LAT, 48.8588376);
            paris.setProperty(LONG, 2.2773455);

            Node dresden = tx.createNode(Label.label(LABEL));
            dresden.setProperty(NAME, "dresden");
            dresden.setProperty(LAT, 51.0767496);
            dresden.setProperty(LONG, 13.6321595);

            bruges.createRelationshipTo(brussels, RelationshipType.withName(RELATES));
            brussels.createRelationshipTo(dresden, RelationshipType.withName(RELATES));
            brussels.createRelationshipTo(paris, RelationshipType.withName(RELATES));
            bruges.createRelationshipTo(paris, RelationshipType.withName(RELATES));
            paris.createRelationshipTo(dresden, RelationshipType.withName(RELATES));

            tx.commit();
        }
    }
}
