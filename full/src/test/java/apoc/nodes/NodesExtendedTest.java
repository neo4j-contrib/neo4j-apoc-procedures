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
package apoc.nodes;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

import apoc.create.Create;
import apoc.meta.Meta;
import apoc.util.TestUtil;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class NodesExtendedTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.procedure_unrestricted, singletonList("apoc.*"));

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, NodesExtended.class, Create.class, Meta.class);
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    @Test
    public void rebind() {
        TestUtil.testCall(
                db,
                "CREATE (a:Foo)-[r1:MY_REL]->(b:Bar)-[r2:ANOTHER_REL]->(c:Baz) WITH a,b,c,r1,r2 \n"
                        + "RETURN apoc.any.rebind({first: a, second: b, third: c, rels: [r1, r2]}) as rebind",
                (row) -> {
                    final Map<String, Object> rebind = (Map<String, Object>) row.get("rebind");
                    final List<Relationship> rels = (List<Relationship>) rebind.get("rels");
                    final Relationship firstRel = rels.get(0);
                    final Relationship secondRel = rels.get(1);
                    assertEquals(firstRel.getStartNode(), rebind.get("first"));
                    assertEquals(firstRel.getEndNode(), rebind.get("second"));
                    assertEquals(secondRel.getStartNode(), rebind.get("second"));
                    assertEquals(secondRel.getEndNode(), rebind.get("third"));
                });

        TestUtil.testCall(
                db,
                "CREATE p1=(a:Foo)-[r1:MY_REL]->(b:Bar), p2=(:Bar)-[r2:ANOTHER_REL]->(c:Baz) \n"
                        + "RETURN apoc.any.rebind([p1, p2]) as rebind",
                (row) -> {
                    final List<Path> rebindList = (List<Path>) row.get("rebind");
                    assertEquals(2, rebindList.size());
                    final Path firstPath = rebindList.get(0);
                    assertFooBarPath(firstPath);
                    final Path secondPath = rebindList.get(1);
                    assertPath(secondPath, List.of("Bar", "Baz"), List.of("ANOTHER_REL"));
                });

        // check via `apoc.meta.cypher.type()` that, even if the return type is Object,
        // the output of a rebound Path is also a Path (i.e.: `PATH NOT NULL`)
        TestUtil.testCall(
                db,
                "CREATE path=(a:Foo)-[r1:MY_REL]->(b:Bar)\n" + "WITH apoc.any.rebind(path) AS rebind\n"
                        + "RETURN rebind, apoc.meta.cypher.type(rebind) as valueType",
                (row) -> {
                    final String valueType = (String) row.get("valueType");
                    assertEquals("PATH", valueType);

                    final Path pathRebind = (Path) row.get("rebind");
                    assertFooBarPath(pathRebind);
                });
    }

    private void assertFooBarPath(Path pathRebind) {
        assertPath(pathRebind, List.of("Foo", "Bar"), List.of("MY_REL"));
    }

    private void assertPath(Path rebind, List<String> labels, List<String> relTypes) {
        final List<String> actualLabels = Iterables.stream(rebind.nodes())
                .map(i -> i.getLabels().iterator().next())
                .map(Label::name)
                .collect(Collectors.toList());
        assertEquals(labels, actualLabels);
        final List<String> actualRelTypes = Iterables.stream(rebind.relationships())
                .map(Relationship::getType)
                .map(RelationshipType::name)
                .collect(Collectors.toList());
        assertEquals(relTypes, actualRelTypes);
    }
}
