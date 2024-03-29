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
package apoc.gephi;

import static apoc.gephi.GephiMock.Node.node;
import static apoc.gephi.GephiMock.Relationship.relationship;
import static apoc.util.TestUtil.registerProcedure;
import static apoc.util.TestUtil.testCall;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;

import java.util.Set;
import org.junit.*;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

/**
 * @author mh
 * @since 29.05.16
 */
public class GephiTest {

    private static final String GEPHI_WORKSPACE = "workspace1";

    public static GephiMock gephiMock;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        gephiMock = new GephiMock();
        registerProcedure(db, Gephi.class);
        db.executeTransactionally(
                "CREATE (:Foo {name:'Foo'})-[:KNOWS{weight:7.2,foo:'foo',bar:3.0,directed:'error',label:'foo'}]->(:Bar {name:'Bar'})");
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
        gephiMock.shutdown();
    }

    @Before
    public void clearMockExpectations() {
        gephiMock.clearAllExpectations();
    }

    @Test
    public void testAdd() {
        gephiMock.mockSuccess(GEPHI_WORKSPACE, node(0, "Foo"), node(1, "Bar"), relationship(0, "KNOWS", 0, 1));
        testCall(
                db,
                "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,$workspace,p) yield nodes, relationships, format return *",
                map("workspace", GEPHI_WORKSPACE),
                r -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals("gephi", r.get("format"));
                });
    }

    @Test
    public void testWeightParameter() {
        gephiMock.mockSuccess(GEPHI_WORKSPACE, node(0, "Foo"), node(1, "Bar"), relationship(0, "KNOWS", 0, 1, "7.2"));
        testCall(
                db,
                "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,$workspace,p,'weight') yield nodes, relationships, format return *",
                map("workspace", GEPHI_WORKSPACE),
                r -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals("gephi", r.get("format"));
                });
    }

    @Test
    public void testWrongWeightParameter() {
        gephiMock.mockSuccess(GEPHI_WORKSPACE, node(0, "Foo"), node(1, "Bar"), relationship(0, "KNOWS", 0, 1));
        testCall(
                db,
                "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,$workspace,p,'test') yield nodes, relationships, format return *",
                map("workspace", GEPHI_WORKSPACE),
                r -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals("gephi", r.get("format"));
                });
    }

    @Test
    public void testRightExportParameter() {
        gephiMock.mockSuccess(
                GEPHI_WORKSPACE, node(0, "Foo"), node(1, "Bar"), relationship(0, "KNOWS", 0, 1, "7.2", Set.of("foo")));
        testCall(
                db,
                "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,$workspace,p,'weight',['foo']) yield nodes, relationships, format return *",
                map("workspace", GEPHI_WORKSPACE),
                r -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals("gephi", r.get("format"));
                });
    }

    @Test
    public void testWrongExportParameter() {
        gephiMock.mockSuccess(GEPHI_WORKSPACE, node(0, "Foo"), node(1, "Bar"), relationship(0, "KNOWS", 0, 1, "7.2"));
        testCall(
                db,
                "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,$workspace,p,'weight',['faa','fee']) yield nodes, relationships, format return *",
                map("workspace", GEPHI_WORKSPACE),
                r -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals("gephi", r.get("format"));
                });
    }

    @Test
    public void reservedExportParameter() {
        gephiMock.mockSuccess(GEPHI_WORKSPACE, node(0, "Foo"), node(1, "Bar"), relationship(0, "KNOWS", 0, 1, "7.2"));
        testCall(
                db,
                "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,$workspace,p,'weight',['directed','label']) yield nodes, relationships, format return *",
                map("workspace", GEPHI_WORKSPACE),
                r -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals("gephi", r.get("format"));
                });
    }
}
