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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import static apoc.util.TestUtil.registerProcedure;
import static apoc.util.TestUtil.testCall;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

/**
 * @author mh
 * @since 29.05.16
 */
public class GephiTest {

    private static final String GEPHI_WORKSPACE = "workspace1";

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static boolean isGephiRunning() {
        try {
            URL url = new URL(String.format("http://localhost:8080/%s", GEPHI_WORKSPACE));
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("HEAD");
            try (InputStream in = con.getInputStream()) {
                return true;
            }
        } catch (Exception e) {
            return false;
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {
        assumeTrue(isGephiRunning());
        registerProcedure(db, Gephi.class);
        db.executeTransactionally("CREATE (:Foo {name:'Foo'})-[:KNOWS{weight:7.2,foo:'foo',bar:3.0,directed:'error',label:'foo'}]->(:Bar {name:'Bar'})");
    }

    @AfterClass
    public static void teardown() {
        db.shutdown();
    }

    @Test
    public void testAdd() throws Exception {
        testCall(db, "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,$workspace,p) yield nodes, relationships, format return *",
                map("workspace", GEPHI_WORKSPACE),
                r -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals("gephi", r.get("format"));
                });
    }
    @Test
    public void testWeightParameter() throws Exception {
        testCall(db, "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,$workspace,p,'weight') yield nodes, relationships, format return *",
                map("workspace", GEPHI_WORKSPACE),
                r -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals("gephi", r.get("format"));
                });
    }

    @Test
    public void testWrongWeightParameter() throws Exception {
        testCall(db, "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,$workspace,p,'test') yield nodes, relationships, format return *",
                map("workspace", GEPHI_WORKSPACE),
                r -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals("gephi", r.get("format"));
                });
    }

    @Test
    public void testRightExportParameter() throws Exception {
        testCall(db, "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,$workspace,p,'weight',['foo']) yield nodes, relationships, format return *",
                map("workspace", GEPHI_WORKSPACE),
                r -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals("gephi", r.get("format"));
                });
    }

    @Test
    public void testWrongExportParameter() throws Exception {
        testCall(db, "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,$workspace,p,'weight',['faa','fee']) yield nodes, relationships, format return *",
                map("workspace", GEPHI_WORKSPACE),
                r -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals("gephi", r.get("format"));
                });
    }

    @Test
    public void reservedExportParameter() throws Exception {
        testCall(db, "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,$workspace,p,'weight',['directed','label']) yield nodes, relationships, format return *",
                map("workspace", GEPHI_WORKSPACE),
                r -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals("gephi", r.get("format"));
                });
    }
}
