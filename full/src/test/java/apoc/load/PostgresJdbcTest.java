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
package apoc.load;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import apoc.util.TestUtil;
import apoc.util.Util;
import java.sql.SQLException;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

public class PostgresJdbcTest extends AbstractJdbcTest {

    // Even if Postgres support the `TIMESTAMP WITH TIMEZONE` type,
    // the JDBC driver doesn't. Please check https://github.com/pgjdbc/pgjdbc/issues/996 and when the issue is closed
    // fix this

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    public static JdbcDatabaseContainer postgress;

    @BeforeClass
    public static void setUp() throws Exception {
        postgress = new PostgreSQLContainer().withInitScript("init_postgres.sql");
        postgress.start();
        TestUtil.registerProcedure(db, Jdbc.class);
        db.executeTransactionally("CALL apoc.load.driver('org.postgresql.Driver')");
    }

    @AfterClass
    public static void tearDown() throws SQLException {
        postgress.stop();
        db.shutdown();
    }

    @Test
    public void testLoadJdbc() {
        testCall(
                db,
                "CALL apoc.load.jdbc($url,'PERSON',[], $config)",
                Util.map(
                        "url",
                        postgress.getJdbcUrl(),
                        "config",
                        Util.map(
                                "schema",
                                "test",
                                "credentials",
                                Util.map("user", postgress.getUsername(), "password", postgress.getPassword()))),
                (row) -> assertResult(row));
    }

    @Test
    public void testLoadJdbSelect() {
        testCall(
                db,
                "CALL apoc.load.jdbc($url,'SELECT * FROM PERSON',[], $config)",
                Util.map(
                        "url",
                        postgress.getJdbcUrl(),
                        "config",
                        Util.map(
                                "schema",
                                "test",
                                "credentials",
                                Util.map("user", postgress.getUsername(), "password", postgress.getPassword()))),
                (row) -> assertResult(row));
    }

    @Test
    public void testLoadJdbSelectWithArrays() throws Exception {
        testCall(
                db,
                "CALL apoc.load.jdbc($url,'SELECT * FROM ARRAY_TABLE',[], $config)",
                Util.map(
                        "url",
                        postgress.getJdbcUrl(),
                        "config",
                        Util.map(
                                "schema",
                                "test",
                                "credentials",
                                Util.map("user", postgress.getUsername(), "password", postgress.getPassword()))),
                (result) -> {
                    Map<String, Object> row = (Map<String, Object>) result.get("row");
                    assertEquals("John", row.get("NAME"));
                    int[] intVals = (int[]) row.get("INT_VALUES");
                    assertArrayEquals(intVals, new int[] {1, 2, 3});
                    double[] doubleVals = (double[]) row.get("DOUBLE_VALUES");
                    assertArrayEquals(doubleVals, new double[] {1.0, 2.0, 3.0}, 0.01);
                });
    }

    @Test
    public void testLoadJdbcUpdate() {
        testCall(
                db,
                "CALL apoc.load.jdbcUpdate($url,'UPDATE PERSON SET \"SURNAME\" = ? WHERE \"NAME\" = ?', ['DOE', 'John'], $config)",
                Util.map(
                        "url",
                        postgress.getJdbcUrl(),
                        "config",
                        Util.map(
                                "schema",
                                "test",
                                "credentials",
                                Util.map("user", postgress.getUsername(), "password", postgress.getPassword()))),
                (row) -> assertEquals(Util.map("count", 1), row.get("row")));
    }

    @Test
    public void testLoadJdbcParams() {
        testCall(
                db,
                "CALL apoc.load.jdbc($url,'SELECT * FROM PERSON WHERE \"NAME\" = ?',['John'], $config)", //  YIELD row
                // RETURN row
                Util.map(
                        "url",
                        postgress.getJdbcUrl(),
                        "config",
                        Util.map(
                                "schema",
                                "test",
                                "credentials",
                                Util.map("user", postgress.getUsername(), "password", postgress.getPassword()))),
                (row) -> assertResult(row));
    }
}
