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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import apoc.util.TestUtil;
import apoc.util.Util;
import java.sql.SQLException;
import java.util.Map;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsMapContaining;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.containers.CassandraContainer;

public class CassandraJdbcTest extends AbstractJdbcTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    public static CassandraContainer cassandra;

    @BeforeClass
    public static void setUp() throws Exception {
        cassandra = new CassandraContainer();
        cassandra.withInitScript("init_cassandra.cql");
        cassandra.start();

        TestUtil.registerProcedure(db, Jdbc.class);
        db.executeTransactionally("CALL apoc.load.driver('com.github.adejanovski.cassandra.jdbc.CassandraDriver')");
    }

    @AfterClass
    public static void tearDown() throws SQLException {
        cassandra.stop();
        db.shutdown();
    }

    @Test
    public void testLoadJdbc() {
        testCall(
                db,
                "CALL apoc.load.jdbc($url,'\"PERSON\"')",
                Util.map(
                        "url",
                        getUrl(),
                        "config",
                        Util.map(
                                "schema",
                                "test",
                                "credentials",
                                Util.map("user", cassandra.getUsername(), "password", cassandra.getPassword()))),
                (row) -> assertResult(row));
    }

    @Test
    public void testLoadJdbcSelect() {
        testCall(
                db,
                "CALL apoc.load.jdbc($url,'SELECT * FROM \"PERSON\"')",
                Util.map(
                        "url",
                        getUrl(),
                        "config",
                        Util.map(
                                "schema",
                                "test",
                                "credentials",
                                Util.map("user", cassandra.getUsername(), "password", cassandra.getPassword()))),
                (row) -> assertResult(row));
    }

    @Test
    public void testLoadJdbcUpdate() {
        TestUtil.singleResultFirstColumn(
                db,
                "CALL apoc.load.jdbcUpdate($url,'UPDATE \"PERSON\" SET \"SURNAME\" = ? WHERE \"NAME\" = ?', ['DOE', 'John'])",
                Util.map(
                        "url",
                        getUrl(),
                        "config",
                        Util.map(
                                "schema",
                                "test",
                                "credentials",
                                Util.map("user", cassandra.getUsername(), "password", cassandra.getPassword()))));
        testCall(
                db,
                "CALL apoc.load.jdbc($url,'SELECT * FROM \"PERSON\" WHERE \"NAME\" = ?', ['John'])",
                Util.map(
                        "url",
                        getUrl(),
                        "config",
                        Util.map(
                                "schema",
                                "test",
                                "credentials",
                                Util.map("user", cassandra.getUsername(), "password", cassandra.getPassword()))),
                (r) -> {
                    Map<String, Object> row = (Map<String, Object>) r.get("row");
                    assertThat(
                            row,
                            Matchers.allOf(
                                    IsMapContaining.hasEntry("NAME", "John"),
                                    IsMapContaining.hasEntry(
                                            "SURNAME",
                                            "DOE"), // FIXME: it seems that cassandra is not updated via first statment
                                    // in this method
                                    IsMapContaining.hasEntry(
                                            "EFFECTIVE_FROM_DATE", (Object) effectiveFromDate.toLocalDateTime())));
                });
    }

    @Test
    public void testLoadJdbcParams() {
        testCall(
                db,
                "CALL apoc.load.jdbc($url,'SELECT * FROM \"PERSON\" WHERE \"NAME\" = ?', ['John'])",
                Util.map(
                        "url",
                        getUrl(),
                        "config",
                        Util.map(
                                "schema",
                                "test",
                                "credentials",
                                Util.map("user", cassandra.getUsername(), "password", cassandra.getPassword()))),
                (row) -> assertResult(row));
    }

    public static String getUrl() {
        return String.format(
                "jdbc:cassandra://%s:%s/apoc_schema",
                cassandra.getContainerIpAddress(), cassandra.getMappedPort(cassandra.CQL_PORT));
    }

    @Override
    public void assertResult(Map<String, Object> row) {
        Map<String, Object> expected =
                Util.map("NAME", "John", "SURNAME", null, "EFFECTIVE_FROM_DATE", effectiveFromDate.toLocalDateTime());
        assertEquals(expected, row.get("row"));
    }
}
