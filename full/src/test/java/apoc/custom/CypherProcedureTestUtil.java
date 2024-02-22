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
package apoc.custom;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;

public class CypherProcedureTestUtil {

    public static void assertProcedureFails(
            GraphDatabaseService db, String expectedMessage, String query, Map<String, Object> params) {
        try {
            testCall(db, query, params, row -> fail("The test should fail because of: " + expectedMessage));
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            String message = except.getMessage();
            assertTrue("Actual error is: " + message, message.contains(expectedMessage));
        }
    }

    public static void assertProcedureFails(GraphDatabaseService db, String expectedMessage, String query) {
        assertProcedureFails(db, expectedMessage, query, Map.of());
    }
}
