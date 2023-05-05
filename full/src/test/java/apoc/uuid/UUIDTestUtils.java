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
package apoc.uuid;

import org.hamcrest.Matchers;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Map;

import static apoc.ApocConfig.APOC_UUID_ENABLED;
import static apoc.util.SystemDbTestUtil.PROCEDURE_DEFAULT_REFRESH;
import static apoc.util.SystemDbTestUtil.TIMEOUT;
import static apoc.util.TestUtil.testCallEventually;
import static apoc.uuid.UUIDTest.UUID_TEST_REGEXP;
import static apoc.uuid.UuidConfig.*;
import static apoc.uuid.UuidConfig.ADD_TO_SET_LABELS_KEY;
import static apoc.uuid.UuidHandler.APOC_UUID_REFRESH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class UUIDTestUtils {
    public static ProvideSystemProperty setUuidApocConfs() {
        return new ProvideSystemProperty(APOC_UUID_REFRESH, String.valueOf(PROCEDURE_DEFAULT_REFRESH))
                .and(APOC_UUID_ENABLED, "true");
    }

    public static void awaitUuidDiscovered(GraphDatabaseService db, String label) {
        awaitUuidDiscovered(db, label, DEFAULT_UUID_PROPERTY, DEFAULT_ADD_TO_SET_LABELS);
    }

    public static void awaitUuidDiscovered(GraphDatabaseService db, String label, String expectedUuidProp, boolean expectedAddToSetLabels) {
        String call = "CALL apoc.uuid.list() YIELD properties, label WHERE label = $label " +
                "RETURN properties.uuidProperty AS uuidProperty, properties.addToSetLabels AS addToSetLabels";
        testCallEventually(db, call,
                Map.of("label", label),
                row -> {
                    assertEquals(expectedUuidProp, row.get(UUID_PROPERTY_KEY));
                    assertEquals(expectedAddToSetLabels, row.get(ADD_TO_SET_LABELS_KEY));
                }, TIMEOUT);
    }

    public static void assertIsUUID(Object uuid) {
        assertThat((String) uuid, Matchers.matchesRegex(UUID_TEST_REGEXP));
    }
}
