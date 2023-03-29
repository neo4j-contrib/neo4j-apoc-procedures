package apoc.uuid;

import org.hamcrest.Matchers;
import org.junit.rules.TemporaryFolder;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;

import java.io.IOException;
import java.util.Map;

import static apoc.ExtendedApocConfig.APOC_UUID_ENABLED;
import static apoc.util.DbmsTestUtil.startDbWithApocConfigs;
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
    public static DatabaseManagementService startDbWithUuidApocConfigs(TemporaryFolder storeDir) throws IOException {
        return startDbWithApocConfigs(storeDir,
                Map.of(APOC_UUID_REFRESH, PROCEDURE_DEFAULT_REFRESH,
                        APOC_UUID_ENABLED, "true")
        );
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
