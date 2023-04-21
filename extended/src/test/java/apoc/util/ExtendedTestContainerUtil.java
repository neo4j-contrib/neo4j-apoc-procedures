package apoc.util;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;

import static apoc.util.TestContainerUtil.copyFilesToPlugin;
import static apoc.util.TestContainerUtil.executeGradleTasks;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class ExtendedTestContainerUtil
{
    public static TestcontainersCausalCluster createEnterpriseCluster( List<TestContainerUtil.ApocPackage> apocPackages, int numOfCoreInstances, int numberOfReadReplica, Map<String, Object> neo4jConfig, Map<String, String> envSettings) {
        return TestcontainersCausalCluster.create(apocPackages, numOfCoreInstances, numberOfReadReplica, Duration.ofMinutes(4), neo4jConfig, envSettings);
    }

    public static <T> T singleResultFirstColumn(Session session, String cypher) {
        return (T) session.writeTransaction(tx -> tx.run(cypher).single().fields().get(0).value().asObject());
    }

    public static void testCallInReadTransaction(Session session, String call, Consumer<Map<String, Object>> consumer) {
        TestContainerUtil.testCallInReadTransaction(session, call, null, consumer);
    }

    public static void addExtraDependencies() {
        File extraDepsDir = new File(TestContainerUtil.baseDir, "extra-dependencies");
        // build the extra-dependencies
        executeGradleTasks(extraDepsDir, "buildDependencies");

        // add all extra deps to the plugin docker folder
        final File directory = new File(extraDepsDir, "build/allJars");
        final IOFileFilter instance = new WildcardFileFilter("*-all.jar");
        copyFilesToPlugin(directory, instance, TestContainerUtil.pluginsFolder);
    }

    public static void testCallEventuallyInReadTransaction(Session session, String call, Consumer<Map<String, Object>> consumer, long timeout) {
        testCallEventuallyInReadTransaction(session, call, Collections.emptyMap(), consumer, timeout);
    }

    public static void testCallEventuallyInReadTransaction(Session session, String call, Map<String,Object> params, Consumer<Map<String, Object>> consumer, long timeout) {
        assertEventually(
                () -> session.readTransaction(tx -> testCallEventuallyCommon(call, params, consumer, tx)),
                (value) -> value, timeout, TimeUnit.SECONDS);
    }

    public static void testCallEventually(Session session, String call, Map<String,Object> params, Consumer<Map<String, Object>> consumer, long timeout) {
        assertEventually(
                () -> session.writeTransaction(tx -> testCallEventuallyCommon(call, params, consumer, tx)),
                (value) -> value, timeout, TimeUnit.SECONDS);
    }

    private static boolean testCallEventuallyCommon(String call, Map<String, Object> params, Consumer<Map<String, Object>> consumer, Transaction tx) {
        try {
            Iterator<Map<String, Object>> res = tx.run(call, params).stream()
                    .map(Record::asMap)
                    .toList().iterator();
            assertNotNull("result should be not null", res);
            assertTrue("result should be not empty", res.hasNext());
            Map<String, Object> row = res.next();
            consumer.accept(row);
            assertFalse("result should not have next", res.hasNext());
            return true;
        } catch (Exception e) {
            System.out.println("eventually error = " + e);
            tx.close();
            return false;
        }
    }

    public static String getBoltAddress(Neo4jContainerExtension instance) {
        return instance.getEnvMap().get("NEO4J_dbms_connector_bolt_advertised__address");
    }

    public static Session getSessionForDb(Neo4jContainerExtension instance, String dbName) {
        final Driver driver = instance.getDriver();
        return driver.session(SessionConfig.forDatabase(dbName));
    }

    public static boolean dbIsWriter(String dbName, Session session, String boltAddress) {
        return session.run( "SHOW DATABASE $dbName WHERE address = $boltAddress",
                        Map.of("dbName", dbName, "boltAddress", boltAddress) )
                .single().get("writer")
                .asBoolean();
    }
}
