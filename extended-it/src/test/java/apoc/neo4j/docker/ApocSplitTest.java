package apoc.neo4j.docker;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestContainerUtil.ApocPackage;
import apoc.util.TestUtil;
import org.junit.Test;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.driver.Session;

import static org.junit.Assert.assertTrue;

/*
 This test is just to verify the split of core and extended
 */
public class ApocSplitTest {
    public static final List<String> PROCEDURES_FROM_CORE = List.of();

    public static final List<String> FUNCTIONS_FROM_CORE = List.of();

    public static final List<String> EXTENDED_PROCEDURES = List.of(
        "apoc.metrics.list",
        "apoc.metrics.storage",
        "apoc.metrics.get",
        "apoc.monitor.kernel",
        "apoc.monitor.store",
        "apoc.monitor.ids",
        "apoc.monitor.tx",
        "apoc.ttl.expire",
        "apoc.ttl.expireIn",
        "apoc.static.list",
        "apoc.static.set",
        "apoc.bolt.load",
        "apoc.bolt.load.fromLocal",
        "apoc.bolt.execute",
        "apoc.config.list",
        "apoc.config.map",
        "apoc.redis.getSet",
        "apoc.redis.get",
        "apoc.redis.append",
        "apoc.redis.incrby",
        "apoc.redis.hdel",
        "apoc.redis.hexists",
        "apoc.redis.hget",
        "apoc.redis.hincrby",
        "apoc.redis.hgetall",
        "apoc.redis.hset",
        "apoc.redis.push",
        "apoc.redis.pop",
        "apoc.redis.lrange",
        "apoc.redis.sadd",
        "apoc.redis.scard",
        "apoc.redis.spop",
        "apoc.redis.smembers",
        "apoc.redis.sunion",
        "apoc.redis.zadd",
        "apoc.redis.zcard",
        "apoc.redis.zrangebyscore",
        "apoc.redis.zrem",
        "apoc.redis.eval",
        "apoc.redis.copy",
        "apoc.redis.exists",
        "apoc.redis.pexpire",
        "apoc.redis.persist",
        "apoc.redis.pttl",
        "apoc.redis.info",
        "apoc.redis.configGet",
        "apoc.redis.configSet",
        "apoc.systemdb.export.metadata",
        "apoc.systemdb.graph",
        "apoc.systemdb.execute",
        "apoc.algo.aStarWithPoint",
        "apoc.get.nodes",
        "apoc.get.rels",
        "apoc.cypher.runFile",
        "apoc.cypher.runFiles",
        "apoc.cypher.runSchemaFile",
        "apoc.cypher.runSchemaFiles",
        "apoc.cypher.parallel",
        "apoc.cypher.mapParallel",
        "apoc.cypher.mapParallel2",
        "apoc.cypher.parallel2",
        "apoc.gephi.add",
        "apoc.mongo.aggregate",
        "apoc.mongo.count",
        "apoc.mongo.find",
        "apoc.mongo.insert",
        "apoc.mongo.update",
        "apoc.mongo.delete",
        "apoc.mongodb.get.byObjectId",
        // TODO Re-add this once it's included in the package
        //"apoc.load.xls",
        "apoc.load.csv",
        "apoc.load.csvParams",
        "apoc.load.ldap",
        "apoc.load.driver",
        "apoc.load.jdbc",
        "apoc.load.jdbcUpdate",
        "apoc.load.htmlPlainText",
        "apoc.load.html",
        "apoc.load.directory.async.add",
        "apoc.load.directory.async.remove",
        "apoc.load.directory.async.removeAll",
        "apoc.load.directory.async.list",
        "apoc.load.directory",
        "apoc.dv.catalog.add",
        "apoc.dv.catalog.remove",
        "apoc.dv.catalog.list",
        "apoc.dv.query",
        "apoc.dv.queryAndLink",
        "apoc.model.jdbc",
        "apoc.generate.ba",
        "apoc.generate.ws",
        "apoc.generate.er",
        "apoc.generate.complete",
        "apoc.generate.simple",
        "apoc.log.error",
        "apoc.log.warn",
        "apoc.log.info",
        "apoc.log.debug",
        "apoc.es.stats",
        "apoc.es.get",
        "apoc.es.query",
        "apoc.es.getRaw",
        "apoc.es.postRaw",
        "apoc.es.post",
        "apoc.es.put",
        // TODO Re-add this once it's included in the package
        // "apoc.export.xls.all",
        // "apoc.export.xls.data",
        // "apoc.export.xls.graph",
        // "apoc.export.xls.query",
        "apoc.custom.declareProcedure",
        "apoc.custom.declareFunction",
        "apoc.custom.list",
        "apoc.custom.removeProcedure",
        "apoc.custom.removeFunction",
        "apoc.uuid.install",
        "apoc.uuid.remove",
        "apoc.uuid.removeAll",
        "apoc.uuid.list",
        "apoc.couchbase.get",
        "apoc.couchbase.exists",
        "apoc.couchbase.insert",
        "apoc.couchbase.upsert",
        "apoc.couchbase.append",
        "apoc.couchbase.prepend",
        "apoc.couchbase.remove",
        "apoc.couchbase.replace",
        "apoc.couchbase.query",
        "apoc.couchbase.posParamsQuery",
        "apoc.couchbase.namedParamsQuery",
        "apoc.nlp.azure.entities.graph",
        "apoc.nlp.azure.entities.stream",
        "apoc.nlp.azure.keyPhrases.graph",
        "apoc.nlp.azure.keyPhrases.stream",
        "apoc.nlp.azure.sentiment.graph",
        "apoc.nlp.azure.sentiment.stream");

    public static final List<String> EXTENDED_FUNCTIONS = List.of(
        "apoc.trigger.nodesByLabel",
        "apoc.trigger.propertiesByKey",
        "apoc.trigger.toNode",
        "apoc.trigger.toRelationship",
        "apoc.ttl.config",
        "apoc.static.get",
        "apoc.static.getAll",
        "apoc.coll.avgDuration"
        // TODO Re-add this once it's included in the package
        //"apoc.data.email"
       );

    @Test
    public void test() {
        if (!TestUtil.isRunningInCI()) {
            Neo4jContainerExtension neo4jContainer = TestContainerUtil.createEnterpriseDB(List.of(ApocPackage.EXTENDED), true)
                    .withNeo4jConfig("dbms.transaction.timeout", "60s");


            neo4jContainer.start();

            assertTrue("Neo4j Instance should be up-and-running", neo4jContainer.isRunning());

            Session session = neo4jContainer.getSession();
            Set<String> procedureNames = session.run("SHOW PROCEDURES YIELD name WHERE name STARTS WITH 'apoc' RETURN name").stream().map(s -> s.get( "name" ).asString()).collect(Collectors.toSet());
            Set<String> functionNames = session.run("SHOW FUNCTIONS YIELD name WHERE name STARTS WITH 'apoc' RETURN name").stream().map(s -> s.get( "name" ).asString()).collect(Collectors.toSet());

            var expectedProcedures = Stream.concat(PROCEDURES_FROM_CORE.stream(), EXTENDED_PROCEDURES.stream()).collect(Collectors.toSet());
            var expectedFunctions = Stream.concat(FUNCTIONS_FROM_CORE.stream(), EXTENDED_FUNCTIONS.stream()).collect(Collectors.toSet());
            expectedProcedures.stream().filter(s -> !procedureNames.contains( s ) ).collect(Collectors.toList()).forEach( s -> System.out.println(s) );
            expectedFunctions.stream().filter(s -> !functionNames.contains( s ) ).collect(Collectors.toList()).forEach( s -> System.out.println(s) );
            procedureNames.stream().filter(s -> !expectedProcedures.contains( s ) ).collect(Collectors.toList()).forEach( s -> System.out.println(s) );
            functionNames.stream().filter(s -> !expectedFunctions.contains( s ) ).collect(Collectors.toList()).forEach( s -> System.out.println(s) );

            assertTrue(procedureNames.containsAll(expectedProcedures) && procedureNames.size() == expectedProcedures.size());
            assertTrue(functionNames.containsAll(expectedFunctions) && functionNames.size() == expectedFunctions.size());

            neo4jContainer.close();
        }
    }
}
