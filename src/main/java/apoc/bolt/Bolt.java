package apoc.bolt;

import apoc.Description;
import apoc.result.RowResult;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.UriResolver;
import apoc.util.Util;
import org.neo4j.driver.*;
import org.neo4j.driver.internal.InternalEntity;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.logging.JULogging;
import org.neo4j.driver.summary.SummaryCounters;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.File;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.util.MapUtil.map;

/**
 * @author AgileLARUS
 * @since 29.08.17
 */
public class Bolt {

    @Context
    public GraphDatabaseService db;

    @Procedure()
    @Description("apoc.bolt.load(url-or-key, kernelTransaction, params, config) - access to other databases via bolt for read")
    public Stream<RowResult> load(@Name("url") String url, @Name("kernelTransaction") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws URISyntaxException {
        if (params == null) params = Collections.emptyMap();
        if (config == null) config = Collections.emptyMap();
        boolean virtual = (boolean) config.getOrDefault("virtual", false);
        boolean addStatistics = (boolean) config.getOrDefault("statistics", false);
        boolean readOnly = (boolean) config.getOrDefault("readOnly", true);

        Config driverConfig = toDriverConfig(config.getOrDefault("driverConfig", map()));
        UriResolver uri = new UriResolver(url, "bolt");
        uri.initialize();

        try (Driver driver = GraphDatabase.driver(uri.getConfiguredUri(), uri.getToken(), driverConfig);
             Session session = driver.session()) {
            if (addStatistics)
                return Stream.of(new RowResult(toMap(runStatement(statement, session, params, readOnly).summary().counters())));
            else
                return getRowResultStream(virtual, session, params, statement, readOnly);
        } catch (Exception e) {
            throw new RuntimeException("It's not possible to create a connection due to: " + e.getMessage());
        }
    }

    @Procedure()
    @Description("apoc.bolt.execute(url-or-key, kernelTransaction, params, config) - access to other databases via bolt for read")
    public Stream<RowResult> execute(@Name("url") String url, @Name("kernelTransaction") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws URISyntaxException {
        Map<String, Object> configuration = new HashMap<>(config);
        configuration.put("readOnly", false);
        return load(url, statement, params, configuration);
    }

    private StatementResult runStatement(@Name("kernelTransaction") String statement, Session session, Map<String, Object> finalParams, boolean read) {
        if (read) return session.readTransaction((Transaction tx) -> tx.run(statement, finalParams));
        else return session.writeTransaction((Transaction tx) -> tx.run(statement, finalParams));
    }

    private Stream<RowResult> getRowResultStream(boolean virtual, Session session, Map<String, Object> params, String statement, boolean read) {
        Map<Long, Object> nodesCache = new HashMap<>();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(runStatement(statement, session, params, read), 0), true)
                .map(record -> new RowResult(record.asMap(value -> {
                    Object entity = value.asObject();
                    if (entity instanceof Node) return toNode(entity, virtual, nodesCache);
                    if (entity instanceof Relationship) return toRelationship(entity, virtual, nodesCache);
                    if (entity instanceof Path) return toPath(entity, virtual, nodesCache);
                    return entity;
                })));
    }

    private Object toNode(Object value, boolean virtual, Map<Long, Object> nodesCache) {
        Value internalValue = ((InternalEntity) value).asValue();
        Node node = internalValue.asNode();
        if (virtual) {
            List<Label> labels = new ArrayList<>();
            node.labels().forEach(l -> labels.add(Label.label(l)));
            VirtualNode virtualNode = new VirtualNode(node.id(), labels.toArray(new Label[0]), node.asMap());
            nodesCache.put(node.id(), virtualNode);
            return virtualNode;
        } else
            return Util.map("entityType", internalValue.type().name(), "labels", node.labels(), "id", node.id(), "properties", node.asMap());
    }

    private Object toRelationship(Object value, boolean virtual, Map<Long, Object> nodesCache) {
        Value internalValue = ((InternalEntity) value).asValue();
        Relationship relationship = internalValue.asRelationship();
        if (virtual) {
            VirtualNode start = (VirtualNode) nodesCache.getOrDefault(relationship.startNodeId(), new VirtualNode(relationship.startNodeId()));
            VirtualNode end = (VirtualNode) nodesCache.getOrDefault(relationship.endNodeId(), new VirtualNode(relationship.endNodeId()));
            VirtualRelationship virtualRelationship = new VirtualRelationship(relationship.id(), start, end, RelationshipType.withName(relationship.type()), relationship.asMap());
            return virtualRelationship;
        } else
            return Util.map("entityType", internalValue.type().name(), "type", relationship.type(), "id", relationship.id(), "start", relationship.startNodeId(), "end", relationship.endNodeId(), "properties", relationship.asMap());
    }

    private Object toPath(Object value, boolean virtual, Map<Long, Object> nodesCache) {
        List<Object> entityList = new LinkedList<>();
        Value internalValue = ((InternalPath) value).asValue();
        internalValue.asPath().forEach(p -> {
            entityList.add(toNode(p.start(), virtual, nodesCache));
            entityList.add(toRelationship(p.relationship(), virtual, nodesCache));
            entityList.add(toNode(p.end(), virtual, nodesCache));
        });
        return entityList;
    }

    private Map<String, Object> toMap(SummaryCounters resultSummary) {
        return map(
                "nodesCreated", resultSummary.nodesCreated(),
                "nodesDeleted", resultSummary.nodesDeleted(),
                "labelsAdded", resultSummary.labelsAdded(),
                "labelsRemoved", resultSummary.labelsRemoved(),
                "relationshipsCreated", resultSummary.relationshipsCreated(),
                "relationshipsDeleted", resultSummary.relationshipsDeleted(),
                "propertiesSet", resultSummary.propertiesSet(),
                "constraintsAdded", resultSummary.constraintsAdded(),
                "constraintsRemoved", resultSummary.constraintsRemoved(),
                "indexesAdded", resultSummary.indexesAdded(),
                "indexesRemoved", resultSummary.indexesRemoved()
        );
    }

    private Config toDriverConfig(Object driverConfig) {
        Map<String, Object> driverConfMap = (Map<String, Object>) driverConfig;
        String logging = (String) driverConfMap.getOrDefault("logging", "INFO");
        boolean encryption = (boolean) driverConfMap.getOrDefault("encryption", false);
        boolean logLeakedSessions = (boolean) driverConfMap.getOrDefault("logLeakedSessions", true);
        Long idleTimeBeforeConnectionTest = (Long) driverConfMap.getOrDefault("idleTimeBeforeConnectionTest", -1L);
        String trustStrategy = (String) driverConfMap.getOrDefault("trustStrategy", "TRUST_ALL_CERTIFICATES");
//        Long routingFailureLimit = (Long) driverConfMap.getOrDefault("routingFailureLimit", 1L);
//        Long routingRetryDelayMillis = (Long) driverConfMap.getOrDefault("routingRetryDelayMillis", 5000L);
        Long connectionTimeoutMillis = (Long) driverConfMap.get("connectionTimeoutMillis");
        Long maxRetryTimeMs = (Long) driverConfMap.get("maxRetryTimeMs");
        Long maxConnectionLifeTime = (Long) driverConfMap.get("maxConnectionLifeTime");
        Long maxConnectionPoolSize = (Long) driverConfMap.get("maxConnectionPoolSize");
        Long routingTablePurgeDelay = (Long) driverConfMap.get("routingTablePurgeDelay");
        Long connectionAcquisitionTimeout = (Long) driverConfMap.get("connectionAcquisitionTimeout");

        Config.ConfigBuilder config = Config.builder();

        config.withLogging(new JULogging(Level.parse(logging)));
        if(encryption) config.withEncryption();
        config.withTrustStrategy(Config.TrustStrategy.trustAllCertificates());
        if(!logLeakedSessions) config.withoutEncryption();

        if (connectionAcquisitionTimeout!=null) {
            config.withConnectionAcquisitionTimeout(connectionAcquisitionTimeout, TimeUnit.MILLISECONDS);
        }
        //config.withDriverMetrics();
        if (maxConnectionLifeTime!=null) {
            config.withMaxConnectionLifetime(maxConnectionLifeTime, TimeUnit.MILLISECONDS);
        }
        if (maxConnectionPoolSize!=null) {
            config.withMaxConnectionPoolSize(maxConnectionPoolSize.intValue());
        }
        if (routingTablePurgeDelay!=null) {
            config.withRoutingTablePurgeDelay(routingTablePurgeDelay, TimeUnit.MILLISECONDS);
        }
//        config.withRoutingFailureLimit(routingFailureLimit.intValue());
//        config.withRoutingRetryDelay(routingRetryDelayMillis,TimeUnit.MILLISECONDS);
        if (idleTimeBeforeConnectionTest!=null) {
            config.withConnectionLivenessCheckTimeout(idleTimeBeforeConnectionTest, TimeUnit.MILLISECONDS);
        }
        if (connectionTimeoutMillis!=null) {
            config.withConnectionTimeout(connectionTimeoutMillis, TimeUnit.MILLISECONDS);
        }
        if (maxRetryTimeMs!=null) {
            config.withMaxTransactionRetryTime(maxRetryTimeMs, TimeUnit.MILLISECONDS);
        }
        if(trustStrategy.equals("TRUST_ALL_CERTIFICATES")) config.withTrustStrategy(Config.TrustStrategy.trustAllCertificates());
        else if(trustStrategy.equals("TRUST_SYSTEM_CA_SIGNED_CERTIFICATES")) config.withTrustStrategy(Config.TrustStrategy.trustSystemCertificates());
        else {
            File file = new File(trustStrategy);
            config.withTrustStrategy(Config.TrustStrategy.trustCustomCertificateSignedBy(file));
        }
        return config.build();
    }
}
