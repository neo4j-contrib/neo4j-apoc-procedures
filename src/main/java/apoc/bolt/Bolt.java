package apoc.bolt;

import apoc.Description;
import apoc.result.RowResult;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.UriResolver;
import apoc.util.Util;
import org.neo4j.driver.internal.InternalEntity;
import org.neo4j.driver.internal.logging.JULogging;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.summary.SummaryCounters;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.File;
import java.net.URISyntaxException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.stream.Collectors;
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
    @Description("apoc.bolt.load(url-or-key, statement, params, config) - access to other databases via bolt for read/write")
    public Stream<RowResult> load(@Name("url") String url, @Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws URISyntaxException {
        if (params == null) params = Collections.emptyMap();
        BoltConfig boltConfig = new BoltConfig(config);


        Config driverConfig = toDriverConfig(config.getOrDefault("driverConfig", map()));
        UriResolver uri = new UriResolver(url, "bolt");
        uri.initialize();
        try {
            final Driver driver = GraphDatabase.driver(uri.getConfiguredUri(), uri.getToken(), driverConfig);
            final Session session = driver.session();
            final Stream<RowResult> result;
            if (boltConfig.isAddStatistics()) {
                result = Stream.of(new RowResult(toMap(runStatement(statement, session, params, boltConfig).summary().counters())));
            } else {
                result = getRowResultStream(session, params, statement, boltConfig);
            }
            return result.onClose(() -> {
                session.close();
                driver.close();
            });
        } catch (Exception e) {
            throw new RuntimeException("It's not possible to create a connection due to: " + e.getMessage());
        }
    }

    @Procedure()
    @Description("apoc.bolt.execute(url-or-key, statement, params, config) - access to other databases via bolt for read")
    public Stream<RowResult> execute(@Name("url") String url, @Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws URISyntaxException {
        Map<String, Object> configuration = new HashMap<>(config);
        configuration.put("readOnly", false);
        return load(url, statement, params, configuration);
    }

    private StatementResult runStatement(@Name("statement") String statement, Session session, Map<String, Object> finalParams, BoltConfig boltConfig) {
        return boltConfig.isReadOnly()
                ? session.readTransaction((Transaction tx) -> tx.run(statement, finalParams))
                : session.writeTransaction((Transaction tx) -> tx.run(statement, finalParams));
    }

    private Stream<RowResult> getRowResultStream(Session session, Map<String, Object> params, String statement, BoltConfig boltConfig) {
        Map<Long, org.neo4j.graphdb.Node> nodesCache = new ConcurrentHashMap<>();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(runStatement(statement, session, params, boltConfig), 0), true)
                .map(record -> new RowResult(record.asMap(value -> convert(session, value, boltConfig, nodesCache))));
    }

    private Object convert(Session session, Object entity, BoltConfig boltConfig, Map<Long, org.neo4j.graphdb.Node> nodeCache) {
        if (entity instanceof Value) return convert(session, ((Value) entity).asObject(), boltConfig, nodeCache);
        if (entity instanceof Node) return toNode(entity, boltConfig, nodeCache);
        if (entity instanceof Relationship) return toRelationship(session, entity, boltConfig, nodeCache);
        if (entity instanceof Path) return toPath(session, entity, boltConfig, nodeCache);
        if (entity instanceof Collection) return toCollection(session, (Collection) entity, boltConfig, nodeCache);
        if (entity instanceof Map) return toMap(session, (Map<String, Object>) entity, boltConfig, nodeCache);
        return entity;
    }

    private Object toMap(Session session, Map<String, Object> entity, BoltConfig boltConfig, Map<Long, org.neo4j.graphdb.Node> nodeCache) {
        return entity.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry(entry.getKey(), convert(session, entry.getValue(), boltConfig, nodeCache)))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    }

    private Object toCollection(Session session, Collection entity, BoltConfig boltConfig, Map<Long, org.neo4j.graphdb.Node> nodeCache) {
        return entity.stream()
                .map(elem -> convert(session, elem, boltConfig, nodeCache))
                .collect(Collectors.toList());
    }

    private Object toNode(Object value, BoltConfig boltConfig, Map<Long, org.neo4j.graphdb.Node> nodesCache) {
        Node node;
        if (value instanceof Value) {
            node = ((InternalEntity) value).asValue().asNode();
        } else if (value instanceof Node) {
            node = (Node) value;
        } else {
            throw getUnsupportedConversionException(value);
        }
        if (boltConfig.isVirtual()) {
            final Label[] labels = getLabelsAsArray(node);
            return nodesCache.computeIfAbsent(node.id(), (id) -> new VirtualNode(id, labels, node.asMap(), db));
        } else {
            return Util.map("entityType", "NODE", "labels", node.labels(), "id", node.id(), "properties", node.asMap());
        }
    }

    private Object toRelationship(Session session, Object value, BoltConfig boltConfig, Map<Long, org.neo4j.graphdb.Node> nodesCache) {
        Relationship relationship;
        if (value instanceof Value) {
            relationship = ((InternalEntity) value).asValue().asRelationship();
        } else if (value instanceof Relationship) {
            relationship = (Relationship) value;
        } else {
            throw getUnsupportedConversionException(value);
        }
        if (boltConfig.isVirtual()) {
            final org.neo4j.graphdb.Node start;
            final org.neo4j.graphdb.Node end;
            if (boltConfig.isWithRelationshipNodeProperties()) {
                final Function<Long, org.neo4j.graphdb.Node> retrieveNode = (id) -> {
                    final Node node = session.readTransaction(tx -> tx.run("MATCH (n) WHERE id(n) = $id RETURN n",
                            Collections.singletonMap("id", id)))
                            .single()
                            .get("n")
                            .asNode();
                    final Label[] labels = getLabelsAsArray(node);
                    return new VirtualNode(id, labels, node.asMap(), db);
                };
                start = nodesCache.computeIfAbsent(relationship.startNodeId(), retrieveNode);
                end = nodesCache.computeIfAbsent(relationship.endNodeId(), retrieveNode);
            } else {
                start = nodesCache.getOrDefault(relationship.startNodeId(), new VirtualNode(relationship.startNodeId(), db));
                end = nodesCache.getOrDefault(relationship.startNodeId(), new VirtualNode(relationship.endNodeId(), db));
            }
            return new VirtualRelationship(relationship.id(), start, end, RelationshipType.withName(relationship.type()), relationship.asMap());
        } else {
            return Util.map("entityType", "RELATIONSHIP", "type", relationship.type(), "id", relationship.id(), "start", relationship.startNodeId(), "end", relationship.endNodeId(), "properties", relationship.asMap());
        }
    }

    private ClassCastException getUnsupportedConversionException(Object value) {
        return new ClassCastException("Conversion from class " + value.getClass().getName() + " not supported");
    }

    private Label[] getLabelsAsArray(Node node) {
        return StreamSupport.stream(node.labels().spliterator(), false)
                .map(Label::label)
                .collect(Collectors.toList())
                .toArray(new Label[0]);
    }

    private Object toPath(Session session, Object value, BoltConfig boltConfig, Map<Long, org.neo4j.graphdb.Node> nodesCache) {
        List<Object> entityList = new LinkedList<>();
        Path path;
        if (value instanceof Value) {
            path = ((InternalEntity) value).asValue().asPath();
        } else if (value instanceof Path) {
            path = (Path) value;
        } else {
            throw getUnsupportedConversionException(value);
        }
        path.forEach(p -> {
            entityList.add(toNode(p.start(), boltConfig, nodesCache));
            entityList.add(toRelationship(session, p.relationship(), boltConfig, nodesCache));
            entityList.add(toNode(p.end(), boltConfig, nodesCache));
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
        boolean encryption = (boolean) driverConfMap.getOrDefault("encryption", true);
        boolean logLeakedSessions = (boolean) driverConfMap.getOrDefault("logLeakedSessions", true);
        Long maxIdleConnectionPoolSize = (Long) driverConfMap.getOrDefault("maxIdleConnectionPoolSize", 10L);
        Long idleTimeBeforeConnectionTest = (Long) driverConfMap.getOrDefault("idleTimeBeforeConnectionTest", -1L);
        String trustStrategy = (String) driverConfMap.getOrDefault("trustStrategy", "TRUST_ALL_CERTIFICATES");
        Long routingFailureLimit = (Long) driverConfMap.getOrDefault("routingFailureLimit", 1L);
        Long routingRetryDelayMillis = (Long) driverConfMap.getOrDefault("routingRetryDelayMillis", 5000L);
        Long connectionTimeoutMillis = (Long) driverConfMap.getOrDefault("connectionTimeoutMillis", 5000L);
        Long maxRetryTimeMs = (Long) driverConfMap.getOrDefault("maxRetryTimeMs", 30000L);
        Config.ConfigBuilder config = Config.build();
        config.withLogging(new JULogging(Level.parse(logging)));
        if(!encryption) config.withoutEncryption();
        config.withTrustStrategy(Config.TrustStrategy.trustAllCertificates());
        if(!logLeakedSessions) config.withoutEncryption();
        config.withMaxIdleSessions(maxIdleConnectionPoolSize.intValue());
        config.withConnectionLivenessCheckTimeout(idleTimeBeforeConnectionTest, TimeUnit.MILLISECONDS);
        config.withRoutingFailureLimit(routingFailureLimit.intValue());
        config.withConnectionTimeout(connectionTimeoutMillis, TimeUnit.MILLISECONDS);
        config.withRoutingRetryDelay(routingRetryDelayMillis,TimeUnit.MILLISECONDS);
        config.withMaxTransactionRetryTime(maxRetryTimeMs, TimeUnit.MILLISECONDS);
        if(trustStrategy.equals("TRUST_ALL_CERTIFICATES")) config.withTrustStrategy(Config.TrustStrategy.trustAllCertificates());
        else if(trustStrategy.equals("TRUST_SYSTEM_CA_SIGNED_CERTIFICATES")) config.withTrustStrategy(Config.TrustStrategy.trustSystemCertificates());
        else {
            File file = new File(trustStrategy);
            config.withTrustStrategy(Config.TrustStrategy.trustCustomCertificateSignedBy(file));
        }
        return config.toConfig();
    }
}
