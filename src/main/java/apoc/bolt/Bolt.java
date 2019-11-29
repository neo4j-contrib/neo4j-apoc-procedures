package apoc.bolt;

import apoc.Description;
import apoc.export.cypher.ExportCypher;
import apoc.result.MapResult;
import apoc.result.RowResult;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.JsonUtil;
import apoc.util.UriResolver;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.internal.InternalEntity;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.logging.JULogging;
import org.neo4j.driver.internal.summary.InternalSummaryCounters;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
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
import org.neo4j.graphdb.Result;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

import java.io.File;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.concurrent.TimeUnit;
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

    @Context
    public TerminationGuard terminationGuard;

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

    @Procedure("apoc.bolt.load.fromLocal")
    public Stream<RowResult> fromLocal(@Name("url") String url,
                                       @Name("localStatement") String localStatement,
                                       @Name("remoteStatement") String remoteStatement,
                                       @Name(value = "config", defaultValue = "{}") Map<String, Object> config)  throws URISyntaxException {
        if (config == null) config = Collections.emptyMap();
        boolean virtual = (boolean) config.getOrDefault("virtual", false);
        boolean addStatistics = (boolean) config.getOrDefault("statistics", false);
        boolean readOnly = (boolean) config.getOrDefault("readOnly", true);
        boolean streamStatements = (boolean) config.getOrDefault("streamStatements", false);

        Config driverConfig = toDriverConfig(config.getOrDefault("driverConfig", map()));
        UriResolver uri = new UriResolver(url, "bolt");
        uri.initialize();
        try (Driver driver = GraphDatabase.driver(uri.getConfiguredUri(), uri.getToken(), driverConfig);
             Session session = driver.session();
             Result localResult = db.execute(localStatement)) {
            String withColumns = "WITH " + localResult.columns().stream()
                    .map(c -> "$" + c + " AS " + c)
                    .collect(Collectors.joining(", ")) + "\n";
            Map<Long, Object> nodesCache = new HashMap<>();
            List<RowResult> response = new ArrayList<>();
            while (localResult.hasNext()) {
                final StatementResult statementResult;
                Map<String, Object> row = localResult.next();
                if (streamStatements) {
                    final String statement = (String) row.get("statement");
                    if (StringUtils.isBlank(statement)) continue;
                    final Map<String, Object> params = Collections.singletonMap("params", row.getOrDefault("params", Collections.emptyMap()));
                    statementResult = runStatement(statement, session, params, readOnly);
                } else {
                    statementResult = runStatement(withColumns + remoteStatement, session, row, readOnly);
                }
                if (addStatistics) {
                    response.add(new RowResult(toMap(statementResult.summary().counters())));
                } else {
                    response.addAll(
                            statementResult.stream()
                            .map(record -> new RowResult(recordAsMap(virtual, nodesCache, record)))
                            .collect(Collectors.toList())
                    );
                }
            }
            return response.stream();
        } catch (Exception e) {
            throw new RuntimeException("It's not possible to create a connection due to: " + e.getMessage());
        }

    }

    private Map<String, Object> recordAsMap(boolean virtual, Map<Long, Object> nodesCache, Record record) {
        return record.asMap(value -> {
            Object entity = value.asObject();
            if (entity instanceof Node) return toNode(entity, virtual, nodesCache);
            if (entity instanceof Relationship) return toRelationship(entity, virtual, nodesCache);
            if (entity instanceof Path) return toPath(entity, virtual, nodesCache);
            return entity;
        });
    }

    @Procedure("apoc.bolt.clone.all")
    public Stream<MapResult> cloneAll(@Name("url") String url, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws URISyntaxException {
        if (config == null) config = Collections.emptyMap();
        Config driverConfig = toDriverConfig(config.getOrDefault("driverConfig", map()));
        UriResolver uri = new UriResolver(url, "bolt");
        uri.initialize();

        final ExportCypher exportCypher = new ExportCypher();
        exportCypher.db = db;
        exportCypher.terminationGuard = terminationGuard;
        try (Driver driver = GraphDatabase.driver(uri.getConfiguredUri(), uri.getToken(), driverConfig);
             Session session = driver.session()) {
            Map<String, Object> exportMap = new HashMap<>(config);
            exportMap.put("stream", true);
            exportMap.put("format", "plain");
            Stream<ExportCypher.DataProgressInfo> stream = exportCypher.all(null, exportMap);
            SummaryCounters counter = stream.flatMap(dataProgressInfo -> Stream.of(dataProgressInfo.cypherStatements.split(";\n")))
                    .map(statement -> session.writeTransaction((tx -> tx.run(statement))).consume().counters())
                    .reduce(InternalSummaryCounters.EMPTY_STATS, (x, y) -> new InternalSummaryCounters(x.nodesCreated() + y.nodesCreated(),
                            x.nodesDeleted() + y.nodesDeleted(),
                            x.relationshipsCreated() + y.relationshipsCreated(),
                            x.relationshipsDeleted() + y.relationshipsDeleted(),
                            x.propertiesSet() + y.propertiesSet(),
                            x.labelsAdded() + y.labelsAdded(),
                            x.labelsRemoved() + y.labelsRemoved(),
                            x.indexesAdded() + y.indexesAdded(),
                            x.indexesRemoved() + y.indexesRemoved(),
                            x.constraintsAdded() + y.constraintsAdded(),
                            x.constraintsRemoved() + y.constraintsRemoved()));
            return Stream.of(new MapResult(JsonUtil.OBJECT_MAPPER.convertValue(counter, Map.class)));
        } catch (Exception e) {
            throw new RuntimeException("It's not possible to create a connection due to: " + e.getMessage());
        }
    }

    private StatementResult runStatement(@Name("kernelTransaction") String statement, Session session, Map<String, Object> finalParams, boolean read) {
        if (read) return session.readTransaction((Transaction tx) -> tx.run(statement, finalParams));
        else return session.writeTransaction((Transaction tx) -> tx.run(statement, finalParams));
    }

    private Stream<RowResult> getRowResultStream(boolean virtual, Session session, Map<String, Object> params, String statement, boolean read) {
        Map<Long, Object> nodesCache = new HashMap<>();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(runStatement(statement, session, params, read), 0), true)
                .map(record -> new RowResult(recordAsMap(virtual, nodesCache, record)));
    }

    private Object toNode(Object value, boolean virtual, Map<Long, Object> nodesCache) {
        Value internalValue = ((InternalEntity) value).asValue();
        Node node = internalValue.asNode();
        if (virtual) {
            List<Label> labels = new ArrayList<>();
            node.labels().forEach(l -> labels.add(Label.label(l)));
            VirtualNode virtualNode = new VirtualNode(node.id(), labels.toArray(new Label[0]), node.asMap(), db);
            nodesCache.put(node.id(), virtualNode);
            return virtualNode;
        } else
            return Util.map("entityType", internalValue.type().name(), "labels", node.labels(), "id", node.id(), "properties", node.asMap());
    }

    private Object toRelationship(Object value, boolean virtual, Map<Long, Object> nodesCache) {
        Value internalValue = ((InternalEntity) value).asValue();
        Relationship relationship = internalValue.asRelationship();
        if (virtual) {
            VirtualNode start = (VirtualNode) nodesCache.getOrDefault(relationship.startNodeId(), new VirtualNode(relationship.startNodeId(), db));
            VirtualNode end = (VirtualNode) nodesCache.getOrDefault(relationship.endNodeId(), new VirtualNode(relationship.endNodeId(), db));
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
