package apoc.bolt;

import apoc.result.RowResult;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.MissingDependencyException;
import apoc.util.UriResolver;
import apoc.util.Util;
import apoc.util.collection.Iterators;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalEntity;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.summary.SummaryCounters;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.util.MapUtil.map;

public class BoltConnection {
    private final BoltConfig config;
    private final UriResolver resolver;

    public BoltConnection(BoltConfig config, UriResolver resolver) {
        this.config = config;
        this.resolver = resolver;
    }
    
    public static BoltConnection from(Map<String, Object> config, String url) throws URISyntaxException {
        try {
            final UriResolver resolver = new UriResolver(url, "bolt");
            resolver.initialize();
            return new BoltConnection(new BoltConfig(config), resolver);
        } catch (NoClassDefFoundError e) {
            throw new MissingDependencyException("Cannot find the needed jar into the plugins folder in order to use . \n" +
                    "Please see the documentation: https://neo4j.com/labs/apoc/4.4/database-integration/bolt-neo4j/#_install_dependencies");
        }
    }

    // methods from Bolt.java
    public Stream<RowResult> loadFromSession(String statement, Map<String, Object> params) {
        return withDriverAndSession(session -> {
            if (config.isAddStatistics()) {
                Result statementResult = session.run(statement, params);
                SummaryCounters counters = statementResult.consume().counters();
                return Stream.of(new RowResult(toMap(counters)));
            } else
                return getRowResultStream(config.isVirtual(), session, params, statement);
        });
    }

    public Stream<RowResult> loadFromLocal(String localStatement, String remoteStatement, GraphDatabaseService db) {
        return withDriverAndSession(session -> {
            try (org.neo4j.graphdb.Transaction tx = db.beginTx()) {
                final org.neo4j.graphdb.Result localResult = tx.execute(localStatement, config.getLocalParams());
                String withColumns = "WITH " + localResult.columns().stream()
                        .map(c -> "$" + c + " AS " + c)
                        .collect(Collectors.joining(", ")) + "\n";
                Map<Long, Object> nodesCache = new HashMap<>();
                List<RowResult> response = new ArrayList<>();
                while (localResult.hasNext()) {
                    final Result statementResult;
                    Map<String, Object> row = localResult.next();
                    if (config.isStreamStatements()) {
                        final String statement = (String) row.get("statement");
                        if (StringUtils.isBlank(statement)) continue;
                        final Map<String, Object> params = Collections.singletonMap("params", row.getOrDefault("params", Collections.emptyMap()));
                        statementResult = session.run(statement, params);
                    } else {
                        statementResult = session.run(withColumns + remoteStatement, row);
                    }
                    if (config.isStreamStatements()) {
                        response.add(new RowResult(toMap(statementResult.consume().counters())));
                    } else {
                        response.addAll(
                                statementResult.stream()
                                        .map(record -> buildRowResult(record, nodesCache, config.isVirtual()))
                                        .collect(Collectors.toList())
                        );
                    }
                }
                return response.stream();
            }
        });
    }
    
    private <T> Stream<T> withDriverAndSession(Function<Session, Stream<T>> funSession) {
        return withDriver(driver -> withSession(driver,funSession));
    }
    
    private <T> Stream<T> withDriver(Function<Driver, Stream<T>> function) {
        Driver driver = GraphDatabase.driver(resolver.getConfiguredUri(), resolver.getToken(), config.getDriverConfig());
        return function.apply(driver).onClose(driver::close);
    }

    private <T> Stream<T> withSession(Driver driver, Function<Session, Stream<T>> function) {
        Session session = driver.session(config.getSessionConfig());
        return function.apply(session).onClose(session::close);
    }

    private <T> Stream<T> withTransaction(Session session, Function<Transaction, Stream<T>> function) {
        Transaction transaction = session.beginTransaction();
        return function.apply(transaction).onClose(transaction::commit).onClose(transaction::close);
    }

    private RowResult buildRowResult(Record record, Map<Long,Object> nodesCache, boolean virtual) {
        return new RowResult(record.asMap(value -> {
            Object entity = value.asObject();
            if (entity instanceof Node) return toNode(entity, virtual, nodesCache);
            if (entity instanceof Relationship) return toRelationship(entity, virtual, nodesCache);
            if (entity instanceof Path) return toPath(entity, virtual, nodesCache);
            return entity;
        }));
    }

    private Stream<RowResult> getRowResultStream(boolean virtual, Session session, Map<String, Object> params, String statement) {
        Map<Long, Object> nodesCache = new HashMap<>();

        return withTransaction(session, tx -> {
            ClosedAwareDelegatingIterator<Record> iterator = new ClosedAwareDelegatingIterator(tx.run(statement, params));
            return Iterators.stream(iterator).map(record -> buildRowResult(record, nodesCache, virtual));
        });
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
}
