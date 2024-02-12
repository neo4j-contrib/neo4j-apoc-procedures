package apoc.bolt;

import apoc.result.RowResult;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
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
        final UriResolver resolver = new UriResolver(url, "bolt");
        resolver.initialize();
        return new BoltConnection(new BoltConfig(config), resolver);
    }

    // methods from Bolt.java
    public Stream<RowResult> loadFromSession(String statement, Map<String, Object> params) {
        return withDriverAndSession(session -> {
            if (config.isAddStatistics()) {
                Result statementResult = session.run(statement, params);
                SummaryCounters counters = statementResult.consume().counters();
                return Stream.of(new RowResult(toMap(counters)));
            } else
                return getRowResultStream(session, params, statement);
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
                                        .map(record -> buildRowResult(record, nodesCache))
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

    private RowResult buildRowResult(Record record, Map<Long, Object> nodesCache) {
        return new RowResult(record.asMap(value -> convertRecursive(value, nodesCache)));
    }

    private Object convertRecursive(Object entity, Map<Long, Object> nodesCache) {
        if (entity instanceof Value) return convertRecursive(((Value) entity).asObject(), nodesCache);
        if (entity instanceof Node) return toNode(entity, nodesCache);
        if (entity instanceof Relationship) return toRelationship(entity, nodesCache);
        if (entity instanceof Path) return toPath(entity, nodesCache);
        if (entity instanceof Collection) return toCollection((Collection) entity, nodesCache);
        if (entity instanceof Map) return toMap((Map<String, Object>) entity, nodesCache);
        return entity;
    }

    private Object toMap(Map<String, Object> entity, Map<Long, Object> nodeCache) {
        return entity.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry(entry.getKey(), convertRecursive(entry.getValue(), nodeCache)))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }

    private Object toCollection(Collection entity, Map<Long, Object> nodeCache) {
        return entity.stream().map(elem -> convertRecursive(elem, nodeCache)).collect(Collectors.toList());
    }

    private Stream<RowResult> getRowResultStream(Session session, Map<String, Object> params, String statement) {
        Map<Long, Object> nodesCache = new HashMap<>();

        return withTransaction(session, tx -> {
            ClosedAwareDelegatingIterator<Record> iterator = new ClosedAwareDelegatingIterator(tx.run(statement, params));
            return Iterators.stream(iterator).map(record -> buildRowResult(record, nodesCache));
        });
    }

    private Object toNode(Object value, Map<Long, Object> nodesCache) {
        Value internalValue = ((InternalEntity) value).asValue();
        Node node = internalValue.asNode();
        if (config.isVirtual()) {
            List<Label> labels = new ArrayList<>();
            node.labels().forEach(l -> labels.add(Label.label(l)));
            VirtualNode virtualNode = new VirtualNode(node.id(), labels.toArray(new Label[0]), node.asMap());
            nodesCache.put(node.id(), virtualNode);
            return virtualNode;
        } else
            return Util.map("entityType", internalValue.type().name(), "labels", node.labels(), "id", node.id(), "properties", node.asMap());
    }

    private Object toRelationship(Object value, Map<Long, Object> nodesCache) {
        Value internalValue = ((InternalEntity) value).asValue();
        Relationship relationship = internalValue.asRelationship();
        if (config.isVirtual()) {
            VirtualNode start = (VirtualNode) nodesCache.getOrDefault(relationship.startNodeId(), new VirtualNode(relationship.startNodeId()));
            VirtualNode end = (VirtualNode) nodesCache.getOrDefault(relationship.endNodeId(), new VirtualNode(relationship.endNodeId()));
            VirtualRelationship virtualRelationship = new VirtualRelationship(relationship.id(), start, end, RelationshipType.withName(relationship.type()), relationship.asMap());
            return virtualRelationship;
        } else
            return Util.map("entityType", internalValue.type().name(), "type", relationship.type(), "id", relationship.id(), "start", relationship.startNodeId(), "end", relationship.endNodeId(), "properties", relationship.asMap());
    }

    private Object toPath(Object value, Map<Long, Object> nodesCache) {
        List<Object> entityList = new LinkedList<>();
        Value internalValue = ((InternalPath) value).asValue();
        internalValue.asPath().forEach(p -> {
            entityList.add(toNode(p.start(), nodesCache));
            entityList.add(toRelationship(p.relationship(), nodesCache));
            entityList.add(toNode(p.end(), nodesCache));
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
