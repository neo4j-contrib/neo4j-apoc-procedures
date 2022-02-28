package apoc.bolt;

import apoc.meta.Meta;
import apoc.result.RowResult;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.MissingDependencyException;
import apoc.util.UriResolver;
import apoc.util.Util;
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
import org.neo4j.internal.helpers.collection.Iterators;

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
import java.util.stream.StreamSupport;

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
                Map<Long, VirtualNode> nodesCache = new HashMap<>();
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
                        final String query = withColumns + remoteStatement;
                        statementResult = session.run(query, row);
                    }
                    if (config.isStreamStatements()) {
                        response.add(new RowResult(toMap(statementResult.consume().counters())));
                    } else {
                        response.addAll(
                                statementResult.stream()
                                        .flatMap(record -> buildRowResult(session, record, nodesCache))
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

    private Stream<RowResult> buildRowResult(Session session, Record record, Map<Long,VirtualNode> nodesCache) {
        return withTransaction(session, tx -> Stream.of(buildRowResult(tx, record, nodesCache)));
    }
    
    private RowResult buildRowResult(Transaction tx, Record record, Map<Long,VirtualNode> nodesCache) {
        return new RowResult(record.asMap(value -> convert(tx, value, nodesCache)));
    }

    private Object convert(Transaction tx, Object entity, Map<Long, VirtualNode> nodesCache) {
        if (entity instanceof Value) return convert(tx, ((Value) entity).asObject(), nodesCache);
        if (entity instanceof Node) return toNode(entity, nodesCache);
        if (entity instanceof Relationship) return toRelationship(tx, entity, nodesCache);
        if (entity instanceof Path) return toPath(tx, entity, nodesCache);
        if (entity instanceof Collection) return toCollection(tx, (Collection) entity, nodesCache);
        if (entity instanceof Map) return toMap(tx, (Map<String, Object>) entity, nodesCache);
        return entity;
    }

    private Object toMap(Transaction tx, Map<String, Object> entity, Map<Long, VirtualNode> nodeCache) {
        return entity.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry(entry.getKey(), convert(tx, entry.getValue(), nodeCache)))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    }

    private Object toCollection(Transaction tx, Collection entity, Map<Long, VirtualNode> nodeCache) {
        return entity.stream()
                .map(elem -> convert(tx, elem, nodeCache))
                .collect(Collectors.toList());
    }

    private Stream<RowResult> getRowResultStream(Session session, Map<String, Object> params, String statement) {
        Map<Long, VirtualNode> nodesCache = new HashMap<>();

        return withTransaction(session, tx -> {
            ClosedAwareDelegatingIterator<Record> iterator = new ClosedAwareDelegatingIterator(tx.run(statement, params));
            return Iterators.stream(iterator).map(record -> buildRowResult(tx, record, nodesCache));
        });
    }

    private Object toNode(Object value, Map<Long, VirtualNode> nodesCache) {
        Node node;
        if (value instanceof Value) {
            node = ((InternalEntity) value).asValue().asNode();
        } else if (value instanceof Node) {
            node = (Node) value;
        } else {
            throw getUnsupportedConversionException(value);
        }
        if (config.isVirtual()) {
            VirtualNode virtualNode = new VirtualNode(node.id(), getLabelsAsArray(node), node.asMap());
            nodesCache.put(node.id(), virtualNode);
            return virtualNode;
        } else
            return Util.map("entityType", Meta.Types.NODE.name(), "labels", node.labels(), "id", node.id(), "properties", node.asMap());
    }

    private Object toRelationship(Transaction tx, Object value, Map<Long, VirtualNode> nodesCache) {
        Relationship rel;
        if (value instanceof Value) {
            rel = ((InternalEntity) value).asValue().asRelationship();
        } else if (value instanceof Relationship) {
            rel = (Relationship) value;
        } else {
            throw getUnsupportedConversionException(value);
        }
        if (config.isVirtual()) {
            VirtualNode start;
            VirtualNode end;
            final long startId = rel.startNodeId();
            final long endId = rel.endNodeId();
            if (config.isWithRelationshipNodeProperties()) {
                final Function<Long, VirtualNode> retrieveNode = (id) -> {
                    final Node node = tx.run("MATCH (n) WHERE id(n) = $id RETURN n", Map.of("id", id))
                            .single()
                            .get("n")
                            .asNode();
                    return new VirtualNode(node.id(), getLabelsAsArray(node), node.asMap());
                };
                start = nodesCache.computeIfAbsent(startId, retrieveNode);
                end = nodesCache.computeIfAbsent(endId, retrieveNode);
            } else {
                start = nodesCache.getOrDefault(startId, new VirtualNode(startId));
                end = nodesCache.getOrDefault(endId, new VirtualNode(endId));
            }
            return new VirtualRelationship(rel.id(), start, end, RelationshipType.withName(rel.type()), rel.asMap());
        } else
            return Util.map("entityType", Meta.Types.RELATIONSHIP.name(), "type", rel.type(), "id", rel.id(), "start", rel.startNodeId(), "end", rel.endNodeId(), "properties", rel.asMap());
    }

    private Object toPath(Transaction tx, Object value, Map<Long, VirtualNode> nodesCache) {
        List<Object> entityList = new LinkedList<>();
        Value internalValue = ((InternalPath) value).asValue();
        internalValue.asPath().forEach(p -> {
            entityList.add(toNode(p.start(), nodesCache));
            entityList.add(toRelationship(tx, p.relationship(), nodesCache));
            entityList.add(toNode(p.end(), nodesCache));
        });
        return entityList;
    }

    private Label[] getLabelsAsArray(Node node) {
        return StreamSupport.stream(node.labels().spliterator(), false)
                .map(Label::label)
                .toArray(Label[]::new);
    }

    private ClassCastException getUnsupportedConversionException(Object value) {
        return new ClassCastException("Conversion from class " + value.getClass().getName() + " not supported");
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
