package apoc.systemdb;

import apoc.ApocConfig;
import apoc.Description;
import apoc.Extended;
import apoc.export.cypher.ExportFileManager;
import apoc.export.cypher.FileManagerFactory;
import apoc.export.util.ExportConfig;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.result.RowResult;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.systemdb.metadata.ExportMetadata;
import apoc.util.collection.Iterables;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


@Extended
public class SystemDb {
    public static final String REMOTE_SENSITIVE_PROP = "password";
    public static final String USER_SENSITIVE_PROP = "credentials";

    @Context
    public ApocConfig apocConfig;

    @Context
    public GraphDatabaseService db;

    public static class NodesAndRelationshipsResult {
        public List<Node> nodes;
        public List<Relationship> relationships;

        public NodesAndRelationshipsResult(List<Node> nodes, List<Relationship> relationships) {
            this.nodes = nodes;
            this.relationships = relationships;
        }
    }

    @Admin
    @Procedure(name = "apoc.systemdb.export.metadata")
    @Description("apoc.systemdb.export.metadata($conf) - export the apoc feature saved in system db (that is: customProcedures, triggers, uuids, and dvCatalogs) in multiple files called <FILE_NAME>.<FEATURE_NAME>.<DB_NAME>.cypher")
    public Stream<ProgressInfo> metadata(@Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        final SystemDbConfig conf = new SystemDbConfig(config);
        final String fileName = conf.getFileName();
        apocConfig.checkWriteAllowed(null, fileName);

        ProgressInfo progressInfo = new ProgressInfo(fileName, null, "cypher");
        ProgressReporter progressReporter = new ProgressReporter(null, null, progressInfo);
        ExportFileManager cypherFileManager = FileManagerFactory.createFileManager(fileName + ".cypher", true, ExportConfig.EMPTY);
        withSystemDbTransaction(tx -> {
            tx.getAllNodes()
                    .stream()
                    .flatMap(node -> StreamSupport.stream(node.getLabels().spliterator(), false)
                            .map(label -> ExportMetadata.Type.from(label, conf))
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .flatMap(type -> type.export(node, progressReporter).stream())
                    )
                    .collect(Collectors.groupingBy(Pair::getLeft, Collectors.toList()))
                    .forEach((fileNameSuffix, fileContent) -> {
                        try(PrintWriter writer = cypherFileManager.getPrintWriter(fileNameSuffix)) {
                            final String stringStatement = fileContent.stream()
                                    .map(Pair::getRight)
                                    .collect(Collectors.joining("\n"));
                            writer.write(stringStatement);
                        }
                    });
            return null;
        });

        progressReporter.done();
        return progressReporter.stream();
    }

    @Admin
    @Procedure
    public Stream<NodesAndRelationshipsResult> graph() {
        return withSystemDbTransaction(tx -> {
            Map<Long, Node> virtualNodes = new HashMap<>();
            for (Node node: tx.getAllNodes())  {
                final Map<String, Object> props = node.getAllProperties();
                props.keySet()
                        .removeAll(Set.of(REMOTE_SENSITIVE_PROP, USER_SENSITIVE_PROP));
                
                virtualNodes.put(-node.getId(), new VirtualNode(-node.getId(), Iterables.asArray(Label.class, node.getLabels()), props));
            }

            List<Relationship> relationships = tx.getAllRelationships().stream().map(rel -> new VirtualRelationship(
                    -rel.getId(),
                    virtualNodes.get(-rel.getStartNodeId()),
                    virtualNodes.get(-rel.getEndNodeId()),
                    rel.getType(),
                    rel.getAllProperties())).collect(Collectors.toList()
            );
            return Stream.of(new NodesAndRelationshipsResult(Iterables.asList(virtualNodes.values()), relationships) );
        });
    }

    @Admin
    @Procedure
    public Stream<RowResult> execute(@Name("DDL commands, either a string or a list of strings") Object ddlStringOrList, @Name(value="params", defaultValue = "{}") Map<String ,Object> params) {
        List<String> commands;
        if (ddlStringOrList instanceof String) {
            commands = Collections.singletonList((String)ddlStringOrList);
        } else if (ddlStringOrList instanceof List) {
            commands = (List<String>) ddlStringOrList;
        } else {
            throw new IllegalArgumentException("don't know how to handle " + ddlStringOrList + ". Supply either a string or a list of strings");
        }

        Transaction tx = apocConfig.getSystemDb().beginTx();  // we can't use try-with-resources otherwise tx gets closed too early
        return commands.stream().flatMap(command -> tx.execute(command, params).stream().map(RowResult::new)).onClose(() -> {
            boolean isOpen = ((TransactionImpl) tx).isOpen(); // no other way to check if a tx is still open
            if (isOpen) {
                tx.commit();
            }
            tx.close();
        });
    }

    private <T> T withSystemDbTransaction(Function<Transaction, T> function) {
        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            T result = function.apply(tx);
            tx.commit();
            return result;
        }
    }
}
