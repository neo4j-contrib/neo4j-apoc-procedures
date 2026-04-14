package apoc.custom;

import apoc.ExtendedSystemLabels;
import apoc.ExtendedSystemPropertyKeys;
import apoc.SystemPropertyKeys;
import apoc.util.SystemDbUtil;
import apoc.util.Util;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.ExtendedSystemLabels.ApocCypherProcedures;
import static apoc.ExtendedSystemLabels.ApocCypherProceduresMeta;
import static apoc.ExtendedSystemLabels.Function;
import static apoc.ExtendedSystemLabels.Procedure;
import static apoc.ExtendedSystemPropertyKeys.description;
import static apoc.ExtendedSystemPropertyKeys.inputs;
import static apoc.ExtendedSystemPropertyKeys.mode;
import static apoc.ExtendedSystemPropertyKeys.output;
import static apoc.ExtendedSystemPropertyKeys.outputs;
import static apoc.ExtendedSystemPropertyKeys.prefix;
import static apoc.SystemPropertyKeys.database;
import static apoc.SystemPropertyKeys.name;
import static apoc.custom.CypherNewProcedures.ALL_DATABASES;
import static apoc.custom.CypherProceduresUtil.qualifiedName;
import static apoc.util.SystemDbUtil.getSystemNodes;
import static apoc.util.SystemDbUtil.withSystemDb;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

public class CypherHandlerNewProcedure {
    private static final String ERROR_DIFFERENT_DB =
            "%5$s `%1$s` is registered in another db (`%2$s`), it's not possible to register a `%1$s` with the same name in a different db.\n" +
                    "You have to remove it via `%3$s` or different db or install it globally by putting null as the 3rd parameter, e.g. `%4$s`";

    public static void checkIfProcOrFuncExistsInAnotherDbAndDbNameIsNotAll(
            Transaction tx,
            QualifiedName qualifiedName,
            String targetDatabaseName,
            ExtendedSystemLabels procOrFunLabel) {
        Node existingNode = tx.findNodes(ApocCypherProcedures,
                        name.name(), qualifiedName.name(),
                        prefix.name(), qualifiedName.namespace()
                ).stream()
                .filter(n -> n.hasLabel(procOrFunLabel))
                .findFirst()
                .orElse(null);

        if (existingNode == null) {
            return;
        }
        
        String existingDatabaseName = (String) existingNode.getProperty(SystemPropertyKeys.database.name());

        boolean isSameDb = targetDatabaseName.equals(existingDatabaseName);
        
        if (isSameDb) {
            return;
        }
        
        if (targetDatabaseName.equals(ALL_DATABASES)) {
            existingNode.delete();
        } else {
            throw new RuntimeException(
                    String.format(ERROR_DIFFERENT_DB,
                            String.join(".", qualifiedName.namespace()),
                            existingDatabaseName,
                            procOrFunLabel.equals(ExtendedSystemLabels.Procedure)
                                    ? "CALL apoc.custom.dropProcedure('" + qualifiedName.name() + "', '" + existingDatabaseName + "')"
                                    : "CALL apoc.custom.dropFunction('" + qualifiedName.name() + "', '" + existingDatabaseName + "')",
                            procOrFunLabel.equals(ExtendedSystemLabels.Procedure)
                                    ? "CALL apoc.custom.installProcedure('<procedure signature>', '<procedure statement>', null)"
                                    : "CALL apoc.custom.installFunction('<function signature>', '<function statement>', null)",
                            procOrFunLabel
                    )
            );
        }
    }
    
    public static void installProcedure(String databaseName, ProcedureSignature signature, String statement) {
        withSystemDb(tx -> {
            checkIfProcOrFuncExistsInAnotherDbAndDbNameIsNotAll(tx, signature.name(), databaseName, Procedure);
            
            Node node = Util.mergeNode(tx, ApocCypherProcedures, Procedure,
                    Pair.of(database.name(), databaseName),
                    Pair.of(name.name(), signature.name().name()),
                    Pair.of(prefix.name(), signature.name().namespace())
            );
            node.setProperty(description.name(), signature.description().orElse(null));
            node.setProperty(SystemPropertyKeys.statement.name(), statement);
            node.setProperty(inputs.name(), serializeSignatures(signature.inputSignature()));
            node.setProperty(outputs.name(), serializeSignatures(signature.outputSignature()));
            node.setProperty(mode.name(), signature.mode().name());

            setLastUpdate(tx, databaseName);
        });
    }

    public static void installFunction(String databaseName, UserFunctionSignature signature, String statement, boolean forceSingle) {
        withSystemDb(tx -> {
            checkIfProcOrFuncExistsInAnotherDbAndDbNameIsNotAll(tx, signature.name(), databaseName, Function);
            
            Node node = Util.mergeNode(tx, ApocCypherProcedures, Function,
                    Pair.of(database.name(), databaseName),
                    Pair.of(name.name(), signature.name().name()),
                    Pair.of(prefix.name(), signature.name().namespace())
            );
            node.setProperty(description.name(), signature.description().orElse(null));
            node.setProperty(SystemPropertyKeys.statement.name(), statement);
            node.setProperty(inputs.name(), serializeSignatures(signature.inputSignature()));
            node.setProperty(output.name(), signature.outputType().toString());
            node.setProperty(ExtendedSystemPropertyKeys.forceSingle.name(), forceSingle);

            setLastUpdate(tx, databaseName);
        });
    }

    public static List<CustomProcedureInfo> dropAll(String databaseName) {
        return withSystemDb(tx -> {
            List<CustomProcedureInfo> previous = getCustomNodes(databaseName, tx)
                    .stream()
                    .map(node -> {
                        // we'll return previous uuid info
                        CustomProcedureInfo info = CustomProcedureInfo.fromNode(node);
                        node.delete();
                        return info;
                    })
                    .sorted(sortNodes())
                    .collect(Collectors.toList());

            setLastUpdate(tx, databaseName);
            return previous;
        });
    }

    public static Stream<CustomProcedureInfo> show(String databaseName, Transaction tx) {
        return getCustomNodes(databaseName, tx).stream()
                .map(CustomProcedureInfo::fromNode)
                .sorted(sortNodes());
    }

    private static Comparator<CustomProcedureInfo> sortNodes() {
        return Comparator.comparing((CustomProcedureInfo i) -> i.name)
                .thenComparing(i -> i.type);
    }

    public static void dropFunction(String databaseName, String name) {
        withSystemDb(tx -> {
            QualifiedName qName = qualifiedName(name);
            getCustomNodes(databaseName, tx,
                    Map.of(SystemPropertyKeys.name.name(), qName.name(),
                            prefix.name(), qName.namespace())
            )
                    .stream()
                    .filter(n -> n.hasLabel(Function)).forEach(node -> {
                        node.delete();
                        setLastUpdate(tx, databaseName);
                    });
        });
    }

    public static void dropProcedure(String databaseName, String name) {
        withSystemDb(tx -> {
            QualifiedName qName = qualifiedName(name);
            getCustomNodes(databaseName, tx,
                    Map.of(SystemPropertyKeys.database.name(), databaseName,
                            SystemPropertyKeys.name.name(), qName.name(),
                            prefix.name(), qName.namespace())
            ).stream().filter(n -> n.hasLabel(Procedure)).forEach(node -> {
                node.delete();
                setLastUpdate(tx, databaseName);
            });
        });
    }

    public static ResourceIterator<Node> getCustomNodes(String databaseName, Transaction tx) {
        return getCustomNodes(databaseName, tx, null);
    }

    public static ResourceIterator<Node> getCustomNodes(String databaseName, Transaction tx, Map<String, Object> props) {
        return getSystemNodes(tx, databaseName, ApocCypherProcedures, props);
    }

    public static String serializeSignatures(List<FieldSignature> signatures) {
        List<Map<String, Object>> mapped = signatures.stream().map(fs -> {
            final Map<String, Object> map = map(
                    "name", fs.name(),
                    "type", fs.neo4jType().toString()
            );
            fs.defaultValue().map(defVal -> map.put("default", defVal.value()));
            return map;
        }).collect(Collectors.toList());
        return Util.toJson(mapped);
    }

    private static void setLastUpdate(Transaction tx, String databaseName) {
        SystemDbUtil.setLastUpdate(tx, databaseName, ApocCypherProceduresMeta);
    }

}