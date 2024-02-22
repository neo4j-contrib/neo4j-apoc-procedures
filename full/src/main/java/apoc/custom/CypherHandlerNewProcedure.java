/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.custom;

import static apoc.SystemLabels.ApocCypherProcedures;
import static apoc.SystemLabels.ApocCypherProceduresMeta;
import static apoc.SystemLabels.Function;
import static apoc.SystemLabels.Procedure;
import static apoc.SystemPropertyKeys.database;
import static apoc.SystemPropertyKeys.description;
import static apoc.SystemPropertyKeys.inputs;
import static apoc.SystemPropertyKeys.mode;
import static apoc.SystemPropertyKeys.name;
import static apoc.SystemPropertyKeys.output;
import static apoc.SystemPropertyKeys.outputs;
import static apoc.SystemPropertyKeys.prefix;
import static apoc.custom.CypherProceduresUtil.qualifiedName;
import static apoc.util.SystemDbUtil.getSystemNodes;
import static apoc.util.SystemDbUtil.withSystemDb;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

import apoc.SystemPropertyKeys;
import apoc.util.SystemDbUtil;
import apoc.util.Util;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;

public class CypherHandlerNewProcedure {

    public static void installProcedure(String databaseName, ProcedureSignature signature, String statement) {
        withSystemDb(tx -> {
            Node node = Util.mergeNode(
                    tx,
                    ApocCypherProcedures,
                    Procedure,
                    Pair.of(database.name(), databaseName),
                    Pair.of(name.name(), signature.name().name()),
                    Pair.of(prefix.name(), signature.name().namespace()));
            node.setProperty(description.name(), signature.description().orElse(null));
            node.setProperty(SystemPropertyKeys.statement.name(), statement);
            node.setProperty(inputs.name(), serializeSignatures(signature.inputSignature()));
            node.setProperty(outputs.name(), serializeSignatures(signature.outputSignature()));
            node.setProperty(mode.name(), signature.mode().name());

            setLastUpdate(tx, databaseName);
        });
    }

    public static void installFunction(
            String databaseName, UserFunctionSignature signature, String statement, boolean forceSingle) {
        withSystemDb(tx -> {
            Node node = Util.mergeNode(
                    tx,
                    ApocCypherProcedures,
                    Function,
                    Pair.of(database.name(), databaseName),
                    Pair.of(name.name(), signature.name().name()),
                    Pair.of(prefix.name(), signature.name().namespace()));
            node.setProperty(description.name(), signature.description().orElse(null));
            node.setProperty(SystemPropertyKeys.statement.name(), statement);
            node.setProperty(inputs.name(), serializeSignatures(signature.inputSignature()));
            node.setProperty(output.name(), signature.outputType().toString());
            node.setProperty(SystemPropertyKeys.forceSingle.name(), forceSingle);

            setLastUpdate(tx, databaseName);
        });
    }

    public static List<CustomProcedureInfo> dropAll(String databaseName) {
        return withSystemDb(tx -> {
            List<CustomProcedureInfo> previous = getCustomNodes(databaseName, tx).stream()
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
        return Comparator.comparing((CustomProcedureInfo i) -> i.name).thenComparing(i -> i.type);
    }

    public static void dropFunction(String databaseName, String name) {
        withSystemDb(tx -> {
            QualifiedName qName = qualifiedName(name);
            getCustomNodes(
                            databaseName,
                            tx,
                            Map.of(SystemPropertyKeys.name.name(), qName.name(), prefix.name(), qName.namespace()))
                    .stream()
                    .filter(n -> n.hasLabel(Function))
                    .forEach(node -> {
                        node.delete();
                        setLastUpdate(tx, databaseName);
                    });
        });
    }

    public static void dropProcedure(String databaseName, String name) {
        withSystemDb(tx -> {
            QualifiedName qName = qualifiedName(name);
            getCustomNodes(
                            databaseName,
                            tx,
                            Map.of(
                                    SystemPropertyKeys.database.name(),
                                    databaseName,
                                    SystemPropertyKeys.name.name(),
                                    qName.name(),
                                    prefix.name(),
                                    qName.namespace()))
                    .stream()
                    .filter(n -> n.hasLabel(Procedure))
                    .forEach(node -> {
                        node.delete();
                        setLastUpdate(tx, databaseName);
                    });
        });
    }

    public static ResourceIterator<Node> getCustomNodes(String databaseName, Transaction tx) {
        return getCustomNodes(databaseName, tx, null);
    }

    public static ResourceIterator<Node> getCustomNodes(
            String databaseName, Transaction tx, Map<String, Object> props) {
        return getSystemNodes(tx, databaseName, ApocCypherProcedures, props);
    }

    public static String serializeSignatures(List<FieldSignature> signatures) {
        List<Map<String, Object>> mapped = signatures.stream()
                .map(fs -> {
                    final Map<String, Object> map = map(
                            "name", fs.name(),
                            "type", fs.neo4jType().toString());
                    fs.defaultValue().map(defVal -> map.put("default", defVal.value()));
                    return map;
                })
                .collect(Collectors.toList());
        return Util.toJson(mapped);
    }

    private static void setLastUpdate(Transaction tx, String databaseName) {
        SystemDbUtil.setLastUpdate(tx, databaseName, ApocCypherProceduresMeta);
    }
}
