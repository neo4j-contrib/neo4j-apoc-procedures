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
package apoc.uuid;

import apoc.ApocConfig;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.Util;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.ApocConfig.*;
import static apoc.SystemPropertyKeys.*;
import static apoc.SystemLabels.*;
import static apoc.util.SystemDbUtil.getSystemNodes;
import static apoc.util.SystemDbUtil.setLastUpdate;
import static apoc.util.SystemDbUtil.withSystemDb;
import static apoc.uuid.UuidHandler.NOT_ENABLED_ERROR;

public class UUIDHandlerNewProcedures {
    public static boolean isEnabled(String databaseName) {
        String apocUUIDEnabledDb = String.format(ApocConfig.APOC_UUID_ENABLED_DB, databaseName);
        boolean enabled = apocConfig().getBoolean(APOC_UUID_ENABLED, false);
        return apocConfig().getConfig().getBoolean(apocUUIDEnabledDb, enabled);
    }

    public static void checkEnabled(String databaseName) {
        if (!isEnabled(databaseName)) {
            String error = String.format(NOT_ENABLED_ERROR, databaseName);
            throw new RuntimeException(error);
        }
    }

    public static UuidInfo create(String databaseName, String label,  UuidConfig config) {
        final UuidInfo[] result = new UuidInfo[1];

        withSystemDb(sysTx -> {
            Node node = Util.mergeNode(sysTx, SystemLabels.ApocUuid, null,
                    Pair.of(database.name(), databaseName),
                    Pair.of(SystemPropertyKeys.label.name(), label)
            );

            node.setProperty(propertyName.name(), config.getUuidProperty());
            node.setProperty(addToSetLabel.name(), config.isAddToSetLabels());
            node.setProperty(addToExistingNodes.name(), config.isAddToExistingNodes());

            // we'll the return current uuid info
            result[0] = new UuidInfo(node, true);

            setLastUpdate(sysTx, databaseName, ApocUuidMeta);
        });

        return result[0];
    }

    public static UuidInfo drop(String databaseName, String labelName) {
        return withSystemDb(tx -> {
            UuidInfo uuidInfo = getUuidNodes(tx, databaseName, Map.of(SystemPropertyKeys.label.name(), labelName))
                    .stream()
                    .map(node -> {
                        UuidInfo info = new UuidInfo(node);
                        node.delete();
                        return info;
                    })
                    .findAny()
                    .orElse(null);

            setLastUpdate(tx, databaseName, ApocUuidMeta);

            return uuidInfo;
        });
    }

    public static List<UuidInfo> dropAll(String databaseName) {
        return withSystemDb(tx -> {
            List<UuidInfo> previous = getUuidNodes(tx, databaseName)
                    .stream()
                    .map(node -> {
                        // we'll return previous uuid info
                        UuidInfo info = new UuidInfo(node);
                        node.delete();
                        return info;
                    })
                    .collect(Collectors.toList());

            setLastUpdate(tx, databaseName, ApocUuidMeta);

            return previous;
        });
    }

    public static ResourceIterator<Node> getUuidNodes(Transaction tx, String databaseName) {
        return getUuidNodes(tx, databaseName, null);
    }

    public static ResourceIterator<Node> getUuidNodes(Transaction tx, String databaseName, Map<String, Object> props) {
        return getSystemNodes(tx, databaseName, SystemLabels.ApocUuid, props);
    }

    public static void createConstraintUuid(Transaction tx, String label, String propertyName) {
        tx.execute(
                String.format("CREATE CONSTRAINT IF NOT EXISTS FOR (n:%s) REQUIRE (n.%s) IS UNIQUE",
                        Util.quote(label),
                        Util.quote(propertyName))
        );
    }
}
