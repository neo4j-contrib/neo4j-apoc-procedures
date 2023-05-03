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
package apoc.systemdb.metadata;

import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.export.util.ProgressReporter;
import apoc.systemdb.SystemDbConfig;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.List;
import java.util.Optional;


public interface ExportMetadata {

    enum Type {
        CypherProcedure(new ExportProcedure()),
        CypherFunction(new ExportFunction()),
        Uuid(new ExportUuid()),
        Trigger(new ExportTrigger()),
        DataVirtualizationCatalog(new ExportDataVirtualization());

        private final ExportMetadata exportMetadata;

        Type(ExportMetadata exportMetadata) {
            this.exportMetadata = exportMetadata;
        }

        public List<Pair<String, String>> export(Node node, ProgressReporter progressReporter) {
            return exportMetadata.export(node, progressReporter);
        }

        public static Optional<Type> from(Label label, SystemDbConfig config) {
            final String name = label.name();
            if (name.equalsIgnoreCase(SystemLabels.Procedure.name())) {
                return get(CypherProcedure, config);
            } else if(name.equalsIgnoreCase(SystemLabels.Function.name())) {
                return get(CypherFunction, config);
            } else if(name.equalsIgnoreCase(SystemLabels.ApocTrigger.name())) {
                return get(Trigger, config);
            } else if(name.equalsIgnoreCase(SystemLabels.ApocUuid.name())) {
                return get(Uuid, config);
            } else if(name.equalsIgnoreCase(SystemLabels.DataVirtualizationCatalog.name())) {
                return get(DataVirtualizationCatalog, config);
            }
            return Optional.empty();
        }

        private static Optional<Type> get(Type cypherProcedure, SystemDbConfig config) {
            return config.getFeatures().contains(cypherProcedure.name())
                    ? Optional.of(cypherProcedure)
                    : Optional.empty();
        }
    }

    List<Pair<String, String>> export(Node node, ProgressReporter progressReporter);

    default String getFileName(Node node, String prefix) {
        // we create a file featureName.dbName because there could be features coming from different databases
        String dbName = (String) node.getProperty(SystemPropertyKeys.database.name(), null);
        dbName = StringUtils.isEmpty(dbName) ? StringUtils.EMPTY : "." + dbName;
        return prefix + dbName;
    }
}