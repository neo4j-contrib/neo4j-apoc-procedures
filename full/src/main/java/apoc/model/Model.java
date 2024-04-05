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
package apoc.model;

import static apoc.load.util.JdbcUtil.getConnection;
import static apoc.load.util.JdbcUtil.getUrlOrKey;
import static apoc.util.Util.map;

import apoc.Extended;
import apoc.load.util.LoadJdbcConfig;
import apoc.result.VirtualNode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;
import org.neo4j.procedure.Procedure;
import schemacrawler.schema.*;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaCrawlerOptionsBuilder;
import schemacrawler.tools.utility.SchemaCrawlerUtility;

@Extended
public class Model {

    @Context
    public Transaction tx;

    private Node createNode(String label, String name, boolean virtual) {
        if (virtual) {
            return new VirtualNode(new Label[] {Label.label(label)}, map("name", name));
        } else {
            Node node = tx.createNode();
            node.addLabel(Label.label(label));
            node.setProperty("name", name);
            return node;
        }
    }

    public static class DatabaseModel {
        public final List<Node> nodes = new ArrayList<>();
        public final List<Relationship> relationships = new ArrayList<>();

        public Node add(Node node) {
            nodes.add(node);
            return node;
        }

        public Relationship add(Relationship relationship) {
            this.relationships.add(relationship);
            return relationship;
        }
    }

    @Procedure(mode = Mode.WRITE)
    @Description(
            "apoc.model.jdbc('key or url', {schema:'<schema>', write: <true/false>, filters: { tables:[], views: [], columns: []}) YIELD nodes, relationships - load schema from relational database")
    public Stream<DatabaseModel> jdbc(
            @Name("jdbc") String urlOrKey, @Name(value = "config", defaultValue = "{}") Map<String, Object> config)
            throws Exception {
        String url = getUrlOrKey(urlOrKey);

        SchemaCrawlerOptions options = SchemaCrawlerOptionsBuilder.newSchemaCrawlerOptions();

        Catalog catalog = SchemaCrawlerUtility.getCatalog(getConnection(url, new LoadJdbcConfig(config)), options);

        DatabaseModel databaseModel = new DatabaseModel();

        ModelConfig modelConfig = new ModelConfig(config != null ? config : Collections.emptyMap());
        boolean virtual = !modelConfig.isWrite();

        for (final Schema schema : catalog.getSchemas()) {
            if (!modelConfig.getSchema().equalsIgnoreCase(schema.getFullName())) {
                continue;
            }
            Node schemaNode = databaseModel.add(createNode("Schema", schema.getFullName(), virtual));

            for (final Table table : catalog.getTables(schema)) {
                boolean isView = table instanceof View;
                List<String> patterns = isView ? modelConfig.getViews() : modelConfig.getTables();
                boolean matchTableView = patterns.isEmpty()
                        ? true
                        : patterns.stream().anyMatch(p -> table.getName().matches(p));
                if (!matchTableView) {
                    continue;
                }
                Node tableNode = databaseModel.add(createNode("Table", table.getName(), virtual));
                databaseModel.add(tableNode.createRelationshipTo(schemaNode, RelationshipType.withName("IN_SCHEMA")));
                if (isView) tableNode.addLabel(Label.label("View"));

                for (final Column column : table.getColumns()) {
                    boolean matchColumn = modelConfig.getColumns().isEmpty()
                            ? true
                            : modelConfig.getColumns().stream()
                                    .anyMatch(p -> column.getName().matches(p));
                    if (!matchColumn) {
                        continue;
                    }
                    Node columnNode = databaseModel.add(createNode("Column", column.getName(), virtual));
                    columnNode.setProperty("type", column.getColumnDataType().getDatabaseSpecificTypeName());
                    databaseModel.add(
                            columnNode.createRelationshipTo(tableNode, RelationshipType.withName("IN_TABLE")));
                    if (column.isPartOfPrimaryKey()) columnNode.addLabel(Label.label("PrimaryKey"));
                    if (column.isPartOfForeignKey()) columnNode.addLabel(Label.label("ForeignKey"));
                    if (column.isPartOfUniqueIndex()) columnNode.setProperty("unique", true);
                }
            }
        }
        return Stream.of(databaseModel);
    }
}
