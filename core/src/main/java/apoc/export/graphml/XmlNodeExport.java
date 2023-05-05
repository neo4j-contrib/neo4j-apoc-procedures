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
package apoc.export.graphml;

import apoc.export.util.ExportConfig;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import static apoc.export.util.ExportConfig.NodeConfig;

public class XmlNodeExport {
    
    public interface ExportNode {
        NodeConfig getNodeConfig(ExportConfig config);
        NodeConfig getNodeConfigReader(XmlGraphMLReader reader);
        Node getNode(Relationship rel);
    }

    enum NodeType {
        SOURCE("source", new ExportNode() {
            @Override
            public ExportConfig.NodeConfig getNodeConfig(ExportConfig config) {
                return config.getSource();
            }

            @Override
            public Node getNode(Relationship rel) {
                return rel.getStartNode();
            }

            @Override
            public NodeConfig getNodeConfigReader(XmlGraphMLReader reader) {
                return reader.getSource();
            }
        }),
        
        TARGET("target", new ExportNode() {
            @Override
            public ExportConfig.NodeConfig getNodeConfig(ExportConfig config) {
                return config.getTarget();
            }

            @Override
            public Node getNode(Relationship rel) {
                return rel.getEndNode();
            }

            @Override
            public NodeConfig getNodeConfigReader(XmlGraphMLReader reader) {
                return reader.getTarget();
            }
        });

        private final String name;
        private final ExportNode exportNode;

        NodeType(String name, ExportNode exportNode) {
            this.name = name;
            this.exportNode = exportNode;
        }

        public String getName() {
            return name;
        }

        public String getNameType() {
            return name + "Type";
        }

        ExportNode get() {
            return exportNode;
        }
    }
}
