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
