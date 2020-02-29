package apoc.export.util;

import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


public class MapSubGraph implements SubGraph {

    public static class MapIndexDefinition implements IndexDefinition {
        private final Label label;
        private final Collection<Label> labels;
        private final Collection<String> properties;
        private final String type;

        public MapIndexDefinition(Map<String, Object> map) {
            Collection<String> labels = (Collection<String>) map.get("labels");
            Collection<String> properties = (Collection<String>) map.get("properties");
            String type = (String) map.get("type");
            this.labels = labels.stream().map(Label::label).collect(Collectors.toList());
            this.label = this.labels.iterator().next();
            this.properties = properties;
            this.type = type;
        }

        @Override
        public Label getLabel() {
            return label;
        }

        @Override
        public Iterable<Label> getLabels() {
            return labels;
        }

        @Override
        public RelationshipType getRelationshipType() {
            throw new UnsupportedOperationException("Method not implemented");
        }

        @Override
        public Iterable<RelationshipType> getRelationshipTypes() {
            throw new UnsupportedOperationException("Method not implemented");
        }

        @Override
        public Iterable<String> getPropertyKeys() {
            return properties;
        }

        @Override
        public void drop() {}

        @Override
        public boolean isConstraintIndex() {
            return false;
        }

        @Override
        public boolean isNodeIndex() {
            return true;
        }

        @Override
        public boolean isRelationshipIndex() {
            return false;
        }

        @Override
        public boolean isMultiTokenIndex() {
            return false;
        }

        @Override
        public boolean isCompositeIndex() {
            return properties.size() > 1;
        }

        @Override
        public String getName() {
            return null;
        }
    }

    public static class MapConstraintDefinition implements ConstraintDefinition {
        private final Label label;
        private final Collection<Label> labels;
        private final Collection<String> properties;

        public MapConstraintDefinition(Map<String, Object> map) {
            Collection<String> labels = (Collection<String>) map.get("labels");
            Collection<String> properties = (Collection<String>) map.get("properties");
            this.labels = labels.stream().map(Label::label).collect(Collectors.toList());
            this.label = this.labels.iterator().next();
            this.properties = properties;
        }


        @Override
        public Label getLabel() {
            return label;
        }

        @Override
        public RelationshipType getRelationshipType() {
            throw new UnsupportedOperationException("Method not implemented");
        }

        @Override
        public Iterable<String> getPropertyKeys() {
            return properties;
        }

        @Override
        public void drop() {}

        @Override
        public ConstraintType getConstraintType() {
            return properties.size() == 1 ? ConstraintType.UNIQUENESS : ConstraintType.NODE_KEY;
        }

        @Override
        public boolean isConstraintType(ConstraintType type) {
            return getConstraintType().equals(type);
        }
    }

    private final Collection<Node> nodes;
    private final Collection<Relationship> rels;
    private final Set<String> labels;
    private final Collection<IndexDefinition> indexes;
    private final Collection<ConstraintDefinition> constraints;

    public MapSubGraph(Map<String, Object> map) {
        this.nodes = (Collection<Node>) map.getOrDefault("nodes", Collections.emptyList());
        this.labels = this.nodes.stream()
                .flatMap(node -> StreamSupport.stream(node.getLabels().spliterator(), true)
                .map(Label::name))
                .collect(Collectors.toSet());
        Collection<Relationship> rels = (Collection<Relationship>) map.getOrDefault("relationships", Collections.emptyList());
        this.rels = new HashSet<>(rels);
        Collection<Map<String, Object>> schema = (Collection<Map<String, Object>>) map.getOrDefault("schema", Collections.emptyList());
        this.indexes = schema.stream().map(MapIndexDefinition::new).collect(Collectors.toList());
        this.constraints = schema.stream().map(MapConstraintDefinition::new).collect(Collectors.toList());
    }

    @Override
    public Iterable<Node> getNodes() {
        return nodes;
    }

    @Override
    public Iterable<Relationship> getRelationships() {
        return rels;
    }

    @Override
    public boolean contains(Relationship relationship) {
        return rels.contains(relationship);
    }

    @Override
    public Iterable<IndexDefinition> getIndexes() {
        return indexes;
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints() {
        return constraints;
    }
}
