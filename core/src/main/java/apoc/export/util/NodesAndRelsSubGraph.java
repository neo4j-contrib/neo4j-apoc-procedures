package apoc.export.util;

import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.helpers.collection.Iterables;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

/**
 * @author mh
 * @since 22.05.16
 */
public class NodesAndRelsSubGraph implements SubGraph {
    private final Collection<Node> nodes;
    private final Collection<Relationship> rels;
    private final Transaction tx;
    private final HashSet<String> labels = new HashSet<>(20);

    public NodesAndRelsSubGraph(Transaction tx, Collection<Node> nodes, Collection<Relationship> rels) {
        this.tx = tx;
        this.nodes = new ArrayList<>(nodes.size());
        for (Node node : nodes) {
            for (Label label : node.getLabels()) labels.add(label.name());
            this.nodes.add(node);
        }
        this.rels = new HashSet<>(rels);
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
        Schema schema = tx.schema();
        ArrayList<IndexDefinition> indexes = new ArrayList<>(labels.size() * 2);
        for (String label : labels) {
            Iterables.addAll(indexes, schema.getIndexes(Label.label(label)));
        }
        return indexes;
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints() {
        Schema schema = tx.schema();
        ArrayList<ConstraintDefinition> constraints = new ArrayList<>(labels.size() * 2);
        for (String label : labels) {
            Iterables.addAll(constraints, schema.getConstraints(Label.label(label)));
        }
        return constraints;
    }
}
