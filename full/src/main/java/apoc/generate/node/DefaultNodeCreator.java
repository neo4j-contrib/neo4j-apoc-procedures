package apoc.generate.node;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

/**
 * A {@link NodeCreator} that assigns every {@link Node} a {@link Label} passed to it in the constructor and a UUID as
 * a property.
 */
public class DefaultNodeCreator implements NodeCreator {

    private static final String UUID = "uuid";

    private final Label label;

    public DefaultNodeCreator(String label) {
        this.label = Label.label(label);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node createNode(Transaction tx) {
        Node node = tx.createNode(label);
        node.setProperty(UUID, java.util.UUID.randomUUID().toString());
        return node;
    }
}
