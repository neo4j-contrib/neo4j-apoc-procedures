package apoc.generate.node;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.unsafe.batchinsert.BatchInserter;

/**
 * A component creating {@link Node}s with labels and properties.
 */
public interface NodeCreator {

    /**
     * Create a node with labels and properties.
     *
     * @param database to create the node in.
     * @return created node.
     */
    Node createNode(GraphDatabaseService database);
}
