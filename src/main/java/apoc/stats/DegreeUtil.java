package apoc.stats;

import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.helpers.Nodes;

import static org.neo4j.internal.kernel.api.TokenRead.ANY_RELATIONSHIP_TYPE;

public class DegreeUtil {

    public static int degree(NodeCursor nodeCursor, CursorFactory cursors, int relType, Direction direction) {

        switch (direction) {
            case INCOMING:
                if (relType==ANY_RELATIONSHIP_TYPE) {
                    return Nodes.countIncoming(nodeCursor, cursors);
                } else {
                    return Nodes.countIncoming(nodeCursor, cursors, relType);
                }
            case OUTGOING:
                if (relType==ANY_RELATIONSHIP_TYPE) {
                    return Nodes.countOutgoing(nodeCursor, cursors);
                } else {
                    return Nodes.countOutgoing(nodeCursor, cursors, relType);
                }
            case BOTH:
                if (relType==ANY_RELATIONSHIP_TYPE) {
                    return Nodes.countAll(nodeCursor, cursors);
                } else {
                    return Nodes.countAll(nodeCursor, cursors, relType);
                }
            default: throw new IllegalArgumentException("invalid direction " + direction);
        }
    }

}
