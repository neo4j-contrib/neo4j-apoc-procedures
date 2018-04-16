package apoc.stats;

import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;

import static org.neo4j.internal.kernel.api.Read.ANY_RELATIONSHIP_TYPE;

public class DegreeUtil {

    public static int degree(NodeCursor nodeCursor, CursorFactory cursors, int relType, Direction direction) {
        return nodeCursor.isDense() ? degreeDense(cursors, relType, direction) : degreeNotDense(nodeCursor, cursors, relType, direction);
    }

    private static int degreeDense(CursorFactory cursors, int relType, Direction direction) {
        try (RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor()) {
            int count = 0;
            while (group.next()) {
                if (reltypeMatches(relType, group.type())) {
                    switch (direction) {
                        case INCOMING:
                            count += group.incomingCount();
                            break;
                        case OUTGOING:
                            count += group.outgoingCount();
                            break;
                        case BOTH:
                            count += group.totalCount();
                            break;
                    }
                }
            }
            return count;
        }
    }

    private static int degreeNotDense(NodeCursor nodeCursor, CursorFactory cursors, int relType, Direction direction) {
        try (RelationshipTraversalCursor traversal = cursors.allocateRelationshipTraversalCursor()) {
            int count = 0;
            nodeCursor.allRelationships(traversal);
            while (traversal.next()) {
                if (reltypeMatches(relType, traversal.type()) && directionMatches(nodeCursor.nodeReference(), direction, traversal)) {
                    count++;
                }
            }
            return count;
        }
    }

    private static boolean reltypeMatches(int relType, int currentType) {
        return (relType==ANY_RELATIONSHIP_TYPE) || (relType==currentType);
    }

    private static boolean directionMatches(long startId, Direction direction, RelationshipTraversalCursor traversal) {
        switch (direction) {
            case INCOMING:
                return traversal.targetNodeReference() == startId;
            case OUTGOING:
                return traversal.sourceNodeReference() == startId;
            case BOTH:
                return true;
            default:
                throw new IllegalArgumentException("invalid direction " + direction);
        }
    }

}
