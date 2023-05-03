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
                    return Nodes.countIncoming(nodeCursor);
                } else {
                    return Nodes.countIncoming(nodeCursor, relType);
                }
            case OUTGOING:
                if (relType==ANY_RELATIONSHIP_TYPE) {
                    return Nodes.countOutgoing(nodeCursor);
                } else {
                    return Nodes.countOutgoing(nodeCursor, relType);
                }
            case BOTH:
                if (relType==ANY_RELATIONSHIP_TYPE) {
                    return Nodes.countAll(nodeCursor);
                } else {
                    return Nodes.countAll(nodeCursor, relType);
                }
            default: throw new IllegalArgumentException("invalid direction " + direction);
        }
    }

}
