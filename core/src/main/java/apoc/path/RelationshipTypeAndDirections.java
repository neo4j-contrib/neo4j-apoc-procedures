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
package apoc.path;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.ArrayList;
import java.util.List;

import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * helper class parsing relationship types and directions
 */

public abstract class RelationshipTypeAndDirections {

	public static final char BACKTICK = '`';

    public static String format(Pair<RelationshipType, Direction> typeAndDirection) {
        String type = typeAndDirection.first().name();
        switch (typeAndDirection.other()) {
            case OUTGOING:
                return type + ">";
            case INCOMING:
                return "<" + type;
            default:
                return type;
        }
    }

	public static List<Pair<RelationshipType, Direction>> parse(String pathFilter) {
		List<Pair<RelationshipType, Direction>> relsAndDirs = new ArrayList<>();
		if (pathFilter == null) {
			relsAndDirs.add(Pair.of(null, BOTH));
		} else {
			String[] defs = pathFilter.split("\\|");
			for (String def : defs) {
				relsAndDirs.add(Pair.of(relationshipTypeFor(def), directionFor(def)));
			}
		}
		return relsAndDirs;
	}

	public static Direction directionFor(String type) {
		if (type.contains("<")) return INCOMING;
		if (type.contains(">")) return OUTGOING;
		return BOTH;
	}

	public static RelationshipType relationshipTypeFor(String name) {
		if (name.indexOf(BACKTICK) > -1) name = name.substring(name.indexOf(BACKTICK)+1,name.lastIndexOf(BACKTICK));
		else {
			name = name.replaceAll("[<>:]", "");
		}
		return name.trim().isEmpty() ? null : RelationshipType.withName(name);
	}
}
