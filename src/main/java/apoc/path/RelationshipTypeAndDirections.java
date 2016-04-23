package apoc.path;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.Pair;

import java.util.ArrayList;
import java.util.Collection;

import static org.neo4j.graphdb.Direction.*;

/**
 * helper class parsing relationship types and directions
 */

public abstract class RelationshipTypeAndDirections {

	public static Iterable<Pair<RelationshipType, Direction>> parse(String pathFilter) {
		Collection<Pair<RelationshipType, Direction>> relsAndDirs = new ArrayList<>();
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

	private static Direction directionFor(String type) {
		if (type.contains("<")) return INCOMING;
		if (type.contains(">")) return OUTGOING;
		return BOTH;
	}

	private static RelationshipType relationshipTypeFor(String name) {
		if (name.startsWith(":")) {
			name = name.substring(1);
		}
		if (name.endsWith(">") || name.endsWith("<")) {
			name = name.substring(0, name.length() -1);
		}
		return name.isEmpty() ? null : RelationshipType.withName(name);
	}
}
