package apoc.path;


import apoc.Description;
import apoc.result.PathResult;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;


public class PathExplorer {
	private static final String VERSION = "0.5";
	public static final Uniqueness UNIQUENESS = Uniqueness.RELATIONSHIP_PATH;

	@Context
    public GraphDatabaseService db;

	@Context
    public Log log;
	
	@Procedure("apoc.path.expand")
	@Description("apoc.path.expand(startNode <id>|Node|list, 'TYPE|TYPE_OUT>|<TYPE_IN', '+YesLabel|-NoLabel', minLevel, maxLevel ) yield path expand from start node following the given relationships from min to max-level adhering to the label filters")
	public Stream<PathResult> explorePath(@Name("start") Object start
			                   , @Name("relationshipFilter") String pathFilter
			                   , @Name("labelFilter") String labelFilter
			                   , @Name("minLevel") long minLevel
			                   , @Name("maxLevel") long maxLevel ) throws Exception {
		List<Node> nodes = startToNodes(start);
		return explorePathPrivate(nodes, pathFilter, labelFilter, minLevel, maxLevel);
	}

	@SuppressWarnings("unchecked")
	private List<Node> startToNodes(Object start) throws Exception {
		if (start == null) return Collections.emptyList();
		if (start instanceof Node) {
			return Collections.singletonList((Node) start);
		}
		if (start instanceof Number) {
			return Collections.singletonList(db.getNodeById(((Number) start).longValue()));
		}
		if (start instanceof List) {
			List list = (List) start;
			if (list.isEmpty()) return Collections.emptyList();

			Object first = list.get(0);
			if (first instanceof Node) return (List<Node>)list;
			if (first instanceof Number) {
                List<Node> nodes = new ArrayList<>();
                for (Number n : ((List<Number>)list)) nodes.add(db.getNodeById(n.longValue()));
                return nodes;
            }
		}
		throw new Exception("Unsupported data type for start parameter a Node or an Identifier (long) of a Node must be given!");
	}

	private Stream<PathResult> explorePathPrivate(Iterable<Node> startNodes
			                   , String pathFilter
			                   , String labelFilter
			                   , long minLevel
			                   , long maxLevel ) {
		// LabelFilter
		// -|Label|:Label|:Label excluded label list
		// +:Label or :Label include labels
		
		int from = new Long(minLevel).intValue();
		int to = new Long(maxLevel).intValue();
		TraversalDescription td = db.traversalDescription().breadthFirst();
		// based on the pathFilter definition now the possible relationships and directions must be shown

		Iterable<Pair<RelationshipType, Direction>> relDirIterable = RelationshipTypeAndDirections.parse(pathFilter);

		for (Pair<RelationshipType, Direction> pair: relDirIterable) {
			if (pair.first() == null) {
				td = td.expand(PathExpanderBuilder.allTypes(pair.other()).build());
			} else {
				td = td.relationships(pair.first(), pair.other());
			}

		}

		LabelEvaluator labelEvaluator = new LabelEvaluator(labelFilter);
		td = td.evaluator(Evaluators.fromDepth(from))
				.evaluator(Evaluators.toDepth(to))
				.evaluator(labelEvaluator);
		td = td.uniqueness(UNIQUENESS); // this is how Cypher works !!
		// uniqueness should be set as last on the TraversalDescription
		return td.traverse(startNodes).stream().map( PathResult::new );
	}

		public static class LabelEvaluator implements Evaluator {
		private boolean included = true;
		private Set<String> labels = new HashSet<String>();
		public LabelEvaluator(String labelFilter) {
			// parse the filter
			if (labelFilter ==  null || labelFilter.equalsIgnoreCase("")) labelFilter = "-"; // exclude nothing
			included = labelFilter.startsWith("+");
			String work = labelFilter.substring(1); // remove the + or -
			// split on |
			String[] defs = work.split("\\|") ;
			for (String def : defs) {
				if (def.startsWith(":")) def = def.substring(1);
				labels.add(def);
			}
		}
		
		
		@Override
		public Evaluation evaluate(Path path) {
			Node check = path.endNode();
			if (included) {
				if (labelExists(check)) {
					return Evaluation.INCLUDE_AND_CONTINUE;
				} else {
					return Evaluation.EXCLUDE_AND_PRUNE;
				}
			} else {
				if (labelExists(check)) {
					return Evaluation.EXCLUDE_AND_PRUNE;
				} else {
					return Evaluation.INCLUDE_AND_CONTINUE;
				}
			}
		}
		private boolean labelExists(Node node) {
			for ( Label lab : node.getLabels() ) {
				if (labels.contains(lab.name())) {
					return true;
				}
			}
			return false;
		}
	}

}
