package apoc.path;

import apoc.Description;
import apoc.result.PathResult;
import apoc.util.Util;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;

import static org.neo4j.graphdb.traversal.Evaluation.*;


public class PathExplorer {
	private static final String VERSION = "0.5";
	public static final Uniqueness UNIQUENESS = Uniqueness.RELATIONSHIP_PATH;
	public static final boolean BFS = true;
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
		return explorePathPrivate(nodes, pathFilter, labelFilter, minLevel, maxLevel, BFS, UNIQUENESS);
	}

	//
	@Procedure("apoc.path.expandConfig")
	@Description("apoc.path.expandConfig(startNode <id>|Node|list, {minLevel,maxLevel,uniqueness,relationshipFilter,labelFilter,uniqueness:'RELATIONSHIP_PATH',bfs:true}) yield path expand from start node following the given relationships from min to max-level adhering to the label filters")
	public Stream<PathResult> expandConfig(@Name("start") Object start, @Name("config") Map<String,Object> config) throws Exception {
		List<Node> nodes = startToNodes(start);

		String uniqueness = (String) config.getOrDefault("uniqueness", UNIQUENESS.name());
		String relationshipFilter = (String) config.getOrDefault("relationshipFilter", null);
		String labelFilter = (String) config.getOrDefault("labelFilter", null);
		long minLevel = Util.toLong(config.getOrDefault("minLevel", "-1"));
		long maxLevel = Util.toLong(config.getOrDefault("maxLevel", "-1"));
		boolean bfs = Util.toBoolean(config.getOrDefault("bfs",true));

		return explorePathPrivate(nodes, relationshipFilter, labelFilter, minLevel, maxLevel, bfs, getUniqueness(uniqueness));
	}

	private Uniqueness getUniqueness(String uniqueness) {
		for (Uniqueness u : Uniqueness.values()) {
			if (u.name().equalsIgnoreCase(uniqueness)) return u;
		}
		return UNIQUENESS;
	}

	/*
    , @Name("relationshipFilter") String pathFilter
    , @Name("labelFilter") String labelFilter
    , @Name("minLevel") long minLevel
    , @Name("maxLevel") long maxLevel ) throws Exception {
     */
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
			, long maxLevel, boolean bfs, Uniqueness uniqueness) {
		// LabelFilter
		// -|Label|:Label|:Label excluded label list
		// +:Label or :Label include labels

		Traverser traverser = traverse(db.traversalDescription(), startNodes, pathFilter, labelFilter, minLevel, maxLevel, uniqueness,bfs);
		return traverser.stream().map( PathResult::new );
	}

	public static Traverser traverse(TraversalDescription traversalDescription, Iterable<Node> startNodes, String pathFilter, String labelFilter, long minLevel, long maxLevel, Uniqueness uniqueness, boolean bfs) {
		TraversalDescription td = traversalDescription;
		// based on the pathFilter definition now the possible relationships and directions must be shown

		td = bfs ? td.breadthFirst() : td.depthFirst();

		Iterable<Pair<RelationshipType, Direction>> relDirIterable = RelationshipTypeAndDirections.parse(pathFilter);

		for (Pair<RelationshipType, Direction> pair: relDirIterable) {
			if (pair.first() == null) {
				td = td.expand(PathExpanderBuilder.allTypes(pair.other()).build());
			} else {
				td = td.relationships(pair.first(), pair.other());
			}
		}

		if (minLevel != -1) td = td.evaluator(Evaluators.fromDepth((int) minLevel));
		if (maxLevel != -1) td = td.evaluator(Evaluators.toDepth((int) maxLevel));

		if (labelFilter != null && !labelFilter.trim().isEmpty()) {
			td = td.evaluator(new LabelEvaluator(labelFilter));
		}

		td = td.uniqueness(uniqueness); // this is how Cypher works !! Uniqueness.RELATIONSHIP_PATH
		// uniqueness should be set as last on the TraversalDescription
		return td.traverse(startNodes);
	}

	public static class LabelEvaluator implements Evaluator {
		private char operator;
		private Set<String> labels = new HashSet<String>();
		public LabelEvaluator(String labelFilter) {
			// parse the filter
			if (labelFilter ==  null || labelFilter.isEmpty()) labelFilter = "-"; // exclude nothing
			operator = labelFilter.charAt(0);
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
			Evaluation result;
			switch (operator) {
				case '+':
					result = labelExists(check) ? INCLUDE_AND_CONTINUE : EXCLUDE_AND_PRUNE;
					break;
				case '-':
					result = labelExists(check) ? EXCLUDE_AND_PRUNE : INCLUDE_AND_CONTINUE;
					break;
				case '/':
					result = labelExists(check) ? INCLUDE_AND_PRUNE : EXCLUDE_AND_CONTINUE;
					break;
				default:
					throw new IllegalArgumentException("evaluator uses unknown operator " + operator);
			}
			return result;
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
