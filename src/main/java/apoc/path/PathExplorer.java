package apoc.path;

import apoc.algo.Cover;
import apoc.result.GraphResult;
import apoc.result.NodeResult;
import org.neo4j.procedure.Description;
import apoc.result.PathResult;
import apoc.util.Util;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
		return explorePathPrivate(nodes, pathFilter, labelFilter, minLevel, maxLevel, BFS, UNIQUENESS, false, -1).map( PathResult::new );
	}

	//
	@Procedure("apoc.path.expandConfig")
	@Description("apoc.path.expandConfig(startNode <id>|Node|list, {minLevel,maxLevel,uniqueness,relationshipFilter,labelFilter,uniqueness:'RELATIONSHIP_PATH',bfs:true, filterStartNode:false}) yield path expand from start node following the given relationships from min to max-level adhering to the label filters")
	public Stream<PathResult> expandConfig(@Name("start") Object start, @Name("config") Map<String,Object> config) throws Exception {
		return expandConfigPrivate(start, config).map( PathResult::new );
	}

	@Procedure("apoc.path.subgraphNodes")
	@Description("apoc.path.subgraphNodes(startNode <id>|Node|list, {maxLevel,relationshipFilter,labelFilter,bfs:true, filterStartNode:false}) yield node expand the subgraph nodes reachable from start node following relationships to max-level adhering to the label filters")
	public Stream<NodeResult> subgraphNodes(@Name("start") Object start, @Name("config") Map<String,Object> config) throws Exception {
		Map<String, Object> configMap = new HashMap<>(config);
		configMap.remove("minLevel");
		configMap.put("uniqueness", "NODE_GLOBAL");

		return expandConfigPrivate(start, configMap).map( path -> path == null ? new NodeResult(null) : new NodeResult(path.endNode()) );
	}

	@Procedure("apoc.path.subgraphAll")
	@Description("apoc.path.subgraphAll(startNode <id>|Node|list, {maxLevel,relationshipFilter,labelFilter,bfs:true, filterStartNode:false}) yield nodes, relationships expand the subgraph reachable from start node following relationships to max-level adhering to the label filters, and also return all relationships within the subgraph")
	public Stream<GraphResult> subgraphAll(@Name("start") Object start, @Name("config") Map<String,Object> config) throws Exception {
		Map<String, Object> configMap = new HashMap<>(config);
		configMap.remove("minLevel");
		configMap.remove("optional"); // not needed, will return empty collections anyway if no results
		configMap.put("uniqueness", "NODE_GLOBAL");

		List<Node> subgraphNodes = expandConfigPrivate(start, configMap).map( Path::endNode ).collect(Collectors.toList());
		List<Relationship> subgraphRels = Cover.coverNodes(subgraphNodes).collect(Collectors.toList());

		return Stream.of(new GraphResult(subgraphNodes, subgraphRels));
	}

	@Procedure("apoc.path.spanningTree")
	@Description("apoc.path.spanningTree(startNode <id>|Node|list, {maxLevel,relationshipFilter,labelFilter,bfs:true, filterStartNode:false}) yield path expand a spanning tree reachable from start node following relationships to max-level adhering to the label filters")
	public Stream<PathResult> spanningTree(@Name("start") Object start, @Name("config") Map<String,Object> config) throws Exception {
		Map<String, Object> configMap = new HashMap<>(config);
		configMap.remove("minLevel");
		configMap.put("uniqueness", "NODE_GLOBAL");

		return expandConfigPrivate(start, configMap).map( PathResult::new );
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

	private Stream<Path> expandConfigPrivate(@Name("start") Object start, @Name("config") Map<String,Object> config) throws Exception {
		List<Node> nodes = startToNodes(start);

		String uniqueness = (String) config.getOrDefault("uniqueness", UNIQUENESS.name());
		String relationshipFilter = (String) config.getOrDefault("relationshipFilter", null);
		String labelFilter = (String) config.getOrDefault("labelFilter", null);
		long minLevel = Util.toLong(config.getOrDefault("minLevel", "-1"));
		long maxLevel = Util.toLong(config.getOrDefault("maxLevel", "-1"));
		boolean bfs = Util.toBoolean(config.getOrDefault("bfs",true));
		boolean filterStartNode = Util.toBoolean(config.getOrDefault("filterStartNode", false));
		long limit = Util.toLong(config.getOrDefault("limit", "-1"));
		boolean optional = Util.toBoolean(config.getOrDefault("optional", false));

		Stream<Path> results = explorePathPrivate(nodes, relationshipFilter, labelFilter, minLevel, maxLevel, bfs, getUniqueness(uniqueness), filterStartNode, limit);

		if (optional) {
			return optionalStream(results);
		} else {
			return results;
		}
	}

	private Stream<Path> explorePathPrivate(Iterable<Node> startNodes
			, String pathFilter
			, String labelFilter
			, long minLevel
			, long maxLevel, boolean bfs, Uniqueness uniqueness, boolean filterStartNode, long limit) {
		// LabelFilter
		// -|Label|:Label|:Label excluded label list
		// +:Label or :Label include labels

		Traverser traverser = traverse(db.traversalDescription(), startNodes, pathFilter, labelFilter, minLevel, maxLevel, uniqueness,bfs,filterStartNode,limit);
		return traverser.stream();
	}

	/**
	 * If the stream is empty, returns a stream of a single null value, otherwise returns the equivalent of the input stream
	 * @param stream the input stream
	 * @return a stream of a single null value if the input stream is empty, otherwise returns the equivalent of the input stream
	 */
	private Stream<Path> optionalStream(Stream<Path> stream) {
		Stream<Path> optionalStream;
		Iterator<Path> itr = stream.iterator();
		if (itr.hasNext()) {
			optionalStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(itr, 0), false);
		} else {
			List<Path> listOfNull = new ArrayList<>();
			listOfNull.add(null);
			optionalStream = listOfNull.stream();
		}

		return optionalStream;
	}

	public static Traverser traverse(TraversalDescription traversalDescription, Iterable<Node> startNodes, String pathFilter, String labelFilter, long minLevel, long maxLevel, Uniqueness uniqueness, boolean bfs, boolean filterStartNode, long limit) {
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
			td = td.evaluator(new LabelEvaluator(labelFilter, filterStartNode, limit, (int) minLevel));
		}

		td = td.uniqueness(uniqueness); // this is how Cypher works !! Uniqueness.RELATIONSHIP_PATH
		// uniqueness should be set as last on the TraversalDescription
		return td.traverse(startNodes);
	}

	public static class LabelEvaluator implements Evaluator {
		private Set<String> whitelistLabels;
		private Set<String> blacklistLabels;
		private Set<String> terminationLabels;
		private Set<String> endNodeLabels;
		private Evaluation whitelistAllowedEvaluation;
		private boolean endNodesOnly;
		private boolean filterStartNode;
		private long limit = -1;
		private long minLevel = -1;
		private long resultCount = 0;

		public LabelEvaluator(String labelFilter, boolean filterStartNode, long limit, int minLevel) {
			this.filterStartNode = filterStartNode;
			this.limit = limit;
			this.minLevel = minLevel;
			Map<Character, Set<String>> labelMap = new HashMap<>(4);

			if (labelFilter !=  null && !labelFilter.isEmpty()) {

				// parse the filter
				// split on |
				String[] defs = labelFilter.split("\\|");
				Set<String> labels = null;

				for (String def : defs) {
					char operator = def.charAt(0);
					switch (operator) {
						case '+':
						case '-':
						case '/':
						case '>':
							labels = labelMap.computeIfAbsent(operator, character -> new HashSet<>());
							def = def.substring(1);
					}

					if (def.startsWith(":")) {
						def = def.substring(1);
					}

					if (!def.isEmpty()) {
						labels.add(def);
					}
				}
			}

			whitelistLabels = labelMap.computeIfAbsent('+', character -> Collections.emptySet());
			blacklistLabels = labelMap.computeIfAbsent('-', character -> Collections.emptySet());
			terminationLabels = labelMap.computeIfAbsent('/', character -> Collections.emptySet());
			endNodeLabels = labelMap.computeIfAbsent('>', character -> Collections.emptySet());
			endNodesOnly = !terminationLabels.isEmpty() || !endNodeLabels.isEmpty();
			whitelistAllowedEvaluation = endNodesOnly ? EXCLUDE_AND_CONTINUE : INCLUDE_AND_CONTINUE;
		}

		@Override
		public Evaluation evaluate(Path path) {
			int depth = path.length();
			Node check = path.endNode();

			// if start node shouldn't be filtered, exclude/include based on if using termination/endnode filter or not
			// minLevel evaluator will separately enforce exclusion if we're below minLevel
			if (depth == 0 && !filterStartNode) {
				return whitelistAllowedEvaluation;
			}

			// below minLevel always exclude; continue if blacklist and whitelist allow it
			if (depth < minLevel) {
				return labelExists(check, blacklistLabels) || !whitelistAllowed(check) ? EXCLUDE_AND_PRUNE : EXCLUDE_AND_CONTINUE;
			}

			// cut off expansion when we reach the limit
			if (limit != -1 && resultCount >= limit) {
				return EXCLUDE_AND_PRUNE;
			}

			Evaluation result = labelExists(check, blacklistLabels) ? EXCLUDE_AND_PRUNE :
					labelExists(check, terminationLabels) ? filterEndNode(check, true) :
					labelExists(check, endNodeLabels) ? filterEndNode(check, false) :
					whitelistAllowed(check) ? whitelistAllowedEvaluation : EXCLUDE_AND_PRUNE;

			return result;
		}

		private boolean labelExists(Node node, Set<String> labels) {
			if (labels.isEmpty()) {
				return false;
			}

			for ( Label lab : node.getLabels() ) {
				if (labels.contains(lab.name())) {
					return true;
				}
			}
			return false;
		}

		private boolean whitelistAllowed(Node node) {
			return whitelistLabels.isEmpty() || labelExists(node, whitelistLabels);
		}

		private Evaluation filterEndNode(Node node, boolean isTerminationFilter) {
			resultCount++;
			return isTerminationFilter || !whitelistAllowed(node) ? INCLUDE_AND_PRUNE : INCLUDE_AND_CONTINUE;
		}
	}
}