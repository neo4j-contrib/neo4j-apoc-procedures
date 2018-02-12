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
	@Description("apoc.path.expand(startNode <id>|Node|list, 'TYPE|TYPE_OUT>|<TYPE_IN', '+YesLabel|-NoLabel', minLevel, maxLevel ) yield path - expand from start node following the given relationships from min to max-level adhering to the label filters")
	public Stream<PathResult> explorePath(@Name("start") Object start
			                   , @Name("relationshipFilter") String pathFilter
			                   , @Name("labelFilter") String labelFilter
			                   , @Name("minLevel") long minLevel
			                   , @Name("maxLevel") long maxLevel ) throws Exception {
		List<Node> nodes = startToNodes(start);
		return explorePathPrivate(nodes, pathFilter, labelFilter, minLevel, maxLevel, BFS, UNIQUENESS, false, -1, Collections.emptyList(), Collections.emptyList(), null, false).map( PathResult::new );
	}

	//
	@Procedure("apoc.path.expandConfig")
	@Description("apoc.path.expandConfig(startNode <id>|Node|list, {minLevel,maxLevel,uniqueness,relationshipFilter,labelFilter,uniqueness:'RELATIONSHIP_PATH',bfs:true, filterStartNode:false, limit:-1, optional:false, endNodes:[], terminatorNodes:[], labelSequence, beginLabelSequenceAtStart:true}) yield path - " +
			"expand from start node following the given relationships from min to max-level adhering to the label filters. ")
	public Stream<PathResult> expandConfig(@Name("start") Object start, @Name("config") Map<String,Object> config) throws Exception {
		return expandConfigPrivate(start, config).map( PathResult::new );
	}

	@Procedure("apoc.path.subgraphNodes")
	@Description("apoc.path.subgraphNodes(startNode <id>|Node|list, {maxLevel,relationshipFilter,labelFilter,bfs:true, filterStartNode:false, limit:-1, optional:false, endNodes:[], terminatorNodes:[], labelSequence, beginLabelSequenceAtStart:true}) yield node - expand the subgraph nodes reachable from start node following relationships to max-level adhering to the label filters")
	public Stream<NodeResult> subgraphNodes(@Name("start") Object start, @Name("config") Map<String,Object> config) throws Exception {
		Map<String, Object> configMap = new HashMap<>(config);
		configMap.remove("minLevel");
		configMap.put("uniqueness", "NODE_GLOBAL");

		return expandConfigPrivate(start, configMap).map( path -> path == null ? new NodeResult(null) : new NodeResult(path.endNode()) );
	}

	@Procedure("apoc.path.subgraphAll")
	@Description("apoc.path.subgraphAll(startNode <id>|Node|list, {maxLevel,relationshipFilter,labelFilter,bfs:true, filterStartNode:false, limit:-1, endNodes:[], terminatorNodes:[], labelSequence, beginLabelSequenceAtStart:true}) yield nodes, relationships - expand the subgraph reachable from start node following relationships to max-level adhering to the label filters, and also return all relationships within the subgraph")
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
	@Description("apoc.path.spanningTree(startNode <id>|Node|list, {maxLevel,relationshipFilter,labelFilter,bfs:true, filterStartNode:false, limit:-1, optional:false, endNodes:[], terminatorNodes:[], labelSequence, beginLabelSequenceAtStart:true}) yield path - expand a spanning tree reachable from start node following relationships to max-level adhering to the label filters")
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
		List<Node> endNodes = startToNodes(config.get("endNodes"));
		List<Node> terminatorNodes = startToNodes(config.get("terminatorNodes"));
		String labelSequence = (String) config.getOrDefault("labelSequence", null);
		boolean beginLabelSequenceAtStart = Util.toBoolean(config.getOrDefault("beginLabelSequenceAtStart", true));


		Stream<Path> results = explorePathPrivate(nodes, relationshipFilter, labelFilter, minLevel, maxLevel, bfs, getUniqueness(uniqueness), filterStartNode, limit, endNodes, terminatorNodes, labelSequence, beginLabelSequenceAtStart);

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
			, long maxLevel
			, boolean bfs
			, Uniqueness uniqueness
			, boolean filterStartNode
			, long limit
	        , List<Node> endNodes
	        , List<Node> terminatorNodes
	        , String labelSequence
	        , boolean beginLabelSequenceAtStart) {
		// LabelMatcher
		// -Label|:Label|:Label excluded label list
		// +:Label or :Label include labels

		Traverser traverser = traverse(db.traversalDescription(), startNodes, pathFilter, labelFilter, minLevel, maxLevel, uniqueness,bfs,filterStartNode, endNodes, terminatorNodes, labelSequence, beginLabelSequenceAtStart);

		if (limit == -1) {
			return traverser.stream();
		} else {
			return traverser.stream().limit(limit);
		}
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

	public static Traverser traverse(TraversalDescription traversalDescription, Iterable<Node> startNodes, String pathFilter, String labelFilter, long minLevel, long maxLevel, Uniqueness uniqueness, boolean bfs, boolean filterStartNode, List<Node> endNodes, List<Node> terminatorNodes, String labelSequence, boolean beginLabelSequenceAtStart) {
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
			td = td.evaluator(new LabelEvaluator(labelFilter, filterStartNode, (int) minLevel));
		}

		// empty list from default can't be added to, so if we need to add, need new instance
		endNodes = !endNodes.isEmpty() ? endNodes : (terminatorNodes.isEmpty() ? endNodes : new ArrayList<Node>(terminatorNodes));

		if (!endNodes.isEmpty()) {
			Node[] nodes = endNodes.toArray(new Node[endNodes.size()]);
			td = td.evaluator(Evaluators.includeWhereEndNodeIs(nodes));
		}

		if (!terminatorNodes.isEmpty()) {
			Node[] nodes = terminatorNodes.toArray(new Node[terminatorNodes.size()]);
			td = td.evaluator(Evaluators.pruneWhereEndNodeIs(nodes));
		}

		if (labelSequence != null && !labelSequence.trim().isEmpty()) {
			td = td.evaluator(new LabelSequenceEvaluator(labelSequence, filterStartNode, beginLabelSequenceAtStart, (int) minLevel));
		}

		td = td.uniqueness(uniqueness); // this is how Cypher works !! Uniqueness.RELATIONSHIP_PATH
		// uniqueness should be set as last on the TraversalDescription
		return td.traverse(startNodes);
	}

	public static class LabelEvaluator implements Evaluator {
		private LabelMatcher whitelistMatcher;
		private LabelMatcher blacklistMatcher;
		private LabelMatcher terminatorMatcher;
		private LabelMatcher endNodeMatcher;

		private Evaluation whitelistAllowedEvaluation;
		private boolean filterStartNode;
		private long minLevel = -1;

		public LabelEvaluator(String labelString, boolean filterStartNode, int minLevel) {
			this.filterStartNode = filterStartNode;
			this.minLevel = minLevel;
			Map<Character, LabelMatcher> matcherMap = new HashMap<>(4);

			if (labelString !=  null && !labelString.isEmpty()) {

				// parse the filter
				// split on |
				String[] defs = labelString.split("\\|");
				LabelMatcher labelMatcher = null;

				for (String def : defs) {
					char operator = def.charAt(0);
					switch (operator) {
						case '+':
						case '-':
						case '/':
						case '>':
							labelMatcher = matcherMap.computeIfAbsent(operator, character -> new LabelMatcher());
							def = def.substring(1);
							break;
						default:
							if (labelMatcher == null) {
								// default to whitelist if no previous matcher
								labelMatcher = matcherMap.computeIfAbsent('+', character -> new LabelMatcher());
							} // else use the currently selected matcher (the one used previously)
							break;
					}

					if (def.startsWith(":")) {
						def = def.substring(1);
					}

					if (!def.isEmpty()) {
						labelMatcher.addLabel(def);
					}
				}
			}

			whitelistMatcher = matcherMap.computeIfAbsent('+', character -> LabelMatcher.acceptsAllLabelMatcher());
			blacklistMatcher = matcherMap.get('-');
			terminatorMatcher = matcherMap.get('/');
			endNodeMatcher = matcherMap.get('>');

			// if we have terminator or end node matchers, we will only include nodes with labels of those types, and exclude all others
			boolean endNodesOnly = terminatorMatcher != null || endNodeMatcher != null;
			whitelistAllowedEvaluation = endNodesOnly ? EXCLUDE_AND_CONTINUE : INCLUDE_AND_CONTINUE;
		}

		@Override
		public Evaluation evaluate(Path path) {
			int depth = path.length();
			Node node = path.endNode();

			// if start node shouldn't be filtered, continue, but exclude/include based on if only returning end nodes
			// minLevel evaluator will separately enforce exclusion if we're below minLevel
			if (depth == 0 && !filterStartNode) {
				return whitelistAllowedEvaluation;
			}

			// always exclude and prune if caught in the blacklist
			if (blacklistMatcher != null && blacklistMatcher.matchesLabels(node)) {
				return EXCLUDE_AND_PRUNE;
			}

			// always include and prune if found in the terminator matcher (if at or above minLevel)
			if (terminatorMatcher != null && depth >= minLevel && terminatorMatcher.matchesLabels(node)) {
				return INCLUDE_AND_PRUNE;
			}

			// always include if found in the end node matcher, but only continue if passes whitelist
			// minLevel evaluator will separately enforce exclusion if we're below minLevel
			if (endNodeMatcher != null && endNodeMatcher.matchesLabels(node)) {
				return whitelistMatcher.matchesLabels(node) ? INCLUDE_AND_CONTINUE : INCLUDE_AND_PRUNE;
			}

			// always continue if found in the whitelist, but include/exclude based on if only end nodes are being returned
			// minLevel evaluator will separately enforce exclusion if we're below minLevel
			if (whitelistMatcher.matchesLabels(node)) {
				return whitelistAllowedEvaluation;
			}

			return EXCLUDE_AND_PRUNE;
		}
	}


	public static class LabelSequenceEvaluator implements Evaluator {
		private List<LabelMatcherGroup> sequenceFilters;

		private Evaluation whitelistAllowedEvaluation;
		private boolean endNodesOnly;
		private boolean filterStartNode;
		private boolean beginLabelSequenceAtStart;
		private long minLevel = -1;

		public LabelSequenceEvaluator(String labelSequence, boolean filterStartNode, boolean beginLabelSequenceAtStart, int minLevel) {
			this.filterStartNode = filterStartNode;
			this.beginLabelSequenceAtStart = beginLabelSequenceAtStart;
			this.minLevel = minLevel;


			// parse sequence
			if (labelSequence != null && !labelSequence.isEmpty()) {
				String[] elements = labelSequence.split(",");
				sequenceFilters = new ArrayList<>(elements.length);

				for (String labelFilterString : elements) {
					LabelMatcherGroup matcherGroup = new LabelMatcherGroup().addLabels(labelFilterString.trim());
					sequenceFilters.add(matcherGroup);
					endNodesOnly = endNodesOnly || matcherGroup.isEndNode() || matcherGroup.isTerminatorNode();
				}

			}

			whitelistAllowedEvaluation = endNodesOnly ? EXCLUDE_AND_CONTINUE : INCLUDE_AND_CONTINUE;
		}

		@Override
		public Evaluation evaluate(Path path) {

			int depth = path.length();
			Node node = path.endNode();

			// if start node shouldn't be filtered, exclude/include based on if using termination/endnode filter or not
			// minLevel evaluator will separately enforce exclusion if we're below minLevel
			if (depth == 0 && (!filterStartNode || !beginLabelSequenceAtStart)) {
				return whitelistAllowedEvaluation;
			}

			// the user may want the sequence to begin at the start node (default), or the sequence may only apply from the next node on
			LabelMatcherGroup matcherGroup = sequenceFilters.get((beginLabelSequenceAtStart ? depth : depth - 1) % sequenceFilters.size());

			// below minLevel always exclude; continue if blacklist and whitelist allow it
			if (depth < minLevel) {
				return matcherGroup.matchesLabels(node) ? EXCLUDE_AND_CONTINUE : EXCLUDE_AND_PRUNE;
			}

			boolean matchesSequence = matcherGroup.matchesLabels(node);

			if (!matchesSequence) {
				return EXCLUDE_AND_PRUNE;
			} else if (endNodesOnly) {
				if (matcherGroup.isEndNode()) {
					return INCLUDE_AND_CONTINUE;
				} else if (matcherGroup.isTerminatorNode()) {
					return INCLUDE_AND_PRUNE;
				} else {
					return EXCLUDE_AND_CONTINUE;
				}
			} else {
				return INCLUDE_AND_CONTINUE;
			}

		}


	}
}