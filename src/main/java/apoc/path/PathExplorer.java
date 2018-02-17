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
		return explorePathPrivate(nodes, pathFilter, labelFilter, minLevel, maxLevel, BFS, UNIQUENESS, false, -1, Collections.emptyList(), Collections.emptyList(), null, true).map( PathResult::new );
	}

	//
	@Procedure("apoc.path.expandConfig")
	@Description("apoc.path.expandConfig(startNode <id>|Node|list, {minLevel,maxLevel,uniqueness,relationshipFilter,labelFilter,uniqueness:'RELATIONSHIP_PATH',bfs:true, filterStartNode:false, limit:-1, optional:false, endNodes:[], terminatorNodes:[], sequence, beginSequenceAtStart:true}) yield path - " +
			"expand from start node following the given relationships from min to max-level adhering to the label filters. ")
	public Stream<PathResult> expandConfig(@Name("start") Object start, @Name("config") Map<String,Object> config) throws Exception {
		return expandConfigPrivate(start, config).map( PathResult::new );
	}

	@Procedure("apoc.path.subgraphNodes")
	@Description("apoc.path.subgraphNodes(startNode <id>|Node|list, {maxLevel,relationshipFilter,labelFilter,bfs:true, filterStartNode:false, limit:-1, optional:false, endNodes:[], terminatorNodes:[], sequence, beginSequenceAtStart:true}) yield node - expand the subgraph nodes reachable from start node following relationships to max-level adhering to the label filters")
	public Stream<NodeResult> subgraphNodes(@Name("start") Object start, @Name("config") Map<String,Object> config) throws Exception {
		Map<String, Object> configMap = new HashMap<>(config);
		configMap.put("uniqueness", "NODE_GLOBAL");

		if (config.containsKey("minLevel")) {
			throw new IllegalArgumentException("minLevel not supported in subgraphNodes");
		}

		return expandConfigPrivate(start, configMap).map( path -> path == null ? new NodeResult(null) : new NodeResult(path.endNode()) );
	}

	@Procedure("apoc.path.subgraphAll")
	@Description("apoc.path.subgraphAll(startNode <id>|Node|list, {maxLevel,relationshipFilter,labelFilter,bfs:true, filterStartNode:false, limit:-1, endNodes:[], terminatorNodes:[], sequence, beginSequenceAtStart:true}) yield nodes, relationships - expand the subgraph reachable from start node following relationships to max-level adhering to the label filters, and also return all relationships within the subgraph")
	public Stream<GraphResult> subgraphAll(@Name("start") Object start, @Name("config") Map<String,Object> config) throws Exception {
		Map<String, Object> configMap = new HashMap<>(config);
		configMap.remove("optional"); // not needed, will return empty collections anyway if no results
		configMap.put("uniqueness", "NODE_GLOBAL");

		if (config.containsKey("minLevel")) {
			throw new IllegalArgumentException("minLevel not supported in subgraphAll");
		}

		List<Node> subgraphNodes = expandConfigPrivate(start, configMap).map( Path::endNode ).collect(Collectors.toList());
		List<Relationship> subgraphRels = Cover.coverNodes(subgraphNodes).collect(Collectors.toList());

		return Stream.of(new GraphResult(subgraphNodes, subgraphRels));
	}

	@Procedure("apoc.path.spanningTree")
	@Description("apoc.path.spanningTree(startNode <id>|Node|list, {maxLevel,relationshipFilter,labelFilter,bfs:true, filterStartNode:false, limit:-1, optional:false, endNodes:[], terminatorNodes:[], sequence, beginSequenceAtStart:true}) yield path - expand a spanning tree reachable from start node following relationships to max-level adhering to the label filters")
	public Stream<PathResult> spanningTree(@Name("start") Object start, @Name("config") Map<String,Object> config) throws Exception {
		Map<String, Object> configMap = new HashMap<>(config);
		configMap.put("uniqueness", "NODE_GLOBAL");

		if (config.containsKey("minLevel")) {
			throw new IllegalArgumentException("minLevel not supported in spanningTree");
		}

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
		String sequence = (String) config.getOrDefault("sequence", null);
		boolean beginSequenceAtStart = Util.toBoolean(config.getOrDefault("beginSequenceAtStart", true));


		Stream<Path> results = explorePathPrivate(nodes, relationshipFilter, labelFilter, minLevel, maxLevel, bfs, getUniqueness(uniqueness), filterStartNode, limit, endNodes, terminatorNodes, sequence, beginSequenceAtStart);

		if (optional) {
			return optionalStream(results);
		} else {
			return results;
		}
	}

	private Stream<Path> explorePathPrivate(Iterable<Node> startNodes,
											String pathFilter,
											String labelFilter,
											long minLevel,
											long maxLevel,
											boolean bfs,
											Uniqueness uniqueness,
											boolean filterStartNode,
											long limit,
											List<Node> endNodes,
											List<Node> terminatorNodes,
											String sequence,
											boolean beginSequenceAtStart) {

		Traverser traverser = traverse(db.traversalDescription(), startNodes, pathFilter, labelFilter, minLevel, maxLevel, uniqueness,bfs,filterStartNode, endNodes, terminatorNodes, sequence, beginSequenceAtStart);

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

	public static Traverser traverse(TraversalDescription traversalDescription,
									 Iterable<Node> startNodes,
									 String pathFilter,
									 String labelFilter,
									 long minLevel,
									 long maxLevel,
									 Uniqueness uniqueness,
									 boolean bfs,
									 boolean filterStartNode,
									 List<Node> endNodes,
									 List<Node> terminatorNodes,
									 String sequence,
									 boolean beginSequenceAtStart) {
		TraversalDescription td = traversalDescription;
		// based on the pathFilter definition now the possible relationships and directions must be shown

		td = bfs ? td.breadthFirst() : td.depthFirst();

		// if `sequence` is present, it overrides `labelFilter` and `relationshipFilter`
		if (sequence != null && !sequence.trim().isEmpty())	{
			String[] sequenceSteps = sequence.split(",");
			List<String> labelSequenceList = new ArrayList<>();
			List<String> relSequenceList = new ArrayList<>();

			for (int index = 0; index < sequenceSteps.length; index++) {
				List<String> seq = (beginSequenceAtStart ? index : index - 1) % 2 == 0 ? labelSequenceList : relSequenceList;
				seq.add(sequenceSteps[index]);
			}

			td = td.expand(new RelationshipSequenceExpander(relSequenceList, beginSequenceAtStart));
			td = td.evaluator(new LabelSequenceEvaluator(labelSequenceList, filterStartNode, beginSequenceAtStart, (int) minLevel));
		} else {
			if (pathFilter != null && !pathFilter.trim().isEmpty()) {
				td = td.expand(new RelationshipSequenceExpander(pathFilter.trim(), beginSequenceAtStart));
			}

			if (labelFilter != null && sequence == null && !labelFilter.trim().isEmpty()) {
				td = td.evaluator(new LabelSequenceEvaluator(labelFilter.trim(), filterStartNode, beginSequenceAtStart, (int) minLevel));
			}
		}

		if (minLevel != -1) td = td.evaluator(Evaluators.fromDepth((int) minLevel));
		if (maxLevel != -1) td = td.evaluator(Evaluators.toDepth((int) maxLevel));

		Evaluator endNodeEvaluator = null;
		Evaluator terminatorNodeEvaluator = null;

		if (!endNodes.isEmpty()) {
			Node[] nodes = endNodes.toArray(new Node[endNodes.size()]);
			endNodeEvaluator = Evaluators.includeWhereEndNodeIs(nodes);
		}

		if (!terminatorNodes.isEmpty()) {
			Node[] nodes = terminatorNodes.toArray(new Node[terminatorNodes.size()]);
			terminatorNodeEvaluator = Evaluators.pruneWhereEndNodeIs(nodes);
		}

		if (endNodeEvaluator != null || terminatorNodeEvaluator != null) {
			td = td.evaluator(new EndAndTerminatorNodeEvaluator(endNodeEvaluator, terminatorNodeEvaluator));
		}

		td = td.uniqueness(uniqueness); // this is how Cypher works !! Uniqueness.RELATIONSHIP_PATH
		// uniqueness should be set as last on the TraversalDescription
		return td.traverse(startNodes);
	}

	// when no commas present, acts as a pathwide label filter
	public static class LabelSequenceEvaluator implements Evaluator {
		private List<LabelMatcherGroup> sequenceMatchers;

		private Evaluation whitelistAllowedEvaluation;
		private boolean endNodesOnly;
		private boolean filterStartNode;
		private boolean beginSequenceAtStart;
		private long minLevel = -1;

		public LabelSequenceEvaluator(String labelSequence, boolean filterStartNode, boolean beginSequenceAtStart, int minLevel) {
			List<String> labelSequenceList;

			// parse sequence
			if (labelSequence != null && !labelSequence.isEmpty()) {
				labelSequenceList = Arrays.asList(labelSequence.split(","));
			} else {
				labelSequenceList = Collections.emptyList();
			}

			initialize(labelSequenceList, filterStartNode, beginSequenceAtStart, minLevel);
		}

		public LabelSequenceEvaluator(List<String> labelSequenceList, boolean filterStartNode, boolean beginSequenceAtStart, int minLevel) {
			initialize(labelSequenceList, filterStartNode, beginSequenceAtStart, minLevel);
		}

		private void initialize(List<String> labelSequenceList, boolean filterStartNode, boolean beginSequenceAtStart, int minLevel) {
			this.filterStartNode = filterStartNode;
			this.beginSequenceAtStart = beginSequenceAtStart;
			this.minLevel = minLevel;
			sequenceMatchers = new ArrayList<>(labelSequenceList.size());

			for (String labelFilterString : labelSequenceList) {
				LabelMatcherGroup matcherGroup = new LabelMatcherGroup().addLabels(labelFilterString.trim());
				sequenceMatchers.add(matcherGroup);
				endNodesOnly = endNodesOnly || matcherGroup.isEndNodesOnly();
			}

			// if true for one matcher, need to set true for all matchers
			if (endNodesOnly) {
				for (LabelMatcherGroup group : sequenceMatchers) {
					group.setEndNodesOnly(endNodesOnly);
				}
			}

			whitelistAllowedEvaluation = endNodesOnly ? EXCLUDE_AND_CONTINUE : INCLUDE_AND_CONTINUE;
		}

		@Override
		public Evaluation evaluate(Path path) {
			int depth = path.length();
			Node node = path.endNode();
			boolean belowMinLevel = depth < minLevel;

			// if start node shouldn't be filtered, exclude/include based on if using termination/endnode filter or not
			// minLevel evaluator will separately enforce exclusion if we're below minLevel
			if (depth == 0 && (!filterStartNode || !beginSequenceAtStart)) {
				return whitelistAllowedEvaluation;
			}

			// the user may want the sequence to begin at the start node (default), or the sequence may only apply from the next node on
			LabelMatcherGroup matcherGroup = sequenceMatchers.get((beginSequenceAtStart ? depth : depth - 1) % sequenceMatchers.size());

			return matcherGroup.evaluate(node, belowMinLevel);
		}
	}

	// The evaluators from pruneWhereEndNodeIs and includeWhereEndNodeIs interfere with each other, this makes them play nice
	public static class EndAndTerminatorNodeEvaluator implements Evaluator {
		private Evaluator endNodeEvaluator;
		private Evaluator terminatorNodeEvaluator;

		public EndAndTerminatorNodeEvaluator(Evaluator endNodeEvaluator, Evaluator terminatorNodeEvaluator) {
			this.endNodeEvaluator = endNodeEvaluator;
			this.terminatorNodeEvaluator = terminatorNodeEvaluator;
		}

		@Override
		public Evaluation evaluate(Path path) {
			boolean includes = evalIncludes(endNodeEvaluator, path) || evalIncludes(terminatorNodeEvaluator, path);
			boolean continues = terminatorNodeEvaluator == null || terminatorNodeEvaluator.evaluate(path).continues();

			return Evaluation.of(includes, continues);
		}

		private boolean evalIncludes(Evaluator eval, Path path) {
			return eval != null && eval.evaluate(path).includes();
		}
	}
}