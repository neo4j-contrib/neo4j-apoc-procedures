package apoc.search;

import apoc.Description;
import apoc.result.NodeResult;
import apoc.util.Util;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.groupingBy;

public class ParallelNodeSearch {

    private final static Set<String> OPERATORS = new HashSet<>(asList("exact","starts with", "ends with", "contains", "<", ">", "=", "<>", "<=", ">=", "=~"));

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;


    @Procedure("apoc.search.nodeAllReduced")
    @Description("Do a parallel search over multiple indexes returning a reduced representation of the nodes found: node id, labels and the searched property. apoc.search.nodeShortAll( map of label and properties which will be searched upon, operator: EXACT / CONTAINS / STARTS WITH | ENDS WITH / = / <> / < / > ..., value ). All 'hits' are returned.")
    public Stream<NodeReducedResult> multiSearchAll(@Name("LabelPropertyMap") final Object labelProperties, @Name("operator") final String operator, @Name("value") final Object value) throws Exception {
        return createWorkersFromValidInput(labelProperties, operator, value).flatMap(QueryWorker::queryForData);
    }


    private NodeReducedResult merge(NodeReducedResult a, NodeReducedResult b) {
        a.values.putAll(b.values);
        for (String label : b.labels)
            if (!a.labels.contains(label))
                a.labels.add(label);
        return a;
    }

    @Procedure("apoc.search.nodeReduced")
    @Description("Do a parallel search over multiple indexes returning a reduced representation of the nodes found: node id, labels and the searched properties. apoc.search.nodeReduced( map of label and properties which will be searched upon, operator: EXACT | CONTAINS | STARTS WITH | ENDS WITH, searchValue ). Multiple search results for the same node are merged into one record.")
    public Stream<NodeReducedResult> multiSearch(@Name("LabelPropertyMap") final Object labelProperties, @Name("operator") final String operator, @Name("value") final String value) throws Exception {
        return createWorkersFromValidInput(labelProperties, operator, value)
                    .flatMap(QueryWorker::queryForData)
                    .collect(groupingBy(res -> res.id,Collectors.reducing(this::merge)))
                    .values().stream().filter(Optional::isPresent).map(Optional::get);
    }

    @Procedure("apoc.search.multiSearchReduced")
    @Description("Do a parallel search over multiple indexes returning a reduced representation of the nodes found: node id, labels and the searched properties. apoc.search.multiSearchReduced( map of label and properties which will be searched upon, operator: EXACT | CONTAINS | STARTS WITH | ENDS WITH, searchValue ). Multiple search results for the same node are merged into one record.")
    public Stream<NodeReducedResult> multiSearchOld(@Name("LabelPropertyMap") final Object labelProperties, @Name("operator") final String operator, @Name("value") final String value) throws Exception {
            return createWorkersFromValidInput(labelProperties, operator, value)
                    .flatMap(QueryWorker::queryForData)
                    .collect(groupingBy(res -> res.id))
                    .values().stream().map( list -> list.stream().reduce( this::merge ))
                    .filter(Optional::isPresent).map(Optional::get);
    }

    @Procedure("apoc.search.nodeAll")
    @Description("Do a parallel search over multiple indexes returning nodes. usage apoc.search.nodeAll( map of label and properties which will be searched upon, operator: EXACT | CONTAINS | STARTS WITH | ENDS WITH, searchValue ) returns all the Nodes found in the different searches.")
    public Stream<NodeResult> multiSearchNodeAll(@Name("LabelPropertyMap") final Object labelProperties, @Name("operator") final String operator, @Name("value") final String value) throws Exception {
        return createWorkersFromValidInput(labelProperties, operator, value).flatMap(QueryWorker::queryForNode);
    }


    @Procedure("apoc.search.node")
    @Description("Do a parallel search over multiple indexes returning nodes. usage apoc.search.node( map of label and properties which will be searched upon, operator: EXACT | CONTAINS | STARTS WITH | ENDS WITH, searchValue ) returns all the DISTINCT Nodes found in the different searches.")
    public Stream<NodeResult> multiSearchNode(@Name("LabelPropertyMap") final Object labelProperties, @Name("operator") final String operator, @Name("value") final String value) throws Exception {
        return createWorkersFromValidInput(labelProperties, operator, value)
                .flatMap(QueryWorker::queryForNode)
                .distinct();
    }


    private Stream<QueryWorker> createWorkersFromValidInput(final Object labelPropertiesInput, String operatorInput, final Object value) throws Exception {
        String operatorNormalized = operatorInput.trim().toLowerCase();
        if (operatorInput == null || !OPERATORS.contains(operatorNormalized)) {
            throw new Exception(format("operator `%s` invalid, it must have one of the following values (case insensitive): %s.", operatorInput, OPERATORS));
        }
        String operator = operatorNormalized.equals("exact") ? "=" : operatorNormalized;

        if (labelPropertiesInput == null || labelPropertiesInput instanceof String && labelPropertiesInput.toString().trim().isEmpty()) {
            throw new Exception("LabelProperties cannot be empty. example { Person: [\"fullName\",\"lastName\"],Company:\"name\", Event : \"Description\"}");
        }
        Map<String, Object> labelProperties = labelPropertiesInput instanceof Map ? (Map<String, Object>) labelPropertiesInput : Util.readMap(labelPropertiesInput.toString());

        return labelProperties.entrySet().parallelStream().flatMap(e -> {
            String label = e.getKey();
            Object properties = e.getValue();
            if (properties instanceof String) {
                return Stream.of(new QueryWorker(api, label, (String) properties, operator, value, log));
            } else if (properties instanceof List) {
                return ((List<String>) properties).stream().map(prop -> new QueryWorker(api, label, prop, operator, value, log));
            }
            throw new RuntimeException("Invalid type for properties " + properties + ": " + (properties == null ? "null" : properties.getClass()));
        });
    }

    public static class QueryWorker {
        private GraphDatabaseAPI db;
        private String label, prop, operator;
        Object value;
        private Log log;

        public QueryWorker(GraphDatabaseAPI db, String label, String prop, String operator, Object value, Log log) {
            this.db = db;
            this.label = label;
            this.prop = prop;
            this.value = value;
            this.operator = operator;
            this.log = log;
        }

        public Stream<NodeReducedResult> queryForData() {
            List<String> labels = singletonList(label);
            String query = format("match (n:`%s`) where n.`%s` %s {value} return id(n) as id,  n.`%s` as value", label, prop, operator, prop);
            return queryForNode(query, (row) -> new NodeReducedResult((long) row.get("id"), labels, singletonMap(prop, row.get("value")))).stream();
        }

        public Stream<NodeResult> queryForNode() {
            String query = format("match (n:`%s`) where n.`%s` %s {value} return n", label, prop, operator);
            return queryForNode(query, (row) -> new NodeResult((Node) row.get("n"))).stream();
        }

        public <T> List<T> queryForNode(String query, Function<Map<String, Object>, T> transformer) {
            long start = currentTimeMillis();
            try (Transaction tx = db.beginTx()) {
                try (Result nodes = db.execute(query, singletonMap("value", value))) {
                    return nodes.stream().map(transformer).collect(Collectors.toList());
                } finally {
                    tx.success();
                    if (log.isDebugEnabled())
                        log.debug(format("(%s) search on label:%s and prop:%s took %d",
                                Thread.currentThread(), label, prop, currentTimeMillis() - start));
                }
            }
        }
    }

    public static class NodeReducedResult {
        public final long id;
        public final List<String> labels;
        public final Map<String, Object> values;

        public NodeReducedResult(long id, List<String> labels, Map<String, Object> val) {
            this.labels = labels;
            this.id = id;
            this.values = val;
        }

    }
}
