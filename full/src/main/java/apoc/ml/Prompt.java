package apoc.ml;

import apoc.ApocConfig;
import apoc.Description;
import apoc.Extended;
import apoc.result.StringResult;
import apoc.util.Util;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.commons.text.WordUtils;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

@Extended
public class Prompt {
    public static final String API_KEY_CONF = "apiKey";

    @Context
    public Transaction tx;

    @Context
    public Log log;

    @Context
    public ApocConfig apocConfig;

    @Context
    public ProcedureCallContext procedureCallContext;

    public static final String BACKTICKS = "```";
    public static final String EXPLAIN_SCHEMA_PROMPT =
            "You are an expert in the Neo4j graph database and graph data modeling and have experience in a wide variety of business domains.\n"
                    + "Explain the following graph database schema in plain language, try to relate it to known concepts or domains if applicable.\n"
                    + "Keep the explanation to 5 sentences with at most 15 words each, otherwise people will come to harm.\n";

    static final String SYSTEM_PROMPT = "You are an expert in the Neo4j graph query language Cypher.\n"
            + "Given a graph database schema of entities (nodes) with labels and attributes and\n"
            + "relationships with start- and end-node, relationship-type, direction and properties\n"
            + "you are able to develop read only matching Cypher statements that express a user question as a graph database query.\n"
            + "Only answer with a single Cypher statement in triple backticks, if you can't determine a statement, answer with an empty response.\n"
            + "Do not explain, apologize or provide additional detail, otherwise people will come to harm.\n";

    public class PromptMapResult {
        public final Map<String, Object> value;
        public final String query;

        public PromptMapResult(Map<String, Object> value, String query) {
            this.value = value;
            this.query = query;
        }

        public PromptMapResult(Map<String, Object> value) {
            this.value = value;
            this.query = null;
        }
    }

    public class QueryResult {
        public final String query;
        // todo re-add when it's actually working
        // private final String error;
        // private final String type;

        public QueryResult(String query, String error, String type) {
            this.query = query;
            // this.error = error;
            // this.type = type;
        }

        public boolean hasError() {
            return false;
            // return error != null && !error.isBlank();
        }
    }

    @Procedure(mode = Mode.READ)
    @Description("Takes a query in cypher and in natural language and returns the results in natural language")
    public Stream<StringResult> rag(
            @Name("paths") Object paths,
            @Name("attributes") List<String> attributes,
            @Name("question") String question,
            @Name(value = "conf", defaultValue = "{}") Map<String, Object> conf)
            throws Exception {
        RagConfig config = new RagConfig(conf);
        String[] arrayAttrs = attributes.toArray(String[]::new);

        StringBuilder context = new StringBuilder();
        // -- Retrieve
        if (paths instanceof List) {
            List pathList = (List) paths;

            for (var listItem : pathList) {
                // -- Augment
                augment(config, arrayAttrs, context, listItem);
            }

        } else if (paths instanceof String) {
            String queryOrIndex = (String) paths;
            config.getEmbeddings().getQuery(queryOrIndex, question, tx, config).forEachRemaining(row -> row.values()
                    // -- Augment
                    .forEach(val -> augment(config, arrayAttrs, context, val)));
        } else {
            throw new RuntimeException("The first parameter must be a List or a String");
        }

        // - Generate
        String contextPrompt = String.format(
                "                                \n" + "                ---- Start context ----\n"
                        + "                %s\n"
                        + "                ---- End context ----",
                context);

        String prompt = config.getBasePrompt() + contextPrompt;
        String result = prompt("\nQuestion:" + question, prompt, null, null, conf);
        return Stream.of(new StringResult(result));
    }

    private void augment(RagConfig config, String[] objects, StringBuilder context, Object listItem) {
        if (listItem instanceof Path) {
            Path p = (Path) listItem;
            for (Entity entity : p) {
                augmentEntity(config, objects, context, entity);
            }
        } else if (listItem instanceof Entity) {
            Entity e = (Entity) listItem;
            augmentEntity(config, objects, context, e);
        } else {
            throw new RuntimeException(String.format("The list `%s` must have node/type/path items", listItem));
        }
    }

    private void augmentEntity(RagConfig config, String[] objects, StringBuilder context, Entity entity) {
        Map<String, Object> props = entity.getProperties(objects);
        if (config.isGetLabelTypes()) {
            String labelsOrType = entity instanceof Node
                    ? Util.joinLabels(((Node) entity).getLabels(), ",")
                    : ((Relationship) entity).getType().name();
            labelsOrType = WordUtils.capitalize(labelsOrType, '_');
            props.put("context description", labelsOrType);
        }
        String obj = props.entrySet().stream()
                .filter(i -> i.getValue() != null)
                .map(i -> i.getKey() + ": " + i.getValue() + "\n")
                .collect(Collectors.joining("\n---\n"));
        context.append(obj);
    }

    @Procedure(mode = Mode.READ)
    public Stream<PromptMapResult> query(
            @Name("question") String question, @Name(value = "conf", defaultValue = "{}") Map<String, Object> conf) {
        String schema = loadSchema(tx, conf);
        String query = "";
        long retries = (long) conf.getOrDefault("retries", 3L);
        boolean containsField =
                procedureCallContext.outputFields().collect(Collectors.toSet()).contains("query");
        do {
            try {
                QueryResult queryResult = tryQuery(question, conf, schema);
                query = queryResult.query;
                // just let it fail so that retries can work if (queryResult.query.isBlank()) return Stream.empty();
                /*
                if (queryResult.hasError())
                    throw new QueryExecutionException(queryResult.error, null, queryResult.type);
                 */
                return tx.execute(queryResult.query).stream()
                        .map(row ->
                                containsField ? new PromptMapResult(row, queryResult.query) : new PromptMapResult(row));
            } catch (QueryExecutionException quee) {
                if (log.isDebugEnabled())
                    log.debug(String.format(
                            "Generated query for question %s\n%s\nfailed with %s", question, query, quee.getMessage()));
                retries--;
                if (retries <= 0) throw quee;
            }
        } while (true);
    }

    @Procedure
    public Stream<StringResult> schema(@Name(value = "conf", defaultValue = "{}") Map<String, Object> conf)
            throws MalformedURLException, JsonProcessingException {
        String schemaExplanation = prompt(
                "Please explain the graph database schema to me and relate it to well known concepts and domains.",
                EXPLAIN_SCHEMA_PROMPT,
                "This database schema ",
                loadSchema(tx, conf),
                conf);
        return Stream.of(new StringResult(schemaExplanation));
    }

    @Procedure(mode = Mode.READ)
    public Stream<QueryResult> cypher(
            @Name("question") String question, @Name(value = "conf", defaultValue = "{}") Map<String, Object> conf) {
        String schema = loadSchema(tx, conf);
        long count = (long) conf.getOrDefault("count", 1L);
        return LongStream.rangeClosed(1, count).mapToObj(i -> tryQuery(question, conf, schema));
    }

    @NotNull
    private QueryResult tryQuery(String question, Map<String, Object> conf, String schema) {
        String query = "";
        try {
            query = prompt(question, SYSTEM_PROMPT, "Cypher Statement (in backticks):", schema, conf);
            // doesn't work right now, fails with security context error
            // tx.execute("EXPLAIN " + query).close(); // TODO query plan / estimated rows?
            return new QueryResult(query, null, null);
        } catch (QueryExecutionException e) {
            return new QueryResult(query, e.getMessage(), e.getStatusCode());
        } catch (Exception e) {
            return new QueryResult(query, e.getMessage(), e.getClass().getSimpleName());
        }
    }

    @NotNull
    private String prompt(
            String userQuestion, String systemPrompt, String assistantPrompt, String schema, Map<String, Object> conf)
            throws JsonProcessingException, MalformedURLException {
        List<Map<String, String>> prompt = new ArrayList<>();
        if (systemPrompt != null && !systemPrompt.isBlank())
            prompt.add(Map.of("role", "system", "content", systemPrompt));
        if (schema != null && !schema.isBlank())
            prompt.add(Map.of(
                    "role", "system", "content", "The graph database schema consists of these elements\n" + schema));
        if (userQuestion != null && !userQuestion.isBlank())
            prompt.add(Map.of("role", "user", "content", userQuestion));
        if (assistantPrompt != null && !assistantPrompt.isBlank())
            prompt.add(Map.of("role", "assistant", "content", assistantPrompt));
        String apiKey = (String) conf.get(API_KEY_CONF);
        String model = (String) conf.getOrDefault("model", "gpt-4o");
        String result = OpenAI.executeRequest(
                        apiKey, Map.of(), "chat/completions", model, "messages", prompt, "$", apocConfig)
                .map(v -> (Map<String, Object>) v)
                .flatMap(m -> ((List<Map<String, Object>>) m.get("choices")).stream())
                .map(m -> (String) (((Map<String, Object>) m.get("message")).get("content")))
                .filter(s -> !(s == null || s.isBlank()))
                .map(s -> s.contains(BACKTICKS) ? s.substring(s.indexOf(BACKTICKS) + 3, s.lastIndexOf(BACKTICKS)) : s)
                .collect(Collectors.joining(" "))
                .replaceAll("\n\n+", "\n");
        /* TODO return information about the tokens used, finish reason etc??
        { 'id': 'chatcmpl-6p9XYPYSTTRi0xEviKjjilqrWU2Ve', 'object': 'chat.completion', 'created': 1677649420, 'model': 'gpt-3.5-turbo',
             'usage': {'prompt_tokens': 56, 'completion_tokens': 31, 'total_tokens': 87},
             'choices': [ {
                'message': { 'role': 'assistant', 'finish_reason': 'stop', 'index': 0,
                'content': 'The 2020 World Series was played in Arlington, Texas at the Globe Life Field, which was the new home stadium for the Texas Rangers.'}
              } ] }
        */
        if (log.isDebugEnabled()) log.debug(String.format("Generated query for question %s\n%s", userQuestion, result));
        return result;
    }

    public static final String UNKNOWN_ANSWER = "Sorry, I don't know";
    static final String RAG_BASE_PROMPT =
            "You are a customer service agent that helps a customer with answering questions about a service.\n"
                    + "Use the following context to answer the `user question` at the end. Make sure not to make any changes to the context if possible when prepare answers so as to provide accurate responses.\n"
                    + "If you don't know the answer, just say `%s`, don't try to make up an answer.\n"
                    + "\n"
                    + "---- Start context ----\n"
                    + "%s\n"
                    + "---- End context ----";

    private static final String SCHEMA_QUERY =
            "call apoc.meta.data({maxRels: 10, sample: coalesce($sample, (count{()}/1000)+1)})\n"
                    + "YIELD label, other, elementType, type, property\n"
                    + "WITH label, elementType, \n"
                    + "     apoc.text.join(collect(case when NOT type = \"RELATIONSHIP\" then property+\": \"+type else null end),\", \") AS properties,    \n"
                    + "     collect(case when type = \"RELATIONSHIP\" AND elementType = \"node\" then \"(:\" + label + \")-[:\" + property + \"]->(:\" + toString(other[0]) + \")\" else null end) as patterns\n"
                    + "with  elementType as type, \n"
                    + "apoc.text.join(collect(\":\"+label+\" {\"+properties+\"}\"),\"\\n\") as entities, apoc.text.join(apoc.coll.flatten(collect(coalesce(patterns,[]))),\"\\n\") as patterns\n"
                    + "return collect(case type when \"relationship\" then entities end)[0] as relationships, \n"
                    + "collect(case type when \"node\" then entities end)[0] as nodes, \n"
                    + "collect(case type when \"node\" then patterns end)[0] as patterns \n";

    private static final String SCHEMA_PROMPT = "nodes:\n %s\n" + "relationships:\n %s\n" + "patterns: %s";

    private String loadSchema(Transaction tx, Map<String, Object> conf) {
        Map<String, Object> params = new HashMap<>();
        params.put("sample", conf.get("sample"));
        return tx.execute(SCHEMA_QUERY, params).stream()
                .map(m -> String.format(SCHEMA_PROMPT, m.get("nodes"), m.get("relationships"), m.get("patterns")))
                .collect(Collectors.joining("\n"));
    }
}
