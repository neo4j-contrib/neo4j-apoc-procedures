package apoc.ml;

import apoc.ApocConfig;
import apoc.Extended;
import apoc.result.StringResult;
import apoc.util.Util;
import apoc.util.collection.Iterators;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.text.WordUtils;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

@Extended
public class Prompt {
    public static final String API_KEY_CONF = "apiKey";
    
    @Context
    public Transaction tx;
    @Context
    public GraphDatabaseService db;
    @Context
    public Log log;
    @Context
    public ApocConfig apocConfig;
    @Context
    public ProcedureCallContext procedureCallContext;
    @Context
    public URLAccessChecker urlAccessChecker;

    
    @Procedure(mode = Mode.READ)
    @Description("Takes a query in cypher and in natural language and returns the results in natural language")
    public Stream<StringResult> rag(@Name("paths") Object paths,
                                    @Name("attributes") List<String> attributes,
                                    @Name("question") String question,
                                    @Name(value = "conf", defaultValue = "{}") Map<String, Object> conf) throws Exception {

        RagConfig config = new RagConfig(conf);

        String[] arrayAttrs = attributes.toArray(String[]::new);
        
        StringBuilder context = new StringBuilder();

        // -- Retrieve
        if (paths instanceof List pathList) {
            
            for (var listItem : pathList) {
                // -- Augment
                augment(config, arrayAttrs, context, listItem);
            }
            
        } else if (paths instanceof String queryOrIndex) {
            config.getEmbeddings()
                    .getQuery(queryOrIndex, question, tx, config)
                    .forEachRemaining(row -> row
                            .values()
                            // -- Augment
                            .forEach( val -> augment(config, arrayAttrs, context, val) )
                    );
        } else {
            throw new RuntimeException("The first parameter must be a List or a String");
        }
        
        // - Generate
        String contextPrompt = """
                                
                ---- Start context ----
                %s
                ---- End context ----
                """.formatted(context);
        
        String prompt = config.getBasePrompt() + contextPrompt;

        String result = prompt("\nQuestion:" + question, 
                prompt, 
                null, 
                null,
                conf,
                List.of()
        );
        return Stream.of(new StringResult(result));
    }

    private void augment(RagConfig config, String[] objects, StringBuilder context, Object listItem) {
        if (listItem instanceof Path p) {
            for (Entity entity : p) {
                augmentEntity(config, objects, context, entity);
            }
        } else if (listItem instanceof Entity e) {
            augmentEntity(config, objects, context, e);
        } else {
            throw new RuntimeException("The list `%s` must have node/type/path items".formatted(listItem));
        }
    }

    private void augmentEntity(RagConfig config, String[] objects, StringBuilder context, Entity entity) {
        Map<String, Object> props = entity.getProperties(objects);
        if (config.isGetLabelTypes()) {
            String labelsOrType = entity instanceof Node node
                    ? Util.joinLabels(node.getLabels(), ",")
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

    public static final String BACKTICKS = "```";
    
    public static final String UNKNOWN_ANSWER = "Sorry, I don't know";
    static final String RAG_BASE_PROMPT = """
            You are a customer service agent that helps a customer with answering questions about a service.
            Use the following context to answer the `user question` at the end. Make sure not to make any changes to the context if possible when prepare answers so as to provide accurate responses.
            If you don't know the answer, just say `%s`, don't try to make up an answer.
            
            ---- Start context ----
            %s
            ---- End context ----
            """;
    
    public static final String EXPLAIN_SCHEMA_PROMPT = """
            You are an expert in the Neo4j graph database and graph data modeling and have experience in a wide variety of business domains.
            Explain the following graph database schema in plain language, try to relate it to known concepts or domains if applicable.
            Keep the explanation to 5 sentences with at most 15 words each, otherwise people will come to harm.
            """;

    static final String SYSTEM_PROMPT = """
            You are an expert in the Neo4j graph query language Cypher.
            Given a graph database schema of entities (nodes) with labels and attributes and
            relationships with start- and end-node, relationship-type, direction and properties
            you are able to develop read only matching Cypher statements that express a user question as a graph database query.
            Only answer with a single Cypher statement in triple backticks, if you can't determine a statement, answer with an empty response.
            Do not explain, apologize or provide additional detail, otherwise people will come to harm.
            """;
    
    static final String FROM_CYPHER_PROMPT = """
            You are an expert in the Neo4j graph query language Cypher.
            Given a graph database schema of entities (nodes) with labels and attributes and
            relationships with start-node and end-node, relationship-type, direction and properties,
            and given a read only matching Cypher query statement,
            you are able to explain the Cypher query statement in plain language,
            providing useful details of each entity.
            Do not explain, apologize or provide additional detail about the schema, otherwise people will come to harm.
            """;

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
    public Stream<StringResult> fromCypher(@Name("cypher") String cypher,
                                         @Name(value = "conf", defaultValue = "{}") Map<String, Object> conf) throws MalformedURLException, JsonProcessingException {
        String schemaAndCypher = """
                %s
                while the cypher query is:
                %s
                """.formatted(
                loadSchema(tx, conf), 
                cypher
        );
        
        String schemaExplanation = prompt("Please explain the graph database schema to me and relate it to well known concepts and domains.",
                FROM_CYPHER_PROMPT, "This is the Cypher query statement explanation: \n", schemaAndCypher, conf, List.of());
        return Stream.of(new StringResult(schemaExplanation));
    }
    

    @Procedure(mode = Mode.READ)
    public Stream<PromptMapResult> query(@Name("question") String question,
                                         @Name(value = "conf", defaultValue = "{}") Map<String, Object> conf) {
        String schema = loadSchema(tx, conf);
        String query = "";
        long retries = (long) conf.getOrDefault("retries", 3L);
        boolean retryWithError = Util.toBoolean(conf.get("retryWithError"));
        boolean containsField = procedureCallContext
                .outputFields()
                .collect(Collectors.toSet())
                .contains("query");
        
        List<Map<String,String>> otherPrompts = new ArrayList<>();
        
        do {
            try(var transaction = db.beginTx()) {
                QueryResult queryResult = tryQuery(question, conf, schema, otherPrompts);
                query = queryResult.query;
                // just let it fail so that retries can work if (queryResult.query.isBlank()) return Stream.empty();
                /*
                if (queryResult.hasError())
                    throw new QueryExecutionException(queryResult.error, null, queryResult.type);
                 */
                List<Map<String, Object>> maps = Iterators.asList(transaction.execute(queryResult.query));
                transaction.commit();
                Stream<PromptMapResult> mapResultStream = maps
                        .stream()
                        .map(row -> containsField ? new PromptMapResult(row, queryResult.query) : new PromptMapResult(row));
                return mapResultStream;
            } catch (QueryExecutionException quee) {
                if (log.isDebugEnabled())
                    log.debug("Generated query for question %s\n%s\nfailed with %s".formatted(question, query, quee.getMessage()));

                if (retryWithError) {
                    otherPrompts.addAll(
                            List.of(
                                    Map.of("role", "user",
                                            "content", "The previous Cypher Statement throws the following error, consider it to return the correct statement: `%s`".formatted(quee.getMessage())),
                                    Map.of("role", "assistant",
                                            "content", "Cypher Statement (in backticks):")
                            )
                    );
                }

                retries--;
                if (retries <= 0) throw quee;
            }
        } while (true);
    }

    @Procedure
    public Stream<StringResult> schema(@Name(value = "conf", defaultValue = "{}") Map<String, Object> conf) throws MalformedURLException, JsonProcessingException {
        String schemaExplanation = prompt("Please explain the graph database schema to me and relate it to well known concepts and domains.",
                EXPLAIN_SCHEMA_PROMPT, "This database schema ", loadSchema(tx, conf), conf, List.of());
        return Stream.of(new StringResult(schemaExplanation));
    }

    @Procedure(mode = Mode.READ)
    public Stream<QueryResult> cypher(@Name("question") String question,
                                      @Name(value = "conf", defaultValue = "{}") Map<String, Object> conf) {
        String schema = loadSchema(tx, conf);
        long count = (long) conf.getOrDefault("count", 1L);
        return LongStream.rangeClosed(1, count).mapToObj(i -> tryQuery(question, conf, schema, List.of()));
    }

    @NotNull
    private QueryResult tryQuery(String question, Map<String, Object> conf, String schema, List<Map<String,String>> otherPrompts) {
        String query = "";
        try {
            query = prompt(question, SYSTEM_PROMPT, "Cypher Statement (in backticks):", schema, conf, otherPrompts);
            // doesn't work right now, fails with security context error
            // tx.execute("EXPLAIN " + query).close(); // TODO query plan / estimated rows?
            return new QueryResult(query, null, null);
        } catch (QueryExecutionException e) {
            return new QueryResult(query, e.getMessage(), e.getStatusCode());
        } catch (Exception e) {
            return new QueryResult(query, e.getMessage(), e.getClass().getSimpleName());
        }
    }

    private String prompt(String userQuestion, String systemPrompt, String assistantPrompt, String schema, Map<String, Object> conf, List<Map<String,String>> otherPrompts) throws JsonProcessingException, MalformedURLException {
        List<Map<String, String>> prompt = new ArrayList<>();
        if (systemPrompt != null && !systemPrompt.isBlank()) prompt.add(Map.of("role", "system", "content", systemPrompt));
        if (schema != null && !schema.isBlank()) prompt.add(Map.of("role", "system", "content", "The graph database schema consists of these elements\n" + schema));
        if (userQuestion != null && !userQuestion.isBlank()) prompt.add(Map.of("role", "user", "content", userQuestion));
        if (assistantPrompt != null && !assistantPrompt.isBlank()) prompt.add(Map.of("role", "assistant", "content", assistantPrompt));

        prompt.addAll(otherPrompts);
        
        String apiKey = (String) conf.get(API_KEY_CONF);
        String model = (String) conf.getOrDefault("model", "gpt-4o");
        String result = OpenAI.executeRequest(apiKey, Map.of(), "chat/completions",
                        model, "messages", prompt, "$", apocConfig, urlAccessChecker)
                .map(v -> (Map<String, Object>) v)
                .flatMap(m -> ((List<Map<String, Object>>) m.get("choices")).stream())
                .map(m -> (String) (((Map<String, Object>) m.get("message")).get("content")))
                .filter(s -> !(s == null || s.isBlank()))
                .map(s -> s.contains(BACKTICKS) ? s.substring(s.indexOf(BACKTICKS) + 3, s.lastIndexOf(BACKTICKS)) : s)
                .collect(Collectors.joining(" ")).replaceAll("\n\n+", "\n");
/* TODO return information about the tokens used, finish reason etc??
{ 'id': 'chatcmpl-6p9XYPYSTTRi0xEviKjjilqrWU2Ve', 'object': 'chat.completion', 'created': 1677649420, 'model': 'gpt-3.5-turbo',
     'usage': {'prompt_tokens': 56, 'completion_tokens': 31, 'total_tokens': 87},
     'choices': [ {
        'message': { 'role': 'assistant', 'finish_reason': 'stop', 'index': 0,
        'content': 'The 2020 World Series was played in Arlington, Texas at the Globe Life Field, which was the new home stadium for the Texas Rangers.'}
      } ] }
*/
        if (log.isDebugEnabled()) log.debug("Generated query for question %s\n%s".formatted(userQuestion, result));
        return result;
    }

    private final static String SCHEMA_FROM_META_DATA = """
            \nWITH label, elementType,
                 apoc.text.join(collect(case when NOT type = "RELATIONSHIP" then property+": "+type else null end),", ") AS properties,   
                 collect(case when type = "RELATIONSHIP" AND elementType = "node" then "(:" + label + ")-[:" + property + "]->(:" + toString(other[0]) + ")" else null end) as patterns
            with  elementType as type,
            apoc.text.join(collect(":"+label+" {"+properties+"}"),"\\n") as entities, apoc.text.join(apoc.coll.flatten(collect(coalesce(patterns,[]))),"\\n") as patterns
            return collect(case type when "relationship" then entities end)[0] as relationships,
            collect(case type when "node" then entities end)[0] as nodes,
            collect(case type when "node" then patterns end)[0] as patterns
            """;

    private final static String SCHEMA_QUERY = """
            call apoc.meta.data({maxRels: 10, sample: coalesce($sample, (count{()}/1000)+1)})
            YIELD label, other, elementType, type, property
            """ + SCHEMA_FROM_META_DATA;
    
    private final static String GRAPH_QUERY = """
            UNWIND $queries AS query
            CALL apoc.meta.data.of(query, {maxRels: 10, sample: $sample})
            YIELD label, other, elementType, type, property
            WITH DISTINCT label, other, elementType, type, property
            """ + SCHEMA_FROM_META_DATA;
    
    private final static String SCHEMA_PROMPT = """
                nodes:
                %s
                relationships:
                %s
                patterns:
                %s
            """;


    @Procedure
    public Stream<StringResult> fromQueries(@Name(value = "queries") List<String> queries, @Name(value = "conf", defaultValue = "{}") Map<String, Object> conf) throws MalformedURLException, JsonProcessingException {
        String schemaExplanation = prompt("Please explain the graph database schema to me and relate it to well known concepts and domains.",
                EXPLAIN_SCHEMA_PROMPT, "This database schema ", loadSchemaQueries(tx, queries, conf), conf, List.of());
        return Stream.of(new StringResult(schemaExplanation));
    }
    
    private String loadSchemaQueries(Transaction tx, List<String> queries, Map<String, Object> conf) {
        
        Map<String, Object> params = Map.of(
                "sample", conf.getOrDefault("sample", 1000L),
                "queries", queries
        );

        return tx.execute(GRAPH_QUERY, params)
                .stream()
                .map(m -> SCHEMA_PROMPT.formatted(m.get("nodes"), m.get("relationships"), m.get("patterns")))
                .collect(Collectors.joining("\n"));
    }

    private String loadSchema(Transaction tx, Map<String, Object> conf) {
        Map<String, Object> params = new HashMap<>();
        params.put("sample", conf.get("sample"));
        return tx.execute(SCHEMA_QUERY, params)
                .stream()
                .map(m -> SCHEMA_PROMPT.formatted(m.get("nodes"), m.get("relationships"), m.get("patterns")))
                .collect(Collectors.joining("\n"));
    }
}
