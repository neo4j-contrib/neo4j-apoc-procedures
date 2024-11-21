package apoc.vectordb;

import apoc.Extended;
import apoc.ExtendedSystemPropertyKeys;
import apoc.SystemPropertyKeys;
import apoc.ml.RestAPIConfig;
import apoc.result.ObjectResult;
import apoc.util.JsonUtil;
import apoc.util.SystemDbUtil;
import apoc.util.Util;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.MultipleFoundException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static apoc.util.ExtendedUtil.listOfNumbersToFloatArray;
import static apoc.util.ExtendedUtil.setProperties;
import static apoc.util.JsonUtil.OBJECT_MAPPER;
import static apoc.util.SystemDbUtil.withSystemDb;
import static apoc.vectordb.VectorDbUtil.EmbeddingResult;
import static apoc.vectordb.VectorDbUtil.appendVersionUrlIfNeeded;
import static apoc.vectordb.VectorDbUtil.getEndpoint;
import static apoc.vectordb.VectorEmbeddingConfig.ALL_RESULTS_KEY;
import static apoc.vectordb.VectorEmbeddingConfig.MAPPING_KEY;

/**
 * Base class
 */
@Extended
public class VectorDb {

    @Context
    public URLAccessChecker urlAccessChecker;
    
    @Context
    public GraphDatabaseService db;
    
    @Context
    public Transaction tx;
    
    @Context
    public ProcedureCallContext procedureCallContext;

    /**
     * We can use this procedure with every API that return something like this:
     * ```
     *   [
     *      "idKey": "idValue",
     *      "scoreKey": 1,
     *      "vectorKey": [ ]
     *      "metadataKey": { .. },
     *      "textKey": "..."
     *   ],
     *   [
     *      ...
     *   ]
     * ```
     * 
     * Otherwise, if the result is different (e.g. the Chroma result), we have to leverage the apoc.vectordb.custom,
     * which return an Object, but we can't use it to filter result via `ProcedureCallContext procedureCallContext` 
     * and mapping data to fetch the associated nodes and relationships and optionally create them
     */
    @Procedure(value = "apoc.vectordb.custom.get", mode = Mode.WRITE)
    @Description("apoc.vectordb.custom.get(host, $configuration) - Customizable get / query procedure, which retrieves vectors from the host and the configuration map")
    public Stream<EmbeddingResult> get(@Name("host") String host,
                                       @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {

        getEndpoint(configuration, host);
        VectorEmbeddingConfig restAPIConfig = new VectorEmbeddingConfig(configuration);
        return getEmbeddingResultStream(restAPIConfig, procedureCallContext, urlAccessChecker, tx);
    }

    public static Stream<EmbeddingResult> getEmbeddingResultStream(VectorEmbeddingConfig conf,
                                                                   ProcedureCallContext procedureCallContext,
                                                                   URLAccessChecker urlAccessChecker,
                                                                   Transaction tx) throws Exception {
        return getEmbeddingResultStream(conf, procedureCallContext, urlAccessChecker, tx, v -> ((List<Map>) v).stream());
    }
    
    public static Stream<EmbeddingResult> getEmbeddingResultStream(VectorEmbeddingConfig conf,
                                                                   ProcedureCallContext procedureCallContext,
                                                                   URLAccessChecker urlAccessChecker,
                                                                   Transaction tx,
                                                                   Function<Object, Stream<Map>> objectMapper) throws Exception {
        List<String> fields = procedureCallContext.outputFields().toList();

        boolean hasVector = fields.contains("vector") && conf.isAllResults();
        boolean hasMetadata = fields.contains("metadata");

        VectorMappingConfig mapping = conf.getMapping();

        return executeRequest(conf.getApiConfig(), urlAccessChecker)
                .flatMap(objectMapper)
                .map(m -> getEmbeddingResult(conf, tx, hasVector, hasMetadata, mapping, m));
    }

    public static EmbeddingResult getEmbeddingResult(VectorEmbeddingConfig conf, Transaction tx, boolean hasEmbedding, boolean hasMetadata, VectorMappingConfig mapping, Map m) {
        Object id = conf.isAllResults() ? m.get(conf.getIdKey()) : null;
        List<Double> embedding = hasEmbedding ? (List<Double>) m.get(conf.getVectorKey()) : null;
        Map<String, Object> metadata = hasMetadata ? (Map<String, Object>) m.get(conf.getMetadataKey()) : null;
        // in case of get operation, e.g. http://localhost:52798/collections/{coll_name}/points with Qdrant db,
        // score is not present
        Double score = Util.toDouble(m.get(conf.getScoreKey()));
        String text = conf.isAllResults() ? (String) m.get(conf.getTextKey()) : null;

        Entity entity = handleMapping(tx, mapping, metadata, embedding);
        if (entity != null) entity = Util.rebind(tx, entity);
        return new EmbeddingResult(id, score, embedding, metadata, text,
                mapping.getNodeLabel() == null ? null : (Node) entity,
                mapping.getNodeLabel() != null ? null : (Relationship) entity
        );
    }

    private static Entity handleMapping(Transaction tx, VectorMappingConfig mapping, Map<String, Object> metadata, List<Double> embedding) {
        if (mapping.getEntityKey() == null) {
            return null;
        }
        if (MapUtils.isEmpty(metadata)) {
            throw new RuntimeException("To use mapping config, the metadata should not be empty. Make sure you execute `YIELD metadata` on the procedure");
        }
        Map<String, Object> metaProps = new HashMap<>(metadata);
        if (mapping.getNodeLabel() != null) {
            return handleMappingNode(tx, mapping, metaProps, embedding);
        } else if (mapping.getRelType() != null) {
            return handleMappingRel(tx, mapping, metaProps, embedding);
        } else {
            throw new RuntimeException("Mapping conf has to contain either label or type key");
        }
    }

    private static Entity handleMappingNode(Transaction transaction, VectorMappingConfig mapping, Map<String, Object> metaProps, List<Double> embedding) {
        try {
            Node node;
            Object propValue = metaProps.get(mapping.getMetadataKey());
            node = transaction.findNode(Label.label(mapping.getNodeLabel()), mapping.getEntityKey(), propValue);
            switch (mapping.getMode()) {
                case READ_ONLY -> {
                    // do nothing, just return the entity
                }
                case UPDATE_EXISTING -> {
                    setPropsIfEntityExists(mapping, metaProps, embedding, node);
                }
                case CREATE_IF_MISSING -> {
                    if (node == null) {
                        node = transaction.createNode(Label.label(mapping.getNodeLabel()));
                        node.setProperty(mapping.getEntityKey(), propValue);
                    }
                    setPropsIfEntityExists(mapping, metaProps, embedding, node);
                }
            }
            return node;
        } catch (MultipleFoundException e) {
            throw new RuntimeException("Multiple nodes found");
        }
    }

    private static Entity handleMappingRel(Transaction transaction, VectorMappingConfig mapping, Map<String, Object> metaProps, List<Double> embedding) {
        try {
            // in this case we cannot auto-create the rel, since we should have to define start and end node as well
            Relationship rel;
            Object propValue = metaProps.get(mapping.getMetadataKey());
            rel = transaction.findRelationship(RelationshipType.withName(mapping.getRelType()), mapping.getEntityKey(), propValue);
            switch (mapping.getMode()) {
                case READ_ONLY -> {
                    // do nothing, just return the entity
                }
                case UPDATE_EXISTING, CREATE_IF_MISSING -> {
                    setPropsIfEntityExists(mapping, metaProps, embedding, rel);
                }
            }

            return rel;
        } catch (MultipleFoundException e) {
            throw new RuntimeException("Multiple relationships found");
        }
    }

    private static void setPropsIfEntityExists(VectorMappingConfig mapping, Map<String, Object> metaProps, List<Double> embedding, Entity entity) {
        if (entity != null) {
            setProperties(entity, metaProps);
            setVectorProp(mapping, embedding, entity);
        }
    }

    private static <T extends Entity> void setVectorProp(VectorMappingConfig mapping, List<Double> embedding, T entity) {
        if (mapping.getEmbeddingKey() == null) {
            return;
        }

        if (embedding == null) {
            String embeddingErrMsg = "The embedding value is null. Make sure you execute `YIELD embedding` on the procedure and you configured `%s: true`"
                    .formatted(ALL_RESULTS_KEY);
            throw new RuntimeException(embeddingErrMsg);
        }

        float[] floats = listOfNumbersToFloatArray(embedding);
        entity.setProperty(mapping.getEmbeddingKey(), floats);
    }
    
    // TODO - evaluate. It could be renamed e.g. to `apoc.util.restapi.custom` or `apoc.restapi.custom`,
    //      since it can potentially be used as a generic method to call any RestAPI 
    @Procedure("apoc.vectordb.custom")
    @Description("apoc.vectordb.custom(host, $configuration) - fully customizable procedure, returns generic object results")
    public Stream<ObjectResult> custom(@Name("host") String host, @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {

        getEndpoint(configuration, host);
        RestAPIConfig restAPIConfig = new RestAPIConfig(configuration);
        return executeRequest(restAPIConfig, urlAccessChecker)
                .map(ObjectResult::new);
    }

    public static Stream<Object> executeRequest(RestAPIConfig apiConfig, URLAccessChecker urlAccessChecker) throws Exception {
        Map<String, Object> headers = apiConfig.getHeaders();
        Map<String, Object> configBody = apiConfig.getBody();
        String body = configBody == null
                ? null
                : OBJECT_MAPPER.writeValueAsString(configBody);
        
        String endpoint = apiConfig.getEndpoint();
        if (endpoint == null) {
            throw new RuntimeException("Endpoint must be specified");
        }

        return JsonUtil.loadJson(endpoint, headers, body, apiConfig.getJsonPath(), true, List.of(), urlAccessChecker);
    }

    @Admin
    @SystemProcedure
    @Procedure(name = "apoc.vectordb.configure")
    @Description("CALL apoc.vectordb.configure(vectorName, host, credentialsValue, mapping) - To configure, given the vector defined by the 1st parameter, `host`, `credentials` and `mapping` into the system db")
    public void vectordb(
            @Name("vectorName") String vectorName, @Name("configKey") String configKey, @Name("databaseName") String databaseName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        SystemDbUtil.checkInSystemLeader(db);
        SystemDbUtil.checkTargetDatabase(tx, databaseName, "Vector DB configuration");
        
        VectorDbHandler.Type type = VectorDbHandler.Type.valueOf( vectorName.toUpperCase() );

        withSystemDb(transaction -> {
            Label label = Label.label(type.get().getLabel());
            Node node = Util.mergeNode(transaction, label, null, Pair.of(SystemPropertyKeys.name.name(), configKey));

            Map mapping = (Map) config.get("mapping");
            String host = appendVersionUrlIfNeeded(type, (String) config.get("host"));
            Object credentials = config.get("credentials");

            if (host != null) {
                node.setProperty(ExtendedSystemPropertyKeys.host.name(), host);
            }
            
            if (credentials != null) {
                node.setProperty( ExtendedSystemPropertyKeys.credentials.name(), Util.toJson(credentials) );
            }

            if (mapping != null) {
                node.setProperty( MAPPING_KEY, Util.toJson(mapping) );
            }
        });

    }
}
