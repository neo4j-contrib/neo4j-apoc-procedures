package apoc.vectordb;

import apoc.ml.RestAPIConfig;
import apoc.result.ObjectResult;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.apache.commons.collections4.MapUtils;
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

import static apoc.util.ExtendedUtil.setProperties;
import static apoc.util.JsonUtil.OBJECT_MAPPER;
import static apoc.vectordb.VectorDbUtil.*;

/**
 * Base class
 */
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
     *      "embeddingKey": [ ]
     *      "metadataKey": { .. },
     *      "textKey": "..."
     *   ],
     *   [
     *      ...
     *   ]
     * ```
     * 
     * Otherwise, if the result is different (e.g. the Chroma result), we have to leverage the apoc.vectordb.custom,
     * which retrurn an Object, but we can't use it to filter result via `ProcedureCallContext procedureCallContext` 
     * and mapping data to auto-create neo4j vector indexes and properties
     */
    @Procedure(value = "apoc.vectordb.custom.get", mode = Mode.SCHEMA)
    @Description("apoc.vectordb.custom.get(host, $configuration) - Customizable get / query procedure")
    public Stream<EmbeddingResult> get(@Name("host") String host,
                                       @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {

        getEndpoint(configuration, host);
        VectorEmbeddingConfig restAPIConfig = new VectorEmbeddingConfig(configuration, Map.of(), Map.of());
        return getEmbeddingResultStream(restAPIConfig, procedureCallContext, urlAccessChecker, db, tx);
    }

    public static Stream<EmbeddingResult> getEmbeddingResultStream(VectorEmbeddingConfig conf,
                                                                   ProcedureCallContext procedureCallContext,
                                                                   URLAccessChecker urlAccessChecker,
                                                                   GraphDatabaseService db,
                                                                   Transaction tx) throws Exception {
        return getEmbeddingResultStream(conf, procedureCallContext, urlAccessChecker, db, tx, v -> ((List<Map>) v).stream());
    }
    
    public static Stream<EmbeddingResult> getEmbeddingResultStream(VectorEmbeddingConfig conf,
                                                                   ProcedureCallContext procedureCallContext,
                                                                   URLAccessChecker urlAccessChecker,
                                                                   GraphDatabaseService db,
                                                                   Transaction tx,
                                                                   Function<Object, Stream<Map>> objectMapper) throws Exception {
        List<String> fields = procedureCallContext.outputFields().toList();

        boolean hasEmbedding = fields.contains("vector");
        boolean hasMetadata = fields.contains("metadata");
        Stream<Object> resultStream = executeRequest(conf, urlAccessChecker);

        VectorMappingConfig mapping = conf.getMapping();

        return resultStream
                .flatMap(objectMapper)
                .map(m -> {
                    Object id = m.get(conf.getIdKey());
                    List<Double> embedding = hasEmbedding ? (List<Double>) m.get(conf.getVectorKey()) : null;
                    Map<String, Object> metadata = hasMetadata ? (Map<String, Object>) m.get(conf.getMetadataKey()) : null;
                    // in case of get operation, e.g. http://localhost:52798/collections/{coll_name}/points with Qdrant db,
                    // score is not present
                    Double score = Util.toDouble(m.get(conf.getScoreKey()));
                    String text = (String) m.get(conf.getTextKey());

                    handleMapping(tx, db, mapping, metadata, embedding);
                    return new EmbeddingResult(id, score, embedding, metadata, text);
                });
    }

    private static void handleMapping(Transaction tx, GraphDatabaseService db, VectorMappingConfig mapping, Map<String, Object> metadata, List<Double> embedding) {
        if (mapping.getProp() == null) {
            return;
        }
        if (MapUtils.isEmpty(metadata)) {
            throw new RuntimeException("To use mapping config, the metadata should not be empty. Make sure you execute `YIELD metadata` on the procedure");
        }
        Map<String, Object> metaProps = new HashMap<>(metadata);
        if (mapping.getLabel() != null) {
            handleMappingNode(tx, db, mapping, metaProps, embedding);
        } else if (mapping.getType() != null) {
            handleMappingRel(tx, db, mapping, metaProps, embedding);
        } else {
            throw new RuntimeException("Mapping conf has to contain either label or type key");
        }
    }

    private static void handleMappingNode(Transaction tx, GraphDatabaseService db, VectorMappingConfig mapping, Map<String, Object> metaProps, List<Double> embedding) {
        String query = "CREATE CONSTRAINT IF NOT EXISTS FOR (n:%s) REQUIRE n.%s IS UNIQUE"
                .formatted(mapping.getLabel(), mapping.getProp());
        db.executeTransactionally(query);

        try {
            Node node;
            try (Transaction transaction = db.beginTx()) {
                Object propValue = metaProps.remove(mapping.getId());
                node = transaction.findNode(Label.label(mapping.getLabel()), mapping.getProp(), propValue);
                if (node == null && mapping.isCreate()) {
                    node = transaction.createNode(Label.label(mapping.getLabel()));
                }
                if (node != null) {
                    setProperties(node, metaProps);
                }
                transaction.commit();
            }

            String indexQuery = "CREATE VECTOR INDEX IF NOT EXISTS FOR (n:%s) ON (n.%s) OPTIONS {indexConfig: {`vector.dimensions`: %s, `vector.similarity_function`: '%s'}}";
            String setVectorQuery = "CALL db.create.setNodeVectorProperty($entity, $key, $vector)";
            setVectorProp(tx, db, mapping, embedding, node, indexQuery, setVectorQuery);

        } catch (MultipleFoundException e) {
            throw new RuntimeException("Multiple nodes found");
        }
    }

    private static void handleMappingRel(Transaction tx, GraphDatabaseService db, VectorMappingConfig mapping, Map<String, Object> metaProps, List<Double> embedding) {
        try {
            String query = "CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:%s]-() REQUIRE (r.%s) IS UNIQUE"
                    .formatted(mapping.getType(), mapping.getProp());
            db.executeTransactionally(query);

            // in this case we cannot auto-create the rel, since we should have to define start and end node as well
            Relationship rel;
            try (Transaction transaction = db.beginTx()) {
                Object propValue = metaProps.remove(mapping.getId());
                rel = transaction.findRelationship(RelationshipType.withName(mapping.getType()), mapping.getProp(), propValue);
                if (rel != null) {
                    setProperties(rel, metaProps);
                }
                transaction.commit();
            }

            String indexQuery ="CREATE VECTOR INDEX IF NOT EXISTS FOR ()-[r:%s]-() ON (r.%s) OPTIONS {indexConfig: {`vector.dimensions`: %s, `vector.similarity_function`: '%s'}}";
            String setVectorQuery = "CALL db.create.setRelationshipVectorProperty($entity, $key, $vector)";
            setVectorProp(tx, db, mapping, embedding, rel, indexQuery, setVectorQuery);

        } catch (MultipleFoundException e) {
            throw new RuntimeException("Multiple relationships found");
        }
    }

    private static <T extends Entity> void setVectorProp(Transaction tx, GraphDatabaseService db, VectorMappingConfig mapping, List<Double> embedding, T entity, String indexQuery, String setVectorQuery) {
        if (entity == null || mapping.getEmbeddingProp() == null) {
            return;
        }

        if (embedding == null) {
            throw new RuntimeException("The embedding value is null. Make sure you execute `YIELD embedding` on the procedure");
        }

        String labelOrType = entity instanceof Node
                ? mapping.getLabel()
                : mapping.getType();
        String vectorIndex = indexQuery
                .formatted(labelOrType, mapping.getEmbeddingProp(), embedding.size(), mapping.getSimilarity());
        db.executeTransactionally(vectorIndex);
        db.executeTransactionally(setVectorQuery,
                Map.of("entity", Util.rebind(tx, entity), "key", mapping.getEmbeddingProp(), "vector", embedding));
    }
    
    // TODO - evaluate. It could be renamed e.g. to `apoc.util.restapi.custom` or `apoc.restapi.custom`,
    //      since it can potentially be used as a generic method to call any RestAPI 
    @Procedure("apoc.vectordb.custom")
    @Description("apoc.vectordb.custom(host, $config) - fully customizable vector db procedure, returns generic object results")
    public Stream<ObjectResult> custom(@Name("host") String host, @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {

        getEndpoint(configuration, host);
        RestAPIConfig restAPIConfig = new RestAPIConfig(configuration);
        return executeRequest(restAPIConfig, urlAccessChecker)
                .map(ObjectResult::new);
    }

    public static Stream<Object> executeRequest(RestAPIConfig apiConfig, URLAccessChecker urlAccessChecker) throws Exception {
        Map<String, Object> headers = apiConfig.getHeaders();
        String body = OBJECT_MAPPER.writeValueAsString(apiConfig.getBody());
        return JsonUtil.loadJson(apiConfig.getEndpoint(), headers, body, apiConfig.getJsonPath(), true, List.of(), urlAccessChecker);
    }
}
