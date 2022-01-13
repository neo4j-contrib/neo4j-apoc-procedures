package apoc.mongodb;

import apoc.Extended;
import apoc.result.LongResult;
import apoc.result.MapResult;
import apoc.util.JsonUtil;
import org.bson.Document;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.mongodb.MongoDBUtils.Coll;
import static apoc.mongodb.MongoDBUtils.getDocument;
import static apoc.mongodb.MongoDBUtils.getMongoColl;

@Extended
public class Mongo {

    @Context
    public Log log;

    @Procedure("apoc.mongo.aggregate")
    @Description("apoc.mongo.aggregate(uri, pipeline, $config) yield value - perform an aggregate operation on mongodb collection")
    public Stream<MapResult> aggregate(@Name("uri") String uri, @Name("pipeline") List<Map<String, Object>> pipeline, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MongoDbConfig conf = new MongoDbConfig(config);
        return executeMongoQuery(uri, conf, 
                coll -> {
                    final List<Document> pipelineDocs = pipeline.stream().map(MongoDBUtils::getDocument).collect(Collectors.toList());
                    return coll.aggregate(pipelineDocs).map(MapResult::new);
                }, 
                getExceptionConsumer("apoc.mongo.aggregate", uri, config));
    }

    @Procedure("apoc.mongo.count")
    @Description("apoc.mongo.count(uri, query, $config) yield value - perform a count operation on mongodb collection")
    public Stream<LongResult> count(@Name("uri") String uri, @Name("query") Object query, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MongoDbConfig conf = new MongoDbConfig(config);
        return executeMongoQuery(uri, conf, coll -> {
            long count = coll.count(getDocument(query));
            return Stream.of(new LongResult(count));
        }, getExceptionConsumer("apoc.mongo.count", uri, config));
    }

    @Procedure("apoc.mongo.find")
    @Description("apoc.mongo.find(uri, query, $config) yield value - perform a find operation on mongodb collection")
    public Stream<MapResult> find(@Name("uri") String uri, @Name(value = "query", defaultValue = "null") Object query, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MongoDbConfig conf = new MongoDbConfig(config);
        return executeMongoQuery(uri, conf, 
                coll -> coll.find(getDocument(query), conf.getProject(), conf.getSort(), conf.getSkip(), conf.getLimit()).map(MapResult::new),
                getExceptionConsumer("apoc.mongo.find", uri, config));
    }

    @Procedure("apoc.mongo.insert")
    @Description("apoc.mongo.insert(uri, documents, $config) yield value - inserts the given documents into the mongodb collection")
    public void insert(@Name("uri") String uri, @Name("documents") List<Object> documents, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MongoDbConfig conf = new MongoDbConfig(config);
        try (Coll coll = getMongoColl(() -> getColl(uri, conf))) {
            coll.insertDocs(documents.stream().map(MongoDBUtils::getDocument).collect(Collectors.toList()));
        } catch (Exception e) {
            mongoErrorLog("apoc.mongo.insert", uri, config, e, "documents = " + documents + ",");
            throw new RuntimeException(e);
        }
    }

    @Procedure("apoc.mongo.update")
    @Description("apoc.mongo.update(uri, query, update, $config) - updates the given documents from the mongodb collection and returns the number of affected documents")
    public Stream<LongResult> update(@Name("uri") String uri, @Name("query") Object query, @Name("update") Object update, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MongoDbConfig conf = new MongoDbConfig(config);
        return executeMongoQuery(uri, conf, coll -> Stream.of(new LongResult(coll.update(getDocument(query), getDocument(update)))),
                getExceptionConsumer("apoc.mongo.update", uri, config, "query = " + query + ",  update = " + update + ","));
    }

    @Procedure("apoc.mongo.delete")
    @Description("apoc.mongo.delete(uri, query, $config) - delete the given documents from the mongodb collection and returns the number of affected documents")
    public Stream<LongResult> delete(@Name("uri") String uri, @Name("query") Object query, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MongoDbConfig conf = new MongoDbConfig(config);
        return executeMongoQuery(uri, conf, coll -> Stream.of(new LongResult(coll.delete(getDocument(query)))),
                getExceptionConsumer("apoc.mongo.delete", uri, config, "query = " + query + ","));
    }

    private Consumer<Exception> getExceptionConsumer(String procedureName, String uri, Map<String, Object> config) {
        return getExceptionConsumer(procedureName, uri, config, "");
    }

    private Consumer<Exception> getExceptionConsumer(String procedureName, String uri, Map<String, Object> config, String others) {
        return e -> mongoErrorLog(procedureName, uri, config, e, others);
    }

    private void mongoErrorLog(String procedureName, String uri, Map<String, Object> config, Exception e, String optionalOthers) {
        log.error(procedureName + " - uri = '" + uri + "', " + optionalOthers + " config = " + JsonUtil.writeValueAsString(config), e);
    }

    private <T> Stream<T> executeMongoQuery(String uri, MongoDbConfig conf, Function<MongoDBUtils.Coll, Stream<T>> execute, Consumer<Exception> onError) {
        Coll coll = null;
        try {
            coll = getMongoColl(() -> getColl(uri, conf));
            return execute.apply(coll).onClose(coll::safeClose);
        } catch (Exception e) {
            if (coll != null) {
                coll.safeClose();
            }
            onError.accept(e);
            throw new RuntimeException(e);
        }
    }

    private MongoDBUtils.Coll getColl(@Name("url") String url, MongoDbConfig conf) {
        return Coll.Factory.create(url, conf);
    }

}
