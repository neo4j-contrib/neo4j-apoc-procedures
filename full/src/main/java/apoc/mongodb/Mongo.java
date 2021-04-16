package apoc.mongodb;

import apoc.result.LongResult;
import apoc.result.MapResult;
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
import static apoc.mongodb.MongoDBUtils.getMongoColl;

public class Mongo {

    @Context
    public Log log;


    @Procedure("apoc.mongo.count")
    @Description("apoc.mongo.count(uri, $config) yield value - perform a find operation on mongodb collection")
    public Stream<LongResult> count(@Name("uri") String uri, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MongoDbConfig conf = new MongoDbConfig(config);
        return executeMongoQuery(uri, conf, coll -> {
            long count = coll.count(conf.getQuery(), conf.isUseExtendedJson());
            return Stream.of(new LongResult(count));
        }, getExceptionConsumer("apoc.mongo.count", uri, config));
    }

    @Procedure("apoc.mongo.first")
    @Description("apoc.mongo.first(uri, $config) yield value - perform a find operation on mongodb collection")
    public Stream<MapResult> first(@Name("uri") String uri, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MongoDbConfig conf = new MongoDbConfig(config);
        return executeMongoQuery(uri, conf, coll -> {
            Map<String, Object> result = coll.first(conf.getQuery(), conf.getProject(), conf.getSkip(), conf.isUseExtendedJson());
            return result == null || result.isEmpty() ? Stream.empty() : Stream.of(new MapResult(result));
        }, getExceptionConsumer("apoc.mongo.first", uri, config));
    }

    @Procedure("apoc.mongo.find")
    @Description("apoc.mongo.find(uri, $config) yield value - perform a find operation on mongodb collection")
    public Stream<MapResult> find(@Name("uri") String uri, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MongoDbConfig conf = new MongoDbConfig(config);
        return executeMongoQuery(uri, conf,
                coll -> coll.find(conf.getQuery(), conf.getProject(), conf.getSort(), conf.getSkip(), conf.getLimit(), conf.isUseExtendedJson()).map(MapResult::new),
                getExceptionConsumer("apoc.mongo.find", uri, config));
    }

    @Procedure("apoc.mongo.insert")
    @Description("apoc.mongo.insert(uri, documents, $config) yield value - inserts the given documents into the mongodb collection")
    public void insert(@Name("uri") String uri, @Name("documents") List<Map<String, Object>> documents, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MongoDbConfig conf = new MongoDbConfig(config);
        try (Coll coll = getMongoColl(() -> getColl(uri, conf))) {
            coll.insert(documents, conf.isUseExtendedJson());
        } catch (Exception e) {
            mongoErrorLog("apoc.mongo.insert", uri, config, e, "documents = " + documents + ",");
            throw new RuntimeException(e);
        }
    }

    @Procedure("apoc.mongo.update")
    @Description("apoc.mongo.update(uri, query, update, $config) - updates the given documents from the mongodb collection and returns the number of affected documents")
    public Stream<LongResult> update(@Name("uri") String uri, @Name("query") Map<String, Object> query, @Name("update") Map<String, Object> update, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MongoDbConfig conf = new MongoDbConfig(config);
        return executeMongoQuery(uri, conf, coll -> Stream.of(new LongResult(coll.update(query, update, conf.isUseExtendedJson()))),
                getExceptionConsumer("apoc.mongo.update", uri, config, "query = " + query + ",  update = " + update + ","));

    }

    @Procedure("apoc.mongo.delete")
    @Description("apoc.mongo.delete(uri, query, $config) - delete the given documents from the mongodb collection and returns the number of affected documents")
    public Stream<LongResult> delete(@Name("uri") String uri, @Name("query") Map<String, Object> query, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MongoDbConfig conf = new MongoDbConfig(config);
        return executeMongoQuery(uri, conf, coll -> Stream.of(new LongResult(coll.delete(query, conf.isUseExtendedJson()))),
                getExceptionConsumer("apoc.mongo.delete", uri, config, "query = " + query + ","));
    }


    public Consumer<Exception> getExceptionConsumer(String procedureName, String uri, Map<String, Object> config) {
        return getExceptionConsumer(procedureName, uri, config, "");
    }

    public Consumer<Exception> getExceptionConsumer(String procedureName, String uri, Map<String, Object> config, String others) {
        return e -> mongoErrorLog(procedureName, uri, config, e, others);
    }

    public void mongoErrorLog(String procedureName, String uri, Map<String, Object> config, Exception e, String optionalOthers) {
        final String configString = config.entrySet().stream().map(entry -> "{" + entry.getKey() + ": " + entry.getValue() + "}").collect(Collectors.joining(", "));
        log.error(procedureName + " - uri = '" + uri + "', " + optionalOthers + " config = {" + configString + "}", e);
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

    protected static MongoDBUtils.Coll getColl(@Name("url") String url, MongoDbConfig conf) {
        return Coll.Factory.create(url, conf);
    }

}
