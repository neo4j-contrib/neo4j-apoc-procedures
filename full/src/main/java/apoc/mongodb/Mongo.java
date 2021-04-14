package apoc.mongodb;

import apoc.result.LongResult;
import apoc.result.MapResult;
import apoc.util.MissingDependencyException;
import apoc.version.Version;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.mongodb.MongoDB.Coll;

public class Mongo {

    /*
    mongodb://[username:password@]host1[:port1][,...hostN[:portN]][/[defaultauthdb][?options]]
     */

    @Context
    public Log log;


    @Procedure("apoc.mongo.get")
    // todo - scrivere che l'host-or-key accetta una stringa tipo ..... mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database.collection][?options]]
    //  mettere il link di javadoc
    @Description("apoc.mongo.get(uri,$config) yield value - perform a find operation on mongodb collection")
    public Stream<MapResult> get(@Name("uri") String uri, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MongoDbConfig conf = new MongoDbConfig(config);
        return executeMongoQuery(uri, conf, coll -> coll.all(conf.getQuery(), conf.getSkip(), conf.getLimit(), conf.isUseExtendedJson()).map(MapResult::new),
                getExceptionConsumer("apoc.mongo.get", uri, config));
    }

    @Procedure("apoc.mongo.count")
    @Description("apoc.mongo.count(uri,$config) yield value - perform a find operation on mongodb collection")
    public Stream<LongResult> count(@Name("uri") String uri, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MongoDbConfig conf = new MongoDbConfig(config);
        return executeMongoQuery(uri, conf, coll -> {
                    long count = coll.count(conf.getQuery(), conf.isUseExtendedJson());
                    return Stream.of(new LongResult(count));
                }, getExceptionConsumer("apoc.mongo.count", uri, config));
    }


    // todo - teoricamente la query può essere null?
    @Procedure("apoc.mongo.first")
    // todo - descr
    @Description("apoc.mongo.first(uri,query,$config) yield value - perform a find operation on mongodb collection")
    public Stream<MapResult> first(@Name("uri") String uri, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MongoDbConfig conf = new MongoDbConfig(config);
        return executeMongoQuery(uri, conf, coll -> {
                    Map<String, Object> result = coll.first(conf.getQuery(), conf.isUseExtendedJson());
                    return result == null || result.isEmpty() ? Stream.empty() : Stream.of(new MapResult(result));
                }, getExceptionConsumer("apoc.mongo.first", uri, config));
    }

    public Consumer<Exception> getExceptionConsumer(String procedureName, String uri, Map<String, Object> config) {
        return getExceptionConsumer(procedureName, uri, config, "");
    }

    public Consumer<Exception> getExceptionConsumer(String procedureName, String uri, Map<String, Object> config, String others) {
        return e -> mongoErrorLog(procedureName, uri, config, e, others);
    }

    public void mongoErrorLog(String procedureName, String uri, Map<String, Object> config, Exception e, String others) {
        final String configString = config.entrySet().stream().map(entry -> "{" + entry.getKey() + ": " + entry.getValue() + "}").collect(Collectors.joining(", "));
        log.error(procedureName + " - uri = [" + uri + "] , config = {" + configString + "}" + others, e);
    }

    @Procedure("apoc.mongo.find")
    // todo - descr
    @Description("apoc.mongo.find(uri,query,$config) yield value - perform a find operation on mongodb collection")
    public Stream<MapResult> find(@Name("uri") String uri, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MongoDbConfig conf = new MongoDbConfig(config);
        return executeMongoQuery(uri, conf,
                coll -> coll.find(conf.getQuery(), conf.getProject(), conf.getSort(), conf.getSkip(), conf.getLimit(), conf.isUseExtendedJson()).map(MapResult::new),
                getExceptionConsumer("apoc.mongo.find", uri, config));
    }


    @Procedure("apoc.mongo.insert")
    // todo - descr
    @Description("apoc.mongo.insert(uri,query,$config) yield value - perform a find operation on mongodb collection")
    public void insert(@Name("uri") String uri, @Name("documents") List<Map<String, Object>> documents, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MongoDbConfig conf = new MongoDbConfig(config);
        // TODO - CAMBIARE NOME withSystemDb
        try (Coll coll = withSystemDb(() -> getColl(uri, conf))) {
            coll.insert(documents, conf.isUseExtendedJson());
        } catch (Exception e) {
            mongoErrorLog("apoc.mongo.insert", uri, config, e, "");
            throw new RuntimeException(e);
        }
    }

    @Procedure("apoc.mongo.update")
    @Description("apoc.mongodb.update(host-or-key,db,collection,query,update) - updates the given documents from the mongodb collection and returns the number of affected documents")
    public Stream<LongResult> update(@Name("uri") String uri, @Name("query") Map<String, Object> query, @Name("update") Map<String, Object> update, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MongoDbConfig conf = new MongoDbConfig(config);

        // todo - onError?
        return executeMongoQuery(uri, conf, coll -> Stream.of(new LongResult(coll.update(query, update, conf.isUseExtendedJson()))),
                getExceptionConsumer("apoc.mongo.update", uri, config));

    }

    @Procedure("apoc.mongo.delete")
    @Description("apoc.mongo.delete(uri, query, $config) - delete the given documents from the mongodb collection and returns the number of affected documents")
    public Stream<LongResult> delete(@Name("uri") String uri, @Name("query") Map<String, Object> query, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MongoDbConfig conf = new MongoDbConfig(config);
        // todo - onError?
        return executeMongoQuery(uri, conf, coll -> Stream.of(new LongResult(coll.delete(query, conf.isUseExtendedJson()))),
                getExceptionConsumer("apoc.mongo.delete", uri, config));
    }


    private <T> Stream<T> executeMongoQuery(String uri, MongoDbConfig conf, Function<MongoDB.Coll, Stream<T>> execute, Consumer<Exception> onError) {
        MongoDB.Coll coll = null;
        try {
            coll = withSystemDb(() -> getColl(uri, conf));
//            coll = getMongoColl(uri, conf);
            return execute.apply(coll).onClose(coll::safeClose);
        } catch (Exception e) {
            if (coll != null) {
                coll.safeClose();
            }
            onError.accept(e);
            throw new RuntimeException(e);
        }
    }

    private Coll withSystemDb(Supplier<Coll> action) {
        MongoDB.Coll coll = null;
        try {
            coll = action.get();
        } catch (NoClassDefFoundError e) {
            final String version = Version.class.getPackage().getImplementationVersion().substring(0, 3);
            throw new MissingDependencyException(String.format("Cannot find the jar into the plugins folder. \n" +
                    "Please put these jar in the plugins folder :\n\n" +
                    "bson-x.y.z.jar\n" +
                    "\n" +
                    "mongo-java-driver-x.y.z.jar\n" +
                    "\n" +
                    "mongodb-driver-x.y.z.jar\n" +
                    "\n" +
                    "mongodb-driver-core-x.y.z.jar\n" +
                    "\n" +
                    "jackson-annotations-x.y.z.jar\n\njackson-core-x.y.z.jar\n\njackson-databind-x.y.z.jar\n\nSee the documentation: https://neo4j.com/labs/apoc/%s/database-integration/mongodb/", version));
        } catch (Exception e) {
            throw new RuntimeException("Error during connection", e);
//            System.out.println("Mongo.withSystemDb");
        }
        System.out.println("coll - " + coll);
        return coll;
    }


//    private MongoDB.Coll getMongoColl(String hostOrKey, MongoDbConfig conf) {
//
//        MongoDB.Coll coll;
//        try {
//            coll = getColl(hostOrKey, conf);
//        } catch (NoClassDefFoundError e) {
//            final String version = Version.class.getPackage().getImplementationVersion().substring(0, 3);
//            throw new MissingDependencyException(String.format("Cannot find the jar into the plugins folder. \n" +
//                    "Please put these jar in the plugins folder :\n\n" +
//                    "bson-x.y.z.jar\n" +
//                    "\n" +
//                    "mongo-java-driver-x.y.z.jar\n" +
//                    "\n" +
//                    "mongodb-driver-x.y.z.jar\n" +
//                    "\n" +
//                    "mongodb-driver-core-x.y.z.jar\n" +
//                    "\n" +
//                    "jackson-annotations-x.y.z.jar\n\njackson-core-x.y.z.jar\n\njackson-databind-x.y.z.jar\n\nSee the documentation: https://neo4j.com/labs/apoc/%s/database-integration/mongodb/", version));
//        }
//        return coll;
//    }


    protected static Coll getColl(@Name("url") String url, MongoDbConfig conf) {
        return Coll.Factory.create(url, conf);
    }

}
