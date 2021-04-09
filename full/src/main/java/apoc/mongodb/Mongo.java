package apoc.mongodb;

import apoc.result.LongResult;
import apoc.result.MapResult;
import apoc.util.MissingDependencyException;
import apoc.util.UrlResolver;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//import static apoc.mongodb.MongoDB.executeMongoQuery;
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
                e -> {
            // todo - vedere che ne esce fuori dal config string - NEL CASO FARE UN COMMON METHOD
                    final String configString = config.entrySet().stream().map(entry -> entry.getKey() + " - " + entry.getValue()).collect(Collectors.joining(", "));
                    final String string = config.toString(); // TODO - FORSE QUESTO BASTA
                    log.error("apoc.mongo.get - uri = [" + uri + "] , config = [ " + configString + " ]", e);
        });
    }

    @Procedure("apoc.mongo.count")
    @Description("apoc.mongo.count(uri,$config) yield value - perform a find operation on mongodb collection")
    public Stream<LongResult> count(@Name("uri") String uri, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MongoDbConfig conf = new MongoDbConfig(config);
        return executeMongoQuery(uri, conf, coll -> {
                    long count = coll.count(conf.getQuery(), conf.isUseExtendedJson());
                    return Stream.of(new LongResult(count));
                },
                e -> {
                    // todo - vedere che ne esce fuori dal config string - NEL CASO FARE UN COMMON METHOD
                    final String configString = config.entrySet().stream().map(entry -> entry.getKey() + " - " + entry.getValue()).collect(Collectors.joining(", "));
                    final String string = config.toString(); // TODO - FORSE QUESTO BASTA
                    log.error("apoc.mongo.get - uri = [" + uri + "] , config = [ " + configString + " ]", e);
                });
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
                },
                e -> {
                    // todo - vedere che ne esce fuori dal config string - NEL CASO FARE UN COMMON METHOD
                    final String configString = config.entrySet().stream().map(entry -> entry.getKey() + " - " + entry.getValue()).collect(Collectors.joining(", "));
                    final String string = config.toString(); // TODO - FORSE QUESTO BASTA
                    log.error("apoc.mongo.get - uri = [" + uri + "] , config = [ " + configString + " ]", e);
                });
    }

    @Procedure("apoc.mongo.find")
    // todo - descr
    @Description("apoc.mongo.find(uri,query,$config) yield value - perform a find operation on mongodb collection")
    public Stream<MapResult> find(@Name("uri") String uri, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MongoDbConfig conf = new MongoDbConfig(config);
        return executeMongoQuery(uri, conf,
                coll -> coll.find(conf.getQuery(), conf.getProject(), conf.getSort(), conf.getSkip(), conf.getLimit(), conf.isUseExtendedJson()).map(MapResult::new),
                e -> {
                    // todo - vedere che ne esce fuori dal config string - NEL CASO FARE UN COMMON METHOD
                    final String configString = config.entrySet().stream().map(entry -> entry.getKey() + " - " + entry.getValue()).collect(Collectors.joining(", "));
                    final String string = config.toString(); // TODO - FORSE QUESTO BASTA
                    log.error("apoc.mongo.get - uri = [" + uri + "] , config = [ " + configString + " ]", e);
                });
    }


    @Procedure("apoc.mongo.insert")
    // todo - descr
    @Description("apoc.mongo.insert(uri,query,$config) yield value - perform a find operation on mongodb collection")
    public void insert(@Name("uri") String uri, @Name("documents") List<Map<String, Object>> documents, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MongoDbConfig conf = new MongoDbConfig(config);
        // todo - qui invece di false, false, false metto direttamente conf
//        try (Coll coll = getMongoColl(hostOrKey, db, collection, false, false, false)) {
        try (Coll coll = getMongoColl(uri, conf)) {
            coll.insert(documents, conf.isUseExtendedJson());

        } catch (Exception e) {
            // TODO - PERCHE QUESTO E DIVERSO DALLE ALTRE PROCEDURE, A STO PUNTO LO FACCIO COERENTE.... e ritorno il numero di affected document...
//            log.error("apoc.mongodb.insert - hostOrKey = [" + hostOrKey + "], db = [" + db + "], collection = [" + collection + "], documents = [" + documents + "]",e);
            throw new RuntimeException(e);
        }
    }

    @Procedure
    @Description("apoc.mongodb.update(host-or-key,db,collection,query,update) - updates the given documents from the mongodb collection and returns the number of affected documents")
    public Stream<LongResult> update(@Name("uri") String uri, @Name("query") Map<String, Object> query, @Name("update") Map<String, Object> update,  @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MongoDbConfig conf = new MongoDbConfig(config);

        // todo - onError?
        return executeMongoQuery(uri, conf, coll -> Stream.of(new LongResult(coll.update(query, update, conf.isUseExtendedJson()))),
                e -> {
                    // todo - vedere che ne esce fuori dal config string - NEL CASO FARE UN COMMON METHOD
                    final String configString = config.entrySet().stream().map(entry -> entry.getKey() + " - " + entry.getValue()).collect(Collectors.joining(", "));
                    final String string = config.toString(); // TODO - FORSE QUESTO BASTA

                    // todo - mettere tutti i campi. Parametrizzare
                    log.error("apoc.mongo.get - uri = [" + uri + "] , config = [ " + configString + " ]", e);
                });
    }


    private <T> Stream<T> executeMongoQuery(String uri, MongoDbConfig conf, Function<MongoDB.Coll, Stream<T>> execute, Consumer<Exception> onError) {
        MongoDB.Coll coll = null;
        try {
            coll = getMongoColl(uri, conf);
            return execute.apply(coll).onClose(coll::safeClose);
        } catch (Exception e) {
            if (coll != null) {
                coll.safeClose();
            }
            onError.accept(e);
            throw new RuntimeException(e);
        }
    }

    private MongoDB.Coll getMongoColl(String hostOrKey, MongoDbConfig conf) {
        MongoDB.Coll coll = null;
        try {
            coll = getColl(hostOrKey, conf);
        } catch (NoClassDefFoundError e) {
            throw new MissingDependencyException("Cannot find the jar into the plugins folder. \n" +
                    "Please put these jar in the plugins folder :\n\n" +
                    "bson-x.y.z.jar\n" +
                    "\n" +
                    "mongo-java-driver-x.y.z.jar\n" +
                    "\n" +
                    "mongodb-driver-x.y.z.jar\n" +
                    "\n" +
                    "mongodb-driver-core-x.y.z.jar\n" +
                    "\n" +
                    "jackson-annotations-x.y.z.jar\n\njackson-core-x.y.z.jar\n\njackson-databind-x.y.z.jar\n\nSee the documentation: https://neo4j-contrib.github.io/neo4j-apoc-procedures/#_interacting_with_mongodb");
        }
        return coll;
    }

    protected static Coll getColl(@Name("url") String url, MongoDbConfig conf) {
//        String url = getMongoDBUrl(hostOrKey);
        return Coll.Factory.create(url, conf);
    }

    protected static String getMongoDBUrl(String hostOrKey) {
        // todo - ma serve? a cosa? e se sì come lo cambio?
        return new UrlResolver("mongodb", "localhost", 27017).getUrl("mongodb", hostOrKey);
    }

}
