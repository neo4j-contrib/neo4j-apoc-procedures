package apoc.mongodb;

import apoc.result.MapResult;
import apoc.util.MissingDependencyException;
import apoc.util.UrlResolver;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

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
    @Description("apoc.mongo.get(host-or-key,query,$config) yield value - perform a find operation on mongodb collection")
    public Stream<MapResult> get(@Name("host") String hostOrKey,
                                 @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) {
        MongoDbConfig conf = new MongoDbConfig(config);

        return executeMongoQuery(hostOrKey, conf, coll -> coll.all(conf.getQuery(), conf.getSkip(), conf.getLimit(), conf.isUseExtendedJson()).map(MapResult::new),
                e -> {
            // todo - vedere che ne esce fuori dal config string - NEL CASO FARE UN COMMON METHOD
                    final String configString = config.entrySet().stream().map(entry -> entry.getKey() + " - " + entry.getValue()).collect(Collectors.joining(", "));
                    final String string = config.toString(); // TODO - FORSE QUESTO BASTA
                    log.error("apoc.mongo.get - hostOrKey = [" + hostOrKey + "] , config = [ " + configString + " ]", e);
        });
    }

    private <T> Stream<T> executeMongoQuery(String hostOrKey, MongoDbConfig conf, Function<MongoDB.Coll, Stream<T>> execute, Consumer<Exception> onError) {
        MongoDB.Coll coll = null;
        try {
            coll = getMongoColl(hostOrKey, conf);
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

    protected static Coll getColl(@Name("host") String hostOrKey, MongoDbConfig conf) {
        String url = getMongoDBUrl(hostOrKey);
        return Coll.Factory.create(url, conf);
    }

    protected static String getMongoDBUrl(String hostOrKey) {
        // todo - ma serve? a cosa? e se sì come lo cambio?
        return new UrlResolver("mongodb", "localhost", 27017).getUrl("mongodb", hostOrKey);
    }

}
