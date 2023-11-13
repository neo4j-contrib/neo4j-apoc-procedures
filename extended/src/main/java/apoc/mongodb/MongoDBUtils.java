package apoc.mongodb;

import apoc.util.JsonUtil;
import apoc.util.MissingDependencyException;
import org.bson.Document;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MongoDBUtils {
    public static final String MONGO_MISSING_DEPS_ERROR = """
            Cannot find the jar into the plugins folder.\s
            Please put the apoc-mongodb-dependencies-5.x.x-all.jar into plugin folder.
            See the documentation: See the documentation: https://neo4j.com/labs/apoc/5/database-integration/mongo/#mongodb-dependencies""";


    protected static Document getDocument(Object query) {
        if (query == null) {
            return new Document();
        }
        return Document.parse(query instanceof String
                ? (String) query
                : JsonUtil.writeValueAsString(query));
    }

    protected static List<Document> getDocuments(List<Map<String, Object>> pipeline) {
        return pipeline.stream()
                .map(MongoDBUtils::getDocument)
                .collect(Collectors.toList());
    }

    protected static MongoDbConfig getMongoConfig(Map<String, Object> config) {
        try {
            return new MongoDbConfig(config);
        } catch (NoClassDefFoundError e) {
            throw new MissingDependencyException(MONGO_MISSING_DEPS_ERROR);
        }
    }
}
