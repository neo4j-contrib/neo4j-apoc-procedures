package apoc.mongodb;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author mh
 * @since 30.06.16
 */
class MongoDBColl implements MongoDB.Coll {

    private static final ObjectMapper jsonMapper = new ObjectMapper().enable(DeserializationFeature.USE_LONG_FOR_INTS);
    private MongoCollection<Document> collection;
    private MongoClient mongoClient;
    private boolean compatibleValues = false;
    private boolean doorStop = false;

    public MongoDBColl(String url, String db, String coll) {
        MongoClientURI connectionString = new MongoClientURI(url);
        mongoClient = new MongoClient(connectionString);
        MongoDatabase database = mongoClient.getDatabase(db);
        collection = database.getCollection(coll);
    }

    /**
     *
     * @param url
     * @param db
     * @param coll
     * @param compatibleValues if true we convert the document to JSON and than back to a Map
     */
    public MongoDBColl(String url, String db, String coll, Boolean compatibleValues) {
        this(url, db, coll);
        this.compatibleValues = compatibleValues;
    }

    @Override
    public void close() throws IOException {
        if (doorStop) return;
        mongoClient.close();
    }

    /**
     * It translates a MongoDB document into a Map where the "_id" field is not an ObjectId
     * but a simple String representation of it
     *
     * @param document
     *
     * @return
     */
    private Map<String, Object> documentToPackableMap(Map<String, Object> document) {
        if (compatibleValues) {
            try {
                return jsonMapper.readValue(jsonMapper.writeValueAsBytes(document), new TypeReference<Map<String, Object>>() {
                });
            } catch (Exception e) {
                throw new RuntimeException("Cannot convert document to json and back to Map " + e.getMessage());
            }
        }

        /**
         * A document in MongoDB has a special field "_id" of type ObjectId
         * This object is not "packable" by Neo4jPacker so it must be converted to a value that Neo4j can deal with
         *
         * If the document is not null we simply override the ObjectId instance with its string representation
         */

        if (document != null) {
            Object objectId = document.get("_id");
            if (objectId != null) {
                document.put("_id", objectId.toString());
            }

        }

        return document;
    }

    @Override
    public Map<String, Object> first(Map<String, Object> query) {
        return documentToPackableMap(collection.find(new Document(query)).first());
    }

    @Override
    public Stream<Map<String, Object>> all(Map<String, Object> query, Long skip, Long limit) {
        FindIterable<Document> documents = query == null ? collection.find() : collection.find(new Document(query));
        if (skip != 0) documents = documents.skip(skip.intValue());
        if (limit != 0) documents = documents.limit(limit.intValue());
        return asStream(documents);
    }

    @Override
    public long count(Map<String, Object> query) {
        return query == null ? collection.count() : collection.count(new Document(query));
    }

    private Stream<Map<String, Object>> asStream(FindIterable<Document> result) {
        this.doorStop = true;
        Iterable<Document> it = () -> result.iterator();
        return StreamSupport
                .stream(it.spliterator(), false)
                .map(doc -> this.documentToPackableMap(doc))
                .onClose( () -> {
                        result.iterator().close();
                        mongoClient.close();
                    } );
    }

    @Override
    public Stream<Map<String, Object>> find(Map<String, Object> query, Map<String, Object> project, Map<String, Object> sort, Long skip, Long limit) {
        FindIterable<Document> documents = query == null ? collection.find() : collection.find(new Document(query));
        if (project != null) documents = documents.projection(new Document(project));
        if (sort != null) documents = documents.sort(new Document(sort));
        if (skip != 0) documents = documents.skip(skip.intValue());
        if (limit != 0) documents = documents.limit(limit.intValue());
        return asStream(documents);
    }

    @Override
    public void insert(List<Map<String, Object>> docs) {
        for (Map<String, Object> doc : docs) {
            collection.insertOne(new Document(doc));
        }
    }

    @Override
    public long update(Map<String, Object> query, Map<String, Object> update) {
        UpdateResult updateResult = collection.updateMany(new Document(query), new Document(update));
        return updateResult.wasAcknowledged() ? updateResult.getModifiedCount() : -updateResult.getModifiedCount();
    }

    @Override
    public long delete(Map<String, Object> query) {
        DeleteResult result = collection.deleteMany(new Document(query));
        return result.wasAcknowledged() ? result.getDeletedCount() : -result.getDeletedCount();
    }


}
