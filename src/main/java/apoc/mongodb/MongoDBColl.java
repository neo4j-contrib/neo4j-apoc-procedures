package apoc.mongodb;

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
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author mh
 * @since 30.06.16
 */
class MongoDBColl implements MongoDB.Coll {

    private MongoCollection<Document> collection;
    private MongoClient mongoClient;
    private FindIterable<Document> result;

    public MongoDBColl(String url, String db, String coll) {
        MongoClientURI connectionString = new MongoClientURI(url);
        mongoClient = new MongoClient(connectionString);
        MongoDatabase database = mongoClient.getDatabase(db);
        collection = database.getCollection(coll);
    }

    @Override
    public void close() throws IOException {
        mongoClient.close();
    }

    /**
     * It translates a MongoDB document into a Map where the "_id" field is not an ObjectId
     * but a simple String representation of it
     *
     * @param document
     * @return
     */
    private Map<String, Object> documentToPackableMap(Map<String, Object> document) {
        /**
         * A document in MongoDB has a special field "_id" of type ObjectId
         * This object is not "packable" by Neo4jPacker so it must be converted to a value that Neo4j can deal with
         *
         * If the document is not null we simply override the ObjectId instance with its string representation
         */
        if (document != null) {
            document.put("_id", document.get("_id").toString());
        }

        return document;
    }

    @Override
    public Map<String, Object> first(Map<String, Object> query) {
        return documentToPackableMap(collection.find(new Document(query)).first());
    }

    @Override
    public Stream<Map<String, Object>> all(Map<String, Object> query) {
        return asStream(query == null ? collection.find() : collection.find(new Document(query)));
    }

    @Override
    public long count(Map<String, Object> query) {
        return query == null ? collection.count() : collection.count(new Document(query));
    }

    private Stream<Map<String, Object>> asStream(FindIterable<Document> result) {
        MongoCursor<Document> iterator = result.iterator();
        Spliterator<Map<String, Object>> spliterator = Spliterators.spliterator(iterator, -1, Spliterator.ORDERED);
        return StreamSupport.stream(spliterator, false).map(doc -> this.documentToPackableMap(doc)).onClose(iterator::close);
    }

    @Override
    public Stream<Map<String, Object>> find(Map<String, Object> query, Map<String, Object> project, Map<String, Object> sort) {
        FindIterable<Document> documents = query == null ? collection.find() : collection.find(new Document(query));
        if (project != null) documents = documents.projection(new Document(project));
        if (sort != null) documents = documents.sort(new Document(sort));
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
