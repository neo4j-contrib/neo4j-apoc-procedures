package apoc.mongodb;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author mh
 * @since 30.06.16
 */
class MongoDBColl implements MongoDB.Coll {

    private static final ObjectMapper jsonMapper = new ObjectMapper().enable(DeserializationFeature.USE_LONG_FOR_INTS);
    public static final String ID = "_id";
    private final MongoCollection<Document> collection;
    private final MongoClient mongoClient;
    private boolean compatibleValues = false;
    private boolean doorStop = false;
    private final MongoDatabase database;
    private boolean extractReferences = false;
    private boolean objectIdAsMap = true;

    public MongoDBColl(String url, String db, String coll) {
        MongoClientURI connectionString = new MongoClientURI(url);
        mongoClient = new MongoClient(connectionString);
        database = mongoClient.getDatabase(db);
        collection = database.getCollection(coll);
    }

    /**
     *
     * @param url
     * @param db
     * @param coll
     * @param compatibleValues if true we convert the document to JSON and than back to a Map
     */
    public MongoDBColl(String url, String db, String coll, boolean compatibleValues,
                       boolean extractReferences, boolean objectIdAsMap) {
        this(url, db, coll);
        this.compatibleValues = compatibleValues;
        this.extractReferences = extractReferences;
        this.objectIdAsMap = objectIdAsMap;
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
        return (Map<String, Object>) convertAndExtract(document);
    }

    public Object convertAndExtract(Object data) {
        if (data == null) {
            return null;
        }
        if (data instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) data;
            return map.entrySet().stream()
                    .map(e -> {
                        Object value;
                        if (ID.equals(e.getKey())) { // avoid circular conversions
                            if (compatibleValues && objectIdAsMap) {
                                try {
                                    value = jsonMapper.readValue(jsonMapper.writeValueAsBytes(e.getValue()), Map.class);
                                } catch (Exception exc) {
                                    throw new RuntimeException("Cannot convert document to json and back to Map " + exc.getMessage());
                                }
                            } else {
                                value = e.getValue().toString();
                            }
                        } else {
                            value = convertAndExtract(e.getValue());
                        }
                        return new AbstractMap.SimpleEntry(e.getKey(), value);
                    })
                    .collect(HashMap::new, (m, e)-> m.put(e.getKey(), e.getValue()), HashMap::putAll); // please look at https://bugs.openjdk.java.net/browse/JDK-8148463
        }
        if (data instanceof Collection) {
            Collection<Object> collection = (Collection<Object>) data;
            return collection.stream()
                    .map(elem -> convertAndExtract(elem))
                    .collect(Collectors.toList());
        }
        if (data.getClass().isArray()
                && !(data.getClass().getComponentType().isPrimitive() || !data.getClass().getComponentType().equals(String.class))) {
            return Stream.of((Object[]) data)
                    .map(elem -> convertAndExtract(elem))
                    .collect(Collectors.toList());
        }
        if (compatibleValues) {
            if (data instanceof Integer) {
                return ((Integer) data).longValue();
            }
            if (data instanceof Float) {
                return ((Float) data).doubleValue();
            }
            if (data instanceof Binary) {
                return ((Binary) data).getData();
            }
        }

        if (data instanceof Date) { // temporal types don't work with ValueUtils.of
            return LocalDateTime.ofInstant(((Date) data).toInstant(), ZoneId.systemDefault());
        }
        if (data instanceof ObjectId) {
            return extractReferences ? extractReference((ObjectId) data) : data.toString();
        }
        return data;
    }

    private Object extractReference(ObjectId objectId) {
        return StreamSupport.stream(database.listCollectionNames().spliterator(), false)
                .map(collectionName -> database.getCollection(collectionName))
                .map(collection -> collection.find(new Document(ID, objectId)).first())
                .filter(result -> result != null && !result.isEmpty())
                .findFirst()
                .map(this::documentToPackableMap)
                .orElse(null);
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
