package apoc.mongodb;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCommandException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonNumber;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.util.Util.toJson;
import static java.lang.String.format;

/**
 * @author mh
 * @since 30.06.16
 */
class MongoDBColl implements MongoDBUtils.Coll {

    private static final ObjectMapper jsonMapper = new ObjectMapper().enable(DeserializationFeature.USE_LONG_FOR_INTS);
    public static final String ID = "_id";
    private final MongoCollection<Document> collection;
    private final MongoClient mongoClient;
    private boolean compatibleValues = false;
    private boolean doorStop = false;
    private final MongoDatabase database;
    private boolean extractReferences = false;
    private boolean objectIdAsMap = true;

    // visible for testing
    public static final String ERROR_MESSAGE = "The connection string must have %s name";

    private MongoDBColl(String url, String db, String coll) {
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
        getConfigs(compatibleValues, extractReferences, objectIdAsMap);
    }

    /**
     *
     * @param uri the string Uri to convert in connectionString
     * @see MongoClientURI
     * @param conf the configuration
     * @see MongoDbConfig
     */
    public MongoDBColl(String uri, MongoDbConfig conf) {

        MongoClientURI connectionString = new MongoClientURI(uri);

        if (connectionString.getDatabase() == null) {
            throw new RuntimeException(format(ERROR_MESSAGE, "db"));
        }
        if (connectionString.getCollection() == null) {
            throw new RuntimeException(format(ERROR_MESSAGE, "collection"));
        }

        mongoClient = new MongoClient(connectionString);
        database = mongoClient.getDatabase(connectionString.getDatabase());

        try {
            // check if correctly authenticated
            database.runCommand(new Document("listCollections", 1));
        } catch (MongoCommandException e) {
            mongoClient.close();
            throw new RuntimeException(e);
        }
        collection = database.getCollection(connectionString.getCollection());

        getConfigs(conf.isCompatibleValues(), conf.isExtractReferences(), conf.isObjectIdAsMap());
    }

    private void getConfigs(boolean compatibleValues, boolean extractReferences, boolean objectIdAsMap) {
        this.compatibleValues = compatibleValues;
        this.extractReferences = extractReferences;
        this.objectIdAsMap = objectIdAsMap;
    }

    @Override
    public void close() {
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
            if (data instanceof BsonNumber) {
                return ((BsonNumber) data).longValue();
            }
            if (data instanceof Binary) {
                return ((Binary) data).getData();
            }
            if (data instanceof Float) {
                return ((Number) data).doubleValue();
            }
            if (data instanceof BsonTimestamp) {
                return (long) ((BsonTimestamp) data).getTime();
            }
            if (data instanceof MinKey || data instanceof MaxKey) {
                return data.toString();
            }
            if (data instanceof BsonRegularExpression) {
                return ((BsonRegularExpression) data).getPattern();
            }
            if (data instanceof Code) {
                return ((Code) data).getCode();
            }
            if (data instanceof Symbol) {
                return ((Symbol) data).getSymbol();
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
        return first(query, Collections.emptyMap(), 0L, false);
    }

    @Override
    public Map<String, Object> first(Map<String, Object> query, Map<String, Object> project, Long skip, boolean useExtendedJson) {
        final FindIterable<Document> documents = getDocuments(query, useExtendedJson)
                .projection(documentBasedOnUseExtJson(project, useExtendedJson))
                .skip(skip.intValue());
        return documentToPackableMap(documents.first());
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
        return count(query, false);
    }

    @Override
    public long count(Map<String, Object> query, boolean useExtendedJson) {
        return query == null ? collection.count() : collection.count(documentBasedOnUseExtJson(query, useExtendedJson));
    }

    @Override
    public Stream<Map<String, Object>> find(Map<String, Object> query, Map<String, Object> project, Map<String, Object> sort, Long skip, Long limit) {
        return find(query, project, sort, skip, limit, false);
    }

    @Override
    public Stream<Map<String, Object>> find(Map<String, Object> query,
                                            Map<String, Object> project,
                                            Map<String, Object> sort,
                                            Long skip,
                                            Long limit,
                                            boolean useExtendedJson) {
        FindIterable<Document> documents = getDocuments(query, useExtendedJson);
        if (project != null) documents = documents.projection(documentBasedOnUseExtJson(project, useExtendedJson));
        if (sort != null) documents = documents.sort(documentBasedOnUseExtJson(sort, useExtendedJson));
        if (skip != 0) documents = documents.skip(skip.intValue());
        if (limit != 0) documents = documents.limit(limit.intValue());
        return asStream(documents);
    }

    private FindIterable<Document> getDocuments(Map<String, Object> query, boolean useExtendedJson) {
        return query == null
                ? collection.find()
                : collection.find(documentBasedOnUseExtJson(query, useExtendedJson));
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
    public void insert(List<Map<String, Object>> docs) {
        insert(docs, false);
    }

    @Override
    public void insert(List<Map<String, Object>> docs, boolean useExtendedJson) {
        for (Map<String, Object> doc : docs) {
            collection.insertOne(documentBasedOnUseExtJson(doc, useExtendedJson));
        }
    }

    @Override
    public long update(Map<String, Object> query, Map<String, Object> update) {
        return update(query, update, false);
    }

    @Override
    public long update(Map<String, Object> query, Map<String, Object> update, boolean useExtendedJson) {
        UpdateResult updateResult = collection.updateMany(
                documentBasedOnUseExtJson(query, useExtendedJson),
                documentBasedOnUseExtJson(update, useExtendedJson)
        );
        return updateResult.wasAcknowledged() ? updateResult.getModifiedCount() : -updateResult.getModifiedCount();
    }

    @Override
    public long delete(Map<String, Object> query) {
        return delete(query, false);
    }

    @Override
    public long delete(Map<String, Object> query, boolean useExtendedJson) {
        DeleteResult result = collection.deleteMany(documentBasedOnUseExtJson(query, useExtendedJson));
        return result.wasAcknowledged() ? result.getDeletedCount() : -result.getDeletedCount();
    }

    private Document documentBasedOnUseExtJson(Map<String, Object> query, boolean useExtendedJson) {
        return useExtendedJson ? Document.parse(toJson(query)) : new Document(query);
    }

}
