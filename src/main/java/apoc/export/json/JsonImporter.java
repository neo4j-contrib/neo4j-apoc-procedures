package apoc.export.json;

import apoc.export.util.Reporter;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import java.io.Closeable;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JsonImporter implements Closeable {
    private static final String CREATE_NODE = "UNWIND $rows AS row " +
            "CREATE (n%s {%s: row.id}) SET n += row.properties";
    private static final String CREATE_RELS = "UNWIND $rows AS row " +
            "MATCH (s%s {%s: row.start.id}) " +
            "MATCH (e%s {%2$s: row.end.id}) " +
            "CREATE (s)-[r:%s]->(e) SET r += row.properties";

    private final List<Map<String, Object>> paramList;
    private final int unwindBatchSize;
    private final int txBatchSize;
    private final GraphDatabaseService db;
    private final Reporter reporter;

    private String lastType;
    private List<String> lastLabels;
    private Map<String, Object> lastRelTypes;

    private final ImportJsonConfig importJsonConfig;

    public JsonImporter(ImportJsonConfig importJsonConfig,
                        GraphDatabaseService db,
                        Reporter reporter) {
        this.paramList = new ArrayList<>(importJsonConfig.getUnwindBatchSize());
        this.db = db;
        this.txBatchSize = importJsonConfig.getTxBatchSize();
        this.unwindBatchSize = Math.min(importJsonConfig.getUnwindBatchSize(), txBatchSize);
        this.reporter = reporter;
        this.importJsonConfig = importJsonConfig;
    }

    public void importRow(Map<String, Object> param) {
        final String type = (String) param.get("type");

        manageEntityType(type);

        switch (type) {
            case "node":
                manageNode(param);
                break;
            case "relationship":
                manageRelationship(param);
                break;
            default:
                throw new IllegalArgumentException("Current type not supported: " + type);
        }

        final Map<String, Object> properties = (Map<String, Object>) param.getOrDefault("properties", Collections.emptyMap());
        updateReporter(type, properties);
        param.put("properties", convertProperties(type, properties, null));

        paramList.add(param);
        if (paramList.size() % txBatchSize == 0) {
            final Collection<List<Map<String, Object>>> results = chunkData();
            paramList.clear();
            // write
            writeUnwindBatch(results);
        }
    }

    private void writeUnwindBatch(Collection<List<Map<String, Object>>> results) {
        try (final Transaction tx = db.beginTx()) {
            results.forEach(resultList -> {
                if (resultList.size() == unwindBatchSize) {
                    write(tx, resultList);
                } else {
                    paramList.addAll(resultList);
                }
            });
            tx.close();
        }
    }

    private void manageEntityType(String type) {
        if (lastType == null) {
            lastType = type;
        }
        if (!type.equals(lastType)) {
            flush();
            lastType = type;
        }
    }

    private void manageRelationship(Map<String, Object> param) {
        Map<String, Object> relType = Util.map(
                "start", getLabels((Map<String, Object>) param.get("start")),
                "end", getLabels((Map<String, Object>) param.get("end")),
                "label", getType(param));
        if (lastRelTypes == null) {
            lastRelTypes = relType;
        }
        if (!relType.equals(lastRelTypes)) {
            flush();
            lastRelTypes = relType;
        }
    }

    private void manageNode(Map<String, Object> param) {
        List<String> labels = getLabels(param);
        if (lastLabels == null) {
            lastLabels = labels;
        }
        if (!labels.equals(lastLabels)) {
            flush();
            lastLabels = labels;
        }
    }

    private void updateReporter(String type, Map<String, Object> properties) {
        final int size = properties.size() + 1; // +1 is for the "neo4jImportId"
        switch (type) {
            case "node":
                reporter.update(1, 0, size);
                break;
            case "relationship":
                reporter.update(0, 1, size);
                break;
            default:
                throw new IllegalArgumentException("Current type not supported: " + type);
        }
    }

    private Stream<Map.Entry<String, Object>> flatMap(Map<String, Object> map, String key) {
        final String prefix = key != null ? key : "";
        return map.entrySet().stream()
                .flatMap(e -> {
                    if (e.getValue() instanceof Map) {
                        return flatMap((Map<String, Object>) e.getValue(), prefix + "." + e.getKey());
                    } else {
                        return Stream.of(new AbstractMap.SimpleEntry<>(prefix + "." + e.getKey(), e.getValue()));
                    }
                });
    }

    private List<Object> convertList(Collection<Object> coll, String classType) {
        return coll.stream()
                .map(c -> {
                    if (c instanceof Collection) {
                        return convertList((Collection<Object>) c, classType);
                    }
                    return convertMappedValue(c, classType);
                })
                .collect(Collectors.toList());
    }

    private Map<String, Object> convertProperties(String type, Map<String, Object> properties, String keyPrefix) {
        return properties.entrySet().stream()
                .flatMap(e -> {
                     if (e.getValue() instanceof Map) {
                         Map<String, Object> map = (Map<String, Object>) e.getValue();
                         String classType = getClassType(type, e.getKey());
                         if (classType != null && "POINT".equals(classType.toUpperCase())) {
                             return Stream.of(e);
                         }
                         return flatMap(map, e.getKey());
                     } else {
                         return Stream.of(e);
                     }
                })
                .map(e -> {
                    String key = e.getKey();
                    final String classType = getClassType(type, key);
                    if (e.getValue() instanceof Collection) {
                        final List<Object> coll = convertList((Collection<Object>) e.getValue(), classType);
                        return new AbstractMap.SimpleEntry<>(e.getKey(), coll);
                    } else {
                        return new AbstractMap.SimpleEntry<>(e.getKey(),
                                convertMappedValue(e.getValue(), classType));
                    }
                })
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    }

    private String getClassType(String type, String key) {
        final String classType;
        switch (type) {
            case "node":
                classType = importJsonConfig.typeForNode(lastLabels, key);
                break;
            case "relationship":
                classType = importJsonConfig.typeForRel((String) lastRelTypes.get("label"), key);
                break;
            default:
                classType = null;
                break;
        }
        return classType;
    }

    private Object convertMappedValue(Object value, String classType) {
        if (classType == null) {
           return value;
        }
        switch (classType.toUpperCase()) {
            case "POINT":
                value = toPoint((Map<String, Object>) value);
                break;
            case "LOCALDATE":
                value = LocalDate.parse((String) value);
                break;
            case "LOCALTIME":
                value = LocalTime.parse((String) value);
                break;
            case "LOCALDATETIME":
                value = LocalDateTime.parse((String) value);
                break;
            case "DURATION":
                value = DurationValue.parse((String) value);
                break;
            case "OFFSETTIME":
                value = OffsetTime.parse((String) value);
                break;
            case "ZONEDDATETIME":
                value = ZonedDateTime.parse((String) value);
                break;
            default:
                break;
        }
        return value;
    }

    private PointValue toPoint(Map<String, Object> pointMap) {
        double x;
        double y;
        Double z = null;

        final CoordinateReferenceSystem crs = CoordinateReferenceSystem.byName((String) pointMap.get("crs"));
        if (crs.getName().startsWith("wgs-84")) {
            x = (double) pointMap.get("latitude");
            y = (double) pointMap.get("longitude");
            if (crs.getName().endsWith("-3d")) {
                z = (double) pointMap.get("height");
            }
        } else {
            x = (double) pointMap.get("x");
            y = (double) pointMap.get("y");
            if (crs.getName().endsWith("-3d")) {
                z = (double) pointMap.get("z");
            }
        }

        return z != null ? Values.pointValue(crs, x, y, z) : Values.pointValue(crs, x, y);
    }

    private String getType(Map<String, Object> param) {
        return Util.quote((String) param.get("label"));
    }

    private List<String> getLabels(Map<String, Object> param) {
        return ((List<String>) param.getOrDefault("labels", Collections.emptyList())).stream()
                .map(Util::quote)
                .collect(Collectors.toList());
    }

    private String getLabelString(List<String> labels) {
        labels = labels == null ? Collections.emptyList() : labels;
        final String join = StringUtils.join(labels, ":");
        return join.isBlank() ? join : (":" + join);
    }

    private void write(Transaction tx, List<Map<String, Object>> resultList) {
        if (resultList.isEmpty()) return;
        final String type = (String) resultList.get(0).get("type");
        String query = null;
        switch (type) {
            case "node":
                query = String.format(CREATE_NODE, getLabelString(lastLabels), importJsonConfig.getImportIdName());
                break;
            case "relationship":
                String rel = (String) lastRelTypes.get("label");
                query = String.format(CREATE_RELS, getLabelString((List<String>) lastRelTypes.get("start")),
                        importJsonConfig.getImportIdName(),
                        getLabelString((List<String>) lastRelTypes.get("end")),
                        rel);
                break;
            default:
                throw new IllegalArgumentException("Current type not supported: " + type);
        }
        if (StringUtils.isNotBlank(query)) {
            db.executeTransactionally(query, Collections.singletonMap("rows", resultList));
        }
    }

    private Collection<List<Map<String, Object>>> chunkData() {
        AtomicInteger chunkCounter = new AtomicInteger(0);
        return paramList.stream()
                .collect(Collectors.groupingBy(it -> chunkCounter.getAndIncrement() / unwindBatchSize))
                .values();
    }

    @Override
    public void close() throws IOException {
        flush();
        reporter.done();
    }

    private void flush() {
        if (!paramList.isEmpty()) {
            final Collection<List<Map<String, Object>>> results = chunkData();
            try (final Transaction tx = db.beginTx()) {
                results.forEach(resultList -> write(tx, resultList));
            }
            paramList.clear();
        }
    }
}
