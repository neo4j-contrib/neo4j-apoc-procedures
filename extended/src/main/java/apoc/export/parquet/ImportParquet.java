package apoc.export.parquet;

import apoc.Extended;
import apoc.Pools;
import apoc.export.util.BatchTransaction;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.Util;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.storable.Value;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static apoc.export.parquet.ParquetReadUtil.getReader;
import static apoc.export.parquet.ParquetUtil.FIELD_ID;
import static apoc.export.parquet.ParquetUtil.FIELD_LABELS;
import static apoc.export.parquet.ParquetUtil.FIELD_SOURCE_ID;
import static apoc.export.parquet.ParquetUtil.FIELD_TARGET_ID;
import static apoc.export.parquet.ParquetUtil.FIELD_TYPE;

@Extended
public class ImportParquet {

    @Context
    public GraphDatabaseService db;

    @Context
    public Pools pools;

    @Context
    public Log log;

    @Procedure(name = "apoc.import.parquet", mode = Mode.WRITE)
    @Description("Imports parquet from the provided file or binary")
    public Stream<ProgressInfo> importParquet(
            @Name("input") Object input,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        ProgressInfo result =
                Util.inThread(pools, () -> {

                    String file = null;
                    String sourceInfo = "binary";
                    if (input instanceof String) {
                        file =  (String) input;
                        sourceInfo = "file";
                    }
                    final ParquetConfig conf = new ParquetConfig(config);

                    final Map<Long, Long> idMapping = new HashMap<>();
                    try (ApocParquetReader reader = getReader(input, conf)) {

                        final ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo(file, sourceInfo, "parquet"));

                        BatchTransaction btx = new BatchTransaction(db, conf.getBatchSize(), reporter);

                        try {
                            Map<String, Object> recordMap;
                            while ((recordMap = reader.getRecord()) != null) {

                                String relType = (String) recordMap.remove(FIELD_TYPE);
                                if (relType == null) {
                                    // is node
                                    Object[] stringLabels = (Object[]) recordMap.remove(FIELD_LABELS);
                                    Label[] labels = Optional.ofNullable(stringLabels)
                                            .map(l -> Arrays.stream(l).map(Object::toString).map(Label::label).toArray(Label[]::new))
                                            .orElse(new Label[]{});
                                    final Node node = btx.getTransaction().createNode(labels);

                                    long id = (long) recordMap.remove(FIELD_ID);
                                    idMapping.put(id, node.getId());

                                    addProps(recordMap, node);
                                    reporter.update(1, 0, recordMap.size());
                                } else {
                                    // is relationship
                                    long sourceId = (long) recordMap.remove(FIELD_SOURCE_ID);
                                    Long idSource = idMapping.get(sourceId);
                                    final Node source = btx.getTransaction().getNodeById(idSource);

                                    long targetId = (long) recordMap.remove(FIELD_TARGET_ID);
                                    Long idTarget = idMapping.get(targetId);
                                    final Node target = btx.getTransaction().getNodeById(idTarget);

                                    final Relationship rel = source.createRelationshipTo(target, RelationshipType.withName(relType));
                                    addProps(recordMap, rel);
                                    reporter.update(0, 1, recordMap.size());
                                }

                                btx.increment();
                            }
                            btx.doCommit();
                        } catch (RuntimeException e) {
                            btx.rollback();
                            throw e;
                        } finally {
                            btx.close();
                        }

                        return reporter.getTotal();
                    }
                });
        return Stream.of(result);
    }

    private void addProps(Map<String, Object> recordMap, Entity rel) {
        recordMap.forEach((k, v)-> {
            Object value = getNeo4jObject(v);
            rel.setProperty(k, value);
        });
    }

    private Object getNeo4jObject(Object object) {
        if (object instanceof Value) {
            return ((Value) object).asObject();
        }
        if (object instanceof Collection) {
            // convert to String[], other array types can be converted via `mapping` config
            return ((Collection) object)
                    .stream()
                    .map(Object::toString).toArray(String[]::new);
        }
        return object;
    }
}
