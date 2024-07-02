package apoc.export.arrow;

import apoc.Extended;
import apoc.Pools;
import apoc.export.util.BatchTransaction;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.FileUtils;
import apoc.util.Util;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static apoc.util.ExtendedUtil.toValidValue;

@Extended
public class ImportArrow {

    // TODO: field similar to the one placed in ArrowUtils (placed in core)
    //  when the Arrow procedures will be placed in extended remove these lines 
    //  and replace FIELD_ID with FIELD_ID.getName(), FIELD_LABELS with FIELD_LABELS.getName(), etc..
    public static String FIELD_ID = "<id>";
    public static String FIELD_LABELS = "labels";
    public static String FIELD_SOURCE_ID = "<source.id>";
    public static String FIELD_TARGET_ID = "<target.id>";
    public static String FIELD_TYPE = "<type>";
    // -- end ArrowUtils fields
    
    @Context
    public Pools pools;

    @Context
    public GraphDatabaseService db;
    
    @Context
    public URLAccessChecker urlAccessChecker;
    
    
    @Procedure(name = "apoc.import.arrow", mode = Mode.WRITE)
    @Description("Imports arrow from the provided arrow file or byte array")
    public Stream<ProgressInfo> importFile(@Name("input") Object input, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {

        ProgressInfo result =
                Util.inThread(pools, () -> {
                    String file = null;
                    String sourceInfo = "binary";
                    if (input instanceof String) {
                        file =  (String) input;
                        sourceInfo = "file";
                    }

                    final ArrowConfig conf = new ArrowConfig(config);

                    final Map<Long, Long> idMapping = new HashMap<>();
                    
                    AtomicInteger counter = new AtomicInteger();
                    try (ArrowReader reader = getReader(input);
                         VectorSchemaRoot schemaRoot = reader.getVectorSchemaRoot()) {

                        final ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo(file, sourceInfo, "arrow"));
                        BatchTransaction btx = new BatchTransaction(db, conf.getBatchSize(), reporter);
                        try {
                            while (hasElements(counter, reader, schemaRoot)) {

                                final Map<String, Object> row = schemaRoot.getFieldVectors()
                                        .stream()
                                        .collect(
                                                HashMap::new,
                                                (map, fieldVector) -> {
                                                    Object read = read(fieldVector, counter.get(), conf);
                                                    if (read == null) {
                                                        return;
                                                    }
                                                    map.put(fieldVector.getName(), read);
                                                },
                                                HashMap::putAll);

                                String relType = (String) row.remove(FIELD_TYPE);
                                if (relType == null) {
                                    // is node
                                    String[] stringLabels = (String[]) row.remove(FIELD_LABELS);
                                    Label[] labels = Optional.ofNullable(stringLabels)
                                            .map(l -> Arrays.stream(l).map(Label::label).toArray(Label[]::new))
                                            .orElse(new Label[]{});
                                    final Node node = btx.getTransaction().createNode(labels);

                                    long id = (long) row.remove(FIELD_ID);
                                    idMapping.put(id, node.getId());

                                    addProps(row, node);
                                    reporter.update(1, 0, row.size());
                                } else {
                                    // is relationship
                                    long sourceId = (long) row.remove(FIELD_SOURCE_ID);
                                    Long idSource = idMapping.get(sourceId);
                                    final Node source = btx.getTransaction().getNodeById(idSource);

                                    long targetId = (long) row.remove(FIELD_TARGET_ID);
                                    Long idTarget = idMapping.get(targetId);
                                    final Node target = btx.getTransaction().getNodeById(idTarget);

                                    final Relationship rel = source.createRelationshipTo(target, RelationshipType.withName(relType));
                                    addProps(row, rel);
                                    reporter.update(0, 1, row.size());
                                }
                                
                                counter.incrementAndGet();
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

    
    private ArrowReader getReader(Object input) throws IOException, URISyntaxException, URLAccessValidationError {
        RootAllocator allocator = new RootAllocator();
        if (input instanceof String) {
            final SeekableByteChannel channel = FileUtils.inputStreamFor(input, null, null, null, urlAccessChecker)
                    .asChannel();
            return new ArrowFileReader(channel, allocator);
        }
        ByteArrayInputStream inputStream = new ByteArrayInputStream((byte[]) input);
        return new ArrowStreamReader(inputStream, allocator);
    }


    private static boolean hasElements(AtomicInteger counter, ArrowReader reader, VectorSchemaRoot schemaRoot) throws IOException {
        if (counter.get() >= schemaRoot.getRowCount()) {
            if (reader.loadNextBatch()) {
                counter.set(0);
            } else {
                return false;
            }
        }
        
        return true;
    }

    private static Object read(FieldVector fieldVector, int index, ArrowConfig conf) {
        
        if (fieldVector.isNull(index)) {
            return null;
        } else if (fieldVector instanceof BitVector) {
            BitVector fe = (BitVector) fieldVector;
            return fe.get(index) == 1;
        } else {
            Object object = fieldVector.getObject(index);
            if (object instanceof Collection coll && coll.isEmpty()) {
                return null;
            }
            return toValidValue(object, fieldVector.getName(), conf.getMapping());
        }
    }

    private void addProps(Map<String, Object> row, Entity rel) {
        row.forEach(rel::setProperty);
    }

    /**
     * Analog to ArrowConfig present in APOC Core.
     * TODO Merge these 2 classes when arrow procedure will be moved to APOC Extended
     */
    public static class ArrowConfig {

        private final int batchSize;
        private final Map<String, Object> mapping;

        public ArrowConfig(Map<String, Object> config) {
            if (config == null) {
                config = Collections.emptyMap();
            }
            this.mapping = (Map<String, Object>) config.getOrDefault("mapping", Map.of());
            this.batchSize = Util.toInteger(config.getOrDefault("batchSize", 2000));
        }

        public int getBatchSize() {
            return batchSize;
        }

        public Map<String, Object> getMapping() {
            return mapping;
        }
    }

}
