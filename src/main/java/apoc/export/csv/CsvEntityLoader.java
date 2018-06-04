package apoc.export.csv;

import apoc.export.util.CountingReader;
import apoc.load.LoadCsv;
import apoc.util.FileUtils;
import au.com.bytecode.opencsv.CSVReader;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CsvEntityLoader {

    /**
     * Loads nodes from a CSV file with given labels to an online database, and fills the {@code idMapping},
     * which will be used by the {@link #loadRelationships(String, String, CsvLoaderConfig, GraphDatabaseService, Map)}
     * method.
     *
     * @param fileName URI of the CSV file representing the node
     * @param labels list of node labels to be applied to each node
     * @param clc configuration object
     * @param db running database instance
     * @param idMapping to be filled with the mapping between the CSV ids and the DB's internal node ids
     * @throws IOException
     */
    public static void loadNodes(final String fileName, final List<String> labels, final CsvLoaderConfig clc,
                                 final GraphDatabaseService db, final Map<String, Map<String, Long>> idMapping) throws IOException {
        final CountingReader reader = FileUtils.readerFor(fileName);
        final String header = readFirstLine(reader);
        reader.skip(clc.getSkipLines() - 1);
        final List<CsvHeaderField> fields = CsvHeaderFields.processHeader(header, clc.getDelimiter(), clc.getQuotationCharacter());

        final Optional<CsvHeaderField> idField = fields.stream()
                .filter(f -> CsvLoaderConstants.ID_FIELD.equals(f.getType()))
                .findFirst();

        final Optional<String> idAttribute = idField.isPresent() ? Optional.of(idField.get().getName()) : Optional.empty();
        final String idSpace = idField.isPresent() ? idField.get().getIdSpace() : CsvLoaderConstants.DEFAULT_IDSPACE;

        idMapping.putIfAbsent(idSpace, new HashMap<>());
        final Map<String, Long> idspaceIdMapping = idMapping.get(idSpace);

        final Map<String, LoadCsv.Mapping> mapping = fields.stream().collect(
                Collectors.toMap(
                        CsvHeaderField::getName,
                        f -> {
                            final Map<String, Object> mappingMap = Collections
                                    .unmodifiableMap(Stream.of(
                                            new AbstractMap.SimpleEntry<>("type", f.getType()),
                                            new AbstractMap.SimpleEntry<>("array", f.isArray())
                                    ).collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue)));
                            return new LoadCsv.Mapping(f.getName(), mappingMap, clc.getArrayDelimiter(), false);
                        }
                )
        );

        final CSVReader csv = new CSVReader(reader, clc.getDelimiter());

        final String[] loadCsvCompatibleHeader = fields.stream().map(f -> f.getName()).toArray(String[]::new);
        int lineNo = 0;
        try (Transaction tx = db.beginTx()) {
            for (String[] line : csv.readAll()) {
                lineNo++;

                final EnumSet<LoadCsv.Results> results = EnumSet.of(LoadCsv.Results.map);
                final LoadCsv.CSVResult result = new LoadCsv.CSVResult(
                        loadCsvCompatibleHeader, line, lineNo, false, mapping, Collections.emptyList(), results
                );

                // create node and add id to the mapping
                final Node node = db.createNode();
                if (idField.isPresent()) {
                    idspaceIdMapping.put(result.map.get(idAttribute.get()).toString(), node.getId());
                }

                // add labels
                for (String label : labels) {
                    node.addLabel(Label.label(label));
                }
                // add properties
                for (CsvHeaderField field : fields) {
                    final String name = field.getName();
                    Object value = result.map.get(name);

                    if (field.isMeta()) {
                        final List<String> customLabels = (List<String>) value;
                        for (String customLabel : customLabels) {
                            node.addLabel(Label.label(customLabel));
                        }
                    } else if (field.isId()) {
                        final Object idValue;
                        if (clc.getStringIds()) {
                            idValue = value;
                        } else {
                            idValue = Long.valueOf((String) value);
                        }
                        node.setProperty(field.getName(), idValue);
                    } else {
                        CsvPropertyConverter.addPropertiesToGraphEntity(node, field, value);
                    }
                }
            }
            tx.success();
        }
    }

    /**
     * Loads relationships from a CSV file with given relationship types to an online database,
     * using the {@code idMapping} created by the
     * {@link #loadNodes(String, List, CsvLoaderConfig, GraphDatabaseService, Map)} method.
     *
     * @param fileName URI of the CSV file representing the relationship
     * @param type relationship type to be applied to each relationships
     * @param clc configuration object
     * @param db running database instance
     * @param idMapping stores mapping between the CSV ids and the DB's internal node ids
     * @throws IOException
     */
    public static void loadRelationships(
            final String fileName, final String type, final CsvLoaderConfig clc,
            final GraphDatabaseService db, final Map<String, Map<String, Long>> idMapping) throws IOException {
        final CountingReader reader = FileUtils.readerFor(fileName);
        final String header = readFirstLine(reader);
        final List<CsvHeaderField> fields = CsvHeaderFields.processHeader(header, clc.getDelimiter(), clc.getQuotationCharacter());

        final CsvHeaderField startIdField = fields.stream()
                .filter(f -> CsvLoaderConstants.START_ID_FIELD.equals(f.getType()))
                .findFirst().get();

        final CsvHeaderField endIdField = fields.stream()
                .filter(f -> CsvLoaderConstants.END_ID_FIELD.equals(f.getType()))
                .findFirst().get();

        final List<CsvHeaderField> edgePropertiesFields = fields.stream()
                .filter(field -> !CsvLoaderConstants.START_ID_FIELD.equals(field.getType()))
                .filter(field -> !CsvLoaderConstants.END_ID_FIELD.equals(field.getType()))
                .collect(Collectors.toList());

        final Map<String, LoadCsv.Mapping> mapping = fields.stream().collect(
                Collectors.toMap(
                        CsvHeaderField::getName,
                        f -> {
                            final Map<String, Object> mappingMap = Collections
                                    .unmodifiableMap(Stream.of(
                                            new AbstractMap.SimpleEntry<>("type", f.getType()),
                                            new AbstractMap.SimpleEntry<>("array", f.isArray())
                                    ).collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue)));

                            return new LoadCsv.Mapping(f.getName(), mappingMap, clc.getArrayDelimiter(), false);
                        }
                )
        );

        final CSVReader csv = new CSVReader(reader, clc.getDelimiter());
        final String[] loadCsvCompatibleHeader = fields.stream().map(f -> f.getName()).toArray(String[]::new);

        int lineNo = 0;
        try (Transaction tx = db.beginTx()) {
            for (String[] line : csv.readAll()) {
                lineNo++;

                final EnumSet<LoadCsv.Results> results = EnumSet.of(LoadCsv.Results.map);
                final LoadCsv.CSVResult result = new LoadCsv.CSVResult(
                        loadCsvCompatibleHeader, line, lineNo, false, mapping, Collections.emptyList(), results
                );

                final Object startId = result.map.get(CsvLoaderConstants.START_ID_ATTR);
                final Object startInternalId = idMapping.get(startIdField.getIdSpace()).get(startId);
                if (startInternalId == null) {
                    throw new IllegalStateException("Node for id space " + endIdField.getIdSpace() + " and id " + startId + " not found");
                }
                final Node source = db.getNodeById((long) startInternalId);

                final Object endId = result.map.get(CsvLoaderConstants.END_ID_ATTR);
                final Object endInternalId = idMapping.get(endIdField.getIdSpace()).get(endId);
                if (endInternalId == null) {
                    throw new IllegalStateException("Node for id space " + endIdField.getIdSpace() + " and id " + endId + " not found");
                }
                final Node target = db.getNodeById((long) endInternalId);

                final String currentType;
                final Object overridingType = result.map.get(CsvLoaderConstants.TYPE_ATTR);
                if (overridingType != null && !((String) overridingType).isEmpty()) {
                    currentType = (String) overridingType;
                } else {
                    currentType = type;
                }
                final Relationship rel = source.createRelationshipTo(target, RelationshipType.withName(currentType));

                // add properties
                for (CsvHeaderField field : edgePropertiesFields) {
                    final String name = field.getName();
                    Object value = result.map.get(name);
                    CsvPropertyConverter.addPropertiesToGraphEntity(rel, field, value);
                }
            }

            tx.success();
        }
    }

    private static String readFirstLine(CountingReader reader) throws IOException {
        String line = "";
        int i;
        while ((i = reader.read()) != 0) {
            char c = (char) i;
            if (c == '\n') break;
            line += c;
        }
        return line;
    }

}
