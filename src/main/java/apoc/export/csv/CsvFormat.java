package apoc.export.csv;

import apoc.export.util.*;
import apoc.result.ProgressInfo;
import com.opencsv.CSVWriter;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.*;

import java.io.Reader;
import java.io.Writer;
import java.util.*;
import java.util.stream.Collectors;

import static apoc.export.util.MetaInformation.*;

/**
 * @author mh
 * @since 22.11.16
 */
public class CsvFormat implements Format {
    private final GraphDatabaseService db;
    private boolean applyQuotesToAll = true;

    public CsvFormat(GraphDatabaseService db) {
        this.db = db;
    }

    @Override
    public ProgressInfo load(Reader reader, Reporter reporter, ExportConfig config) throws Exception {
        return null;
    }

    @Override
    public ProgressInfo dump(SubGraph graph, Writer writer, Reporter reporter, ExportConfig config) throws Exception {
        try (Transaction tx = db.beginTx()) {
            CSVWriter out = getCsvWriter(writer, config);
            writeAll(graph, reporter, config, out);
            tx.success();
            reporter.done();
            writer.close();
            return reporter.getTotal();
        }
    }

    private CSVWriter getCsvWriter(Writer writer, ExportConfig config)
    {
        CSVWriter out;
        switch (config.isQuotes()) {
            case ExportConfig.NONE_QUOTES:
                out = new CSVWriter(writer,
                                    config.getDelimChar(),
                                    '\0', // quote char
                                    '\0', // escape char
                                    CSVWriter.DEFAULT_LINE_END);
                applyQuotesToAll = false;
                break;
            case ExportConfig.IF_NEEDED_QUUOTES:
                out = new CSVWriter(writer,
                                    config.getDelimChar(),
                                    ExportConfig.QUOTECHAR,
                                    '\0', // escape char
                                    CSVWriter.DEFAULT_LINE_END);
                applyQuotesToAll = false;
                break;
            case ExportConfig.ALWAYS_QUOTES:
            default:
                out = new CSVWriter(writer,
                                    config.getDelimChar(),
                                    ExportConfig.QUOTECHAR,
                                    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                                    CSVWriter.DEFAULT_LINE_END);
                applyQuotesToAll = true;
                break;
        }
        return out;
    }

    public ProgressInfo dump(Result result, Writer writer, Reporter reporter, ExportConfig config) throws Exception {
        try (Transaction tx = db.beginTx()) {
            CSVWriter out = getCsvWriter(writer, config);

            String[] header = writeResultHeader(result, out);

            String[] data = new String[header.length];
            result.accept((row) -> {
                for (int col = 0; col < header.length; col++) {
                    Object value = row.get(header[col]);
                    data[col] = FormatUtils.toString(value);
                    reporter.update(value instanceof Node ? 1: 0,value instanceof Relationship ? 1: 0 , value instanceof PropertyContainer ? 0 : 1);
                }
                out.writeNext(data, applyQuotesToAll);
                reporter.nextRow();
                return true;
            });
            tx.success();
            reporter.done();
            writer.close();
            return reporter.getTotal();
        }
    }

    public String[] writeResultHeader(Result result, CSVWriter out) {
        List<String> columns = result.columns();
        int cols = columns.size();
        String[] header = columns.toArray(new String[cols]);
        out.writeNext(header, applyQuotesToAll);
        return header;
    }

    public void writeAll(SubGraph graph, Reporter reporter, ExportConfig config, CSVWriter out) {
        Map<String,Class> nodePropTypes = collectPropTypesForNodes(graph);
        Map<String,Class> relPropTypes = collectPropTypesForRelationships(graph);

        List<String> nodeHeader = generateHeader(nodePropTypes, config.useTypes(), "_id:id", "_labels:label");
        List<String> relHeader = generateHeader(relPropTypes, config.useTypes(), "_start:id", "_end:id", "_type:label");
        List<String> header = new ArrayList<>(nodeHeader); header.addAll(relHeader);
        out.writeNext(header.toArray(new String[header.size()]), applyQuotesToAll);
        int cols = header.size();

        writeNodes(graph, out, reporter, nodePropTypes, cols, config.getBatchSize(), config.getDelim());
        writeRels(graph, out, reporter, relPropTypes, cols, nodeHeader.size(), config.getBatchSize(), config.getDelim());
    }
    public void writeAll2(SubGraph graph, Reporter reporter, ExportConfig config, CSVWriter out) {
        writeNodes(graph, out, reporter,config);
        writeRels(graph, out, reporter,config);
    }

    private List<String> generateHeader(Map<String, Class> propTypes, boolean useTypes, String... starters) {
        List<String> result = new ArrayList<>();
        Collections.addAll(result,starters);
        for (Map.Entry<String, Class> entry : propTypes.entrySet()) {
            String type = MetaInformation.typeFor(entry.getValue(), null);
            if (type==null || type.equals("string") || !useTypes) result.add(entry.getKey());
            else result.add(entry.getKey()+":"+ type);
        }
        if (!useTypes) return result.stream().map( s -> s.split(":")[0]).collect(Collectors.toList());
        return result;
    }

    private void writeNodes(SubGraph graph, CSVWriter out, Reporter reporter, ExportConfig config) {
        Map<String,Class> nodePropTypes = collectPropTypesForNodes(graph);
        List<String> nodeHeader = generateHeader(nodePropTypes, config.useTypes(), "_id:id", "_labels:label");
        String[] header = nodeHeader.toArray(new String[nodeHeader.size()]);
        out.writeNext(header, applyQuotesToAll); // todo types
        int cols = header.length;
        writeNodes(graph, out, reporter, nodePropTypes, cols, config.getBatchSize(), config.getDelim());
    }

    private void writeNodes(SubGraph graph, CSVWriter out, Reporter reporter, Map<String, Class> nodePropTypes, int cols, int batchSize, String delimiter) {
        String[] row=new String[cols];
        int nodes = 0;
        for (Node node : graph.getNodes()) {
            row[0]=String.valueOf(node.getId());
            row[1]=getLabelsString(node);
            collectProps(nodePropTypes.keySet(), node, reporter, row, 2, delimiter);
            out.writeNext(row, applyQuotesToAll);
            nodes++;
            if (batchSize==-1 || nodes % batchSize == 0) {
                reporter.update(nodes, 0, 0);
                nodes = 0;
            }
        }
        if (nodes>0) {
            reporter.update(nodes, 0, 0);
        }
    }

    private void collectProps(Collection<String> fields, PropertyContainer pc, Reporter reporter, String[] row, int offset, String delimiter) {
        for (String field : fields) {
            if (pc.hasProperty(field)) {
                row[offset] = FormatUtils.toString(pc.getProperty(field));
                reporter.update(0,0,1);
            }
            else {
                row[offset] = "";
            }
            offset++;
        }
    }

    private void writeRels(SubGraph graph, CSVWriter out, Reporter reporter, ExportConfig config) {
        Map<String,Class> relPropTypes = collectPropTypesForRelationships(graph);
        List<String> header = generateHeader(relPropTypes, config.useTypes(), "_start:id", "_end:id", "_type:label");
        out.writeNext(header.toArray(new String[header.size()]), applyQuotesToAll);
        int cols = header.size();
        int offset = 0;
        writeRels(graph, out, reporter, relPropTypes, cols, offset, config.getBatchSize(), config.getDelim());
    }

    private void writeRels(SubGraph graph, CSVWriter out, Reporter reporter, Map<String, Class> relPropTypes, int cols, int offset, int batchSize, String delimiter) {
        String[] row=new String[cols];
        int rels = 0;
        for (Relationship rel : graph.getRelationships()) {
            row[offset]=String.valueOf(rel.getStartNode().getId());
            row[offset+1]=String.valueOf(rel.getEndNode().getId());
            row[offset+2]=rel.getType().name();
            collectProps(relPropTypes.keySet(), rel, reporter, row, 3 + offset, delimiter);
            out.writeNext(row,
                          applyQuotesToAll);
            rels++;
            if (batchSize==-1 || rels % batchSize == 0) {
                reporter.update(0, rels, 0);
                rels = 0;
            }
        }
        if (rels > 0) {
            reporter.update(0, rels, 0);
        }
    }
}
