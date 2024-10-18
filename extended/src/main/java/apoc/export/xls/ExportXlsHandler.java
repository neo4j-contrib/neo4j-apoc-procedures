package apoc.export.xls;

import apoc.ApocConfig;
import apoc.export.util.ExportConfig;
import apoc.export.util.ProgressReporter;
import apoc.result.ExportProgressInfo;
import apoc.result.ProgressInfo;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.util.FileUtils.getOutputStream;

public class ExportXlsHandler {
    public static final String XLS_MISSING_DEPS_ERROR = "Cannot find the needed jar into the plugins folder in order to use . \n" +
                                                        "Please see the documentation: https://neo4j.com/labs/apoc/5/overview/apoc.export/apoc.export.xls.all/#_install_dependencies";


    public static Stream<ExportProgressInfo> getProgressInfoStream(String fileName, String source, Object data, Map<String, Object> configMap, ApocConfig apocConfig, GraphDatabaseService db) throws IOException {
        ExportConfig c = new ExportConfig(configMap);
        apocConfig.checkWriteAllowed(c, fileName);
        try (Transaction tx = db.beginTx();
             OutputStream out = getOutputStream(fileName, c);
             SXSSFWorkbook wb = new SXSSFWorkbook(-1)) {

            XlsExportConfig config = new XlsExportConfig(configMap);
            ProgressInfo progressInfo = new ExportProgressInfo(fileName, source, "xls");
            progressInfo.setBatches(config.getBatchSize());
            ProgressReporter reporter = new ProgressReporter(null, null, progressInfo);

            Map<Class, CellStyle> styles = buildCellStyles(config, wb);

            if (data instanceof SubGraph) {
                dumpSubGraph((SubGraph) data, config, reporter, wb, styles);

            } else if (data instanceof Result) {
                Result result = (Result) data;
                dumpResult(result, config, wb, styles);
            } else {
                throw new UnsupportedOperationException("cannot handle " + data.getClass());
            }

            wb.write(out);
            wb.dispose();
            reporter.done();
            tx.commit();
            return Stream.of((ExportProgressInfo) reporter.getTotal());
        }
    }

    private static void dumpResult(Result result, XlsExportConfig config, SXSSFWorkbook wb, Map<Class, CellStyle> styles) {
        SXSSFSheet sheet = wb.createSheet();
        sheet.trackAllColumnsForAutoSizing();

        // header row
        int columnNum = 0;
        int rowNum = 0;
        Row headerRow = sheet.createRow(rowNum++);

        for (String header: result.columns()) {
            Cell cell = headerRow.createCell(columnNum);
            sheet.autoSizeColumn(columnNum);
            cell.setCellValue(header);
            columnNum++;
        }

        while (result.hasNext()) {
            Map<String, Object> map = result.next();
            Row row = sheet.createRow(rowNum++);
            columnNum = 0;
            for (String header: result.columns()) {
                columnNum = amendCell(row, columnNum, map.get(header), config, styles);
            }
        }
    }

    private static void dumpSubGraph(SubGraph subgraph, XlsExportConfig config, ProgressReporter reporter, SXSSFWorkbook wb, Map<Class, CellStyle> styles) {
        // what's in the triple used below?
        // left: sheet instance
        // middle: list of "magic" property keys: <id> for nodes, <startNodeId> and <endNodeId> for rels
        // right: list of  "normal" property keys
        Map<String, Triple<SXSSFSheet, List<String>, List<String>>> sheetAndPropsForName = new HashMap<>();

        for (Node node : subgraph.getNodes()) {
            final List<String> labels;
            if (config.isJoinLabels()) {
                labels = Collections.singletonList(StreamSupport.stream(node.getLabels().spliterator(), false)
                        .map(Label::name)
                        .collect(Collectors.joining(",")));
            } else {
                labels = StreamSupport.stream(node.getLabels().spliterator(), false)
                        .map(Label::name)
                        .collect(Collectors.toList());
            }
            for (String label :labels) {
                String labelName = (config.isPrefixSheetWithEntityType() ? "Node-" : "") + label;
                createRowForEntity(wb, sheetAndPropsForName, node, labelName, reporter, config, styles);
            }
        }
        for (Relationship relationship: subgraph.getRelationships()) {
            String relationshipType = (config.isPrefixSheetWithEntityType() ? "Rel-" : "") + relationship.getType().name();
            createRowForEntity(wb, sheetAndPropsForName, relationship, relationshipType, reporter, config,styles);
        }

        // spit out header lines
        for (Triple<SXSSFSheet,List<String>, List<String>> triple: sheetAndPropsForName.values()) {
            Sheet sheet = triple.getLeft();

            List<String> magicKeys = triple.getMiddle();
            List<String> keys = triple.getRight();
            Row row = sheet.getRow(0);
            int cellNum = 0;
            for (String key: ListUtils.union(magicKeys,keys)) {
                sheet.autoSizeColumn(cellNum);
                Cell cell = row.createCell(cellNum++);
                cell.setCellValue(key);

            }
        }
    }

    private static Map<Class, CellStyle> buildCellStyles(XlsExportConfig config, SXSSFWorkbook wb) {
        CreationHelper createHelper = wb.getCreationHelper();
        CellStyle dateTimeCellStyle = wb.createCellStyle();
        dateTimeCellStyle.setDataFormat(createHelper.createDataFormat().getFormat(config.getDateTimeStyle()));
        CellStyle dateCellStyle = wb.createCellStyle();
        dateCellStyle.setDataFormat(createHelper.createDataFormat().getFormat(config.getDateStyle()));

        Map<Class, CellStyle> styles = new HashMap<>();
        styles.put(ZonedDateTime.class, dateTimeCellStyle);
        styles.put(LocalDate.class, dateCellStyle);
        return styles;
    }

    private static void createRowForEntity(Workbook wb, Map<String, Triple<SXSSFSheet, List<String>, List<String>>> sheetAndPropsForName, Entity entity, String sheetName, ProgressReporter reporter, XlsExportConfig config, Map<Class, CellStyle> styles) {
        Triple<SXSSFSheet, List<String>, List<String>> triple = sheetAndPropsForName.computeIfAbsent(sheetName, s -> {
            SXSSFSheet sheet = (SXSSFSheet) wb.createSheet(sheetName);
            sheet.trackAllColumnsForAutoSizing();
            sheet.createRow(0); // placeholder for header line
            return Triple.of(
                    sheet,
                    entity instanceof Node ?
                            Arrays.asList(config.getHeaderNodeId()) :
                            Arrays.asList(config.getHeaderRelationshipId(), config.getHeaderStartNodeId(), config.getHeaderEndNodeId()),
                    new ArrayList<>());
        });
        Sheet sheet = triple.getLeft();
        List<String> propertyKeys = triple.getRight();

        int lastRowNum = sheet.getLastRowNum();
        Row row = sheet.createRow(lastRowNum+1);
        int cellNum = 0;
        SortedMap<String, Object> props = new TreeMap<>(entity.getAllProperties()); // copy props

        if (entity instanceof Node) {
            Node node = (Node) entity;
            Cell idCell = row.createCell(cellNum++);
            idCell.setCellValue(((Long)(node.getId())).doubleValue());
            reporter.update(1, 0, props.size());
        } else if (entity instanceof Relationship) {
            Relationship relationship = (Relationship) entity;
            Cell idCell = row.createCell(cellNum++);
            idCell.setCellValue(((Long)(relationship.getId())).doubleValue());
            Cell fromCell = row.createCell(cellNum++);
            fromCell.setCellValue(((Long)(relationship.getStartNodeId())).doubleValue());
            Cell toCell = row.createCell(cellNum++);
            toCell.setCellValue(((Long)(relationship.getEndNodeId())).doubleValue());
            reporter.update(0, 1, props.size());
        }

        // deal with property keys already being known
        for (String key: propertyKeys) {
            cellNum = amendCell(row, cellNum, props.remove(key), config, styles);
        }

        // add remaining properties as new keys
        for (String key: props.keySet()) {
            propertyKeys.add(key);
            cellNum = amendCell(row, cellNum, props.get(key), config, styles);
        }
    }

    private static int amendCell(Row row, int cellNum, Object value, XlsExportConfig config, Map<Class, CellStyle> styles) {
        Cell cell = row.createCell(cellNum++);

        if (value == null) {
            cell.setCellType(CellType.BLANK);
        } else {

            CellStyle cellStyle = styles.get(value.getClass());
            if (cellStyle!=null) {
                cell.setCellStyle(cellStyle);
            }

            if (value instanceof String) {
                cell.setCellValue((String) value);
            } else if (value instanceof Number) {
                cell.setCellValue(((Number) value).doubleValue());
            } else if (value instanceof Boolean) {
                cell.setCellValue(((Boolean) value).booleanValue());
            } else if (value instanceof String[]) {
                String[] values = (String[]) value;
                cell.setCellValue(Arrays.stream(values).collect(Collectors.joining(config.getArrayDelimiter())));
            } else if (value instanceof long[]) {
                long[] values = (long[]) value;
                cell.setCellValue(Arrays.stream(values).mapToObj(Long::toString).collect(Collectors.joining(config.getArrayDelimiter())));
            } else if (value instanceof double[]) {
                double[] values = (double[]) value;
                cell.setCellValue(Arrays.stream(values).mapToObj(Double::toString).collect(Collectors.joining(config.getArrayDelimiter())));
            } else if (value instanceof List) {
                List values = (List) value;
                String collect = ((Stream<String>) values.stream().map(x -> x.toString())).collect(Collectors.joining(config.getArrayDelimiter()));
                cell.setCellValue(collect);
            } else if (value instanceof LocalDate) {
                LocalDate localDate = (LocalDate) value;
                cell.setCellValue( Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant()));
            } else if (value instanceof ZonedDateTime) {
                ZonedDateTime zondedDateTime = (ZonedDateTime) value;
                cell.setCellValue( Date.from(zondedDateTime.toInstant()));
            } else {
                cell.setCellValue(value.toString());
                //throw new IllegalArgumentException("dunno know how to handle type " + value.getClass() + ". Please report this as a bug.");
            }
        }
        return cellNum;
    }
}
