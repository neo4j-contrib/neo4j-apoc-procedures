package apoc.load.xls;

import apoc.export.util.CountingInputStream;
import org.apache.poi.ss.usermodel.*;
import org.neo4j.values.storable.LocalDateTimeValue;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;

import static apoc.util.Util.cleanUrl;

public class LoadXlsHandler {

    public static class XLSSpliterator extends Spliterators.AbstractSpliterator<LoadXls.XLSResult> {
        private final Sheet sheet;
        private final LoadXls.Selection selection;
        private final String[] header;
        private final String url;
        private final long limit;
        private final boolean ignore;
        private final boolean skipNulls;
        private final Map<String, LoadXls.Mapping> mapping;
        private final List<Object> nullValues;
        private final long skip;
        long lineNo;

        public XLSSpliterator(Sheet sheet, LoadXls.Selection selection, String[] header, String url, long skip, long limit, boolean ignore, Map<String, LoadXls.Mapping> mapping, List<Object> nullValues, boolean skipNulls) throws IOException {
            super(Long.MAX_VALUE, Spliterator.ORDERED);
            this.sheet = sheet;
            this.selection = selection;
            this.header = header;
            this.url = url;
            this.ignore = ignore;
            this.mapping = mapping;
            this.nullValues = nullValues;
            int headerOffset = header != null ? 1 : 0;
            this.skip = skip + selection.getOrDefault(selection.top, sheet.getFirstRowNum()) + headerOffset;
            this.limit = limit == Long.MAX_VALUE ? selection.getOrDefault(selection.bottom, sheet.getLastRowNum()) : skip + limit;
            lineNo = this.skip;
            this.skipNulls = skipNulls;
        }

        @Override
        public boolean tryAdvance(Consumer<? super LoadXls.XLSResult> action) {
            try {
                Row row = sheet.getRow((int)lineNo);
                if (lineNo <= limit) {

                    Object[] list;
                    if (row != null) {
                        list = extract(row, selection);
                        action.accept(new LoadXls.XLSResult(header, list, lineNo-skip, ignore,mapping, nullValues));
                        lineNo++;
                        return true;
                    }
                    if (skipNulls && lineNo <= sheet.getLastRowNum()) {
                        // list with null values (i.e.: empty xls row)
                        lineNo++;
                        return tryAdvance(action);
                    }
                    return false;
                }
                return false;
            } catch (Exception e) {
                throw new RuntimeException("Error reading XLS from URL " + cleanUrl(url) + " at " + lineNo, e);
            }
        }
    }

    public static XLSSpliterator getXlsSpliterator(String url, CountingInputStream stream, LoadXls.Selection selection, long skip, boolean hasHeader, long limit, List<String> ignore, List<Object> nullValues, Map<String, LoadXls.Mapping> mappings, boolean skipNulls) throws IOException {
        Workbook workbook = WorkbookFactory.create(stream);
        Sheet sheet = workbook.getSheet(selection.sheet);
        if (sheet==null) throw new IllegalStateException("Sheet " + selection.sheet + " not found");
        selection.updateVertical(sheet.getFirstRowNum(),sheet.getLastRowNum());
        Row firstRow = sheet.getRow(selection.top);
        selection.updateHorizontal(firstRow.getFirstCellNum(), firstRow.getLastCellNum());

        String[] header = getHeader(hasHeader, skipNulls, firstRow, selection, ignore, mappings);
        boolean checkIgnore = !ignore.isEmpty() || mappings.values().stream().anyMatch(m -> m.ignore);
        XLSSpliterator xlsSpliterator = new XLSSpliterator(sheet, selection, header, url, skip, limit, checkIgnore, mappings, nullValues, skipNulls);
        return xlsSpliterator;
    }

    private static String[] getHeader(boolean hasHeader, boolean skipNulls, Row header, LoadXls.Selection selection, List<String> ignore, Map<String, LoadXls.Mapping> mapping) throws IOException {
        if (!hasHeader) return null;

        String[] result = new String[selection.right - selection.left];
        for (int i = selection.left; i < selection.right; i++) {
            Cell cell = header.getCell(i);
            String value = getHeaderValue(skipNulls, i, cell);
            result[i- selection.left] = ignore.contains(value) || mapping.getOrDefault(value, LoadXls.Mapping.EMPTY).ignore ? null : value;
        }
        return result;
    }

    private static String getHeaderValue(boolean skipNulls, int i, Cell cell) {
        boolean cellBlank = cell == null || cell.getStringCellValue().isBlank();
        if (cellBlank && skipNulls) {
            return "Empty__" + i;
        }

        if (cell != null) {
            return cell.getStringCellValue();
        }

        throw new IllegalStateException("Header at position " + i + " doesn't have a value");
    }

    private static Object[] extract(Row row, LoadXls.Selection selection) {
        // selection.updateHorizontal(row.getFirstCellNum(),row.getLastCellNum());
        Object[] result = new Object[selection.right-selection.left];
        for (int i = selection.left; i < selection.right; i++) {
            Cell cell = row.getCell(i);
            if (cell == null) continue;
            result[i-selection.left] = getValue(cell, cell.getCellType());
        }
        return result;

    }
    
    private static Object getValue(Cell cell, CellType type) {
        switch (type) {
            case NUMERIC: // In excel the date is NUMERIC Type
                if (DateUtil.isCellDateFormatted(cell)) {
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(cell.getDateCellValue());
                    LocalDateTime localDateTime = LocalDateTime.of(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DAY_OF_MONTH),
                            cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND));
                    return LocalDateTimeValue.localDateTime(localDateTime);
//                    return LocalDateTimeValue.localDateTime(cell.getDateCellValue().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime());
                }
                double value = cell.getNumericCellValue();
                if (value == Math.floor(value)) return (long) value;
                return value;
            case STRING: return cell.getStringCellValue();
            case FORMULA: return getValue(cell,cell.getCachedFormulaResultType());
            case BOOLEAN: return cell.getBooleanCellValue();
            case _NONE: return null;
            case BLANK: return null;
            case ERROR: return null;
            default: return null;
        }
    }
}
