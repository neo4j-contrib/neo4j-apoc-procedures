package apoc.export.csv;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class CsvTestUtil {

    public static final CsvMapper CSV_MAPPER;

    static {
        CSV_MAPPER = new CsvMapper();
        CSV_MAPPER.enable(CsvParser.Feature.WRAP_AS_ARRAY);
    }

    public static void saveCsvFile(String fileName, String content) throws IOException {
        Files.write(Paths.get("src/test/resources/csv-inputs/" + fileName + ".csv"), content.getBytes());
    }

    public static List<String[]> toCollection(String csv) {

        try {
            MappingIterator<String[]> it = CSV_MAPPER.readerFor(String[].class)
//                    .with(CsvSchema.emptySchema().withHeader())
                    .<String[]>readValues(csv.getBytes());
            return it.readAll();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
