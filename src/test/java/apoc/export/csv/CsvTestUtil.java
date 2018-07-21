package apoc.export.csv;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class CsvTestUtil {

    public static void saveCsvFile(String fileName, String content) throws IOException {
        Files.write(Paths.get("src/test/resources/csv-inputs/" + fileName + ".csv"), content.getBytes());
    }

}
