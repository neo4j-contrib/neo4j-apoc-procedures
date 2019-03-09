package apoc.export.cypher;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

public interface ExportFileManager {
    PrintWriter getPrintWriter(String type) throws IOException;

    StringWriter getStringWriter(String type);

    String drain(String type);

    String getFileName();
}
