package apoc.export.cypher;

import java.io.PrintWriter;
import java.io.StringWriter;

public interface ExportFileManager {
    PrintWriter getPrintWriter(String type);

    StringWriter getStringWriter(String type);

    String drain(String type);

    String getFileName();

    Boolean separatedFiles();
}
