package apoc.export.util;

import apoc.result.ProgressInfo;
import org.neo4j.cypher.export.SubGraph;

import java.io.Reader;
import java.io.Writer;

/**
 * @author mh
 * @since 17.01.14
 */
public interface Format {
    ProgressInfo load(Reader reader, Reporter reporter, ExportConfig config) throws Exception;
    ProgressInfo dump(SubGraph graph, Writer writer, Reporter reporter, ExportConfig config) throws Exception;
}
