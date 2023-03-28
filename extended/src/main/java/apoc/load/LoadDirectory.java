package apoc.load;

import apoc.ApocConfig;
import apoc.Extended;
import apoc.result.StringResult;
import apoc.util.FileUtils;
import apoc.util.Util;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ApocConfig.apocConfig;
import static apoc.load.LoadDirectoryHandler.getPathDependingOnUseNeo4jConfig;
import static apoc.util.ExtendedFileUtils.getPathFromUrlString;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_WRITE;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.WRITE;

@Extended
public class LoadDirectory {

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    @Context
    public LoadDirectoryHandler loadDirectoryHandler;

    @Context
    public Transaction tx;


    @Procedure(name="apoc.load.directory.async.add", mode = Mode.WRITE)
    @Description("apoc.load.directory.async.add(name, cypher, pattern, urlDir, {}) YIELD name, status, pattern, cypher, urlDir, config, error - Adds or replaces a folder listener with a specific name, which is triggered for all files with the given pattern and executes the specified Cypher query when triggered. Returns a list of all listeners. It is possible to specify the event type in the config parameter.")
    public Stream<LoadDirectoryItem.LoadDirectoryResult> add(@Name("name") String name,
                                                             @Name("cypher") String cypher,
                                                             @Name(value = "pattern", defaultValue = "*") String pattern,
                                                             @Name(value = "urlDir", defaultValue = "") String urlDir,
                                                             @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws IOException {
        apocConfig().checkReadAllowed(urlDir);
        Util.validateQuery(db, cypher, READ_WRITE, WRITE);

        LoadDirectoryItem.LoadDirectoryConfig conf = new LoadDirectoryItem.LoadDirectoryConfig(config);
        LoadDirectoryItem loadDirectoryItem = new LoadDirectoryItem(name, pattern, cypher, checkIfUrlBlankAndGetFileUrl(urlDir), conf);

        loadDirectoryHandler.add(loadDirectoryItem);
        return loadDirectoryHandler.list();
    }

    @Procedure("apoc.load.directory.async.remove")
    @Description("apoc.load.directory.async.remove(name) YIELD name, status, pattern, cypher, urlDir, config, error - Removes a folder listener by name and returns all remaining listeners, if any")
    public Stream<LoadDirectoryItem.LoadDirectoryResult> remove(@Name("name") String name) {
        loadDirectoryHandler.remove(name);
        return loadDirectoryHandler.list();
    }

    @Procedure("apoc.load.directory.async.removeAll")
    @Description("apoc.load.directory.async.removeAll() - Removes all folder listeners")
    public Stream<LoadDirectoryItem.LoadDirectoryResult> removeAll() {
        loadDirectoryHandler.removeAll();
        return Stream.empty();
    }

    @Procedure("apoc.load.directory.async.list")
    @Description("apoc.load.directory.async.list() YIELD name, status, pattern, cypher, urlDir, config, error - Lists all folder listeners")
    public Stream<LoadDirectoryItem.LoadDirectoryResult> list() {
        return loadDirectoryHandler.list();
    }

    @Procedure
    @Description("apoc.load.directory('pattern', 'urlDir', {config}) YIELD value - Loads list of all files in the folder specified by the parameter urlDir satisfying the given pattern. If the parameter urlDir is not specified or empty, the files of the import folder are loaded instead.")
    public Stream<StringResult> directory(@Name(value = "pattern", defaultValue = "*") String pattern, @Name(value = "urlDir", defaultValue = "") String urlDir, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws IOException {

        if (urlDir == null) {
            throw new IllegalArgumentException("Invalid (null) urlDir");
        }

        urlDir = checkIfUrlBlankAndGetFileUrl(urlDir);

        boolean isRecursive = Util.toBoolean(config.getOrDefault("recursive", true));

        Collection<File> files = org.apache.commons.io.FileUtils.listFiles(
                getPathFromUrlString(urlDir).toFile(),
                new WildcardFileFilter(pattern),
                isRecursive ? TrueFileFilter.TRUE : FileFileFilter.INSTANCE
        );

        return files.stream().map(i -> {
            String urlFile = i.toString();
            return new StringResult(getPathDependingOnUseNeo4jConfig(urlFile));
        });
    }

    // visible for test purpose
    public static String checkIfUrlBlankAndGetFileUrl(String urlDir) throws IOException {
        if (StringUtils.isBlank(urlDir)) {
            final Path pathImport = Paths.get(ApocConfig.apocConfig().getImportDir()).toAbsolutePath();
            // with replaceAll we remove final "/" from path
            return pathImport.toUri().toString().replaceAll(".$", "");
        }
        return FileUtils.changeFileUrlIfImportDirectoryConstrained(urlDir.replace("?", "%3F"));
    }

}