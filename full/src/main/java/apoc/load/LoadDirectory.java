package apoc.load;

import apoc.Extended;
import apoc.result.StringResult;
import apoc.util.Util;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Collection;
import java.util.stream.Stream;

import static apoc.ApocConfig.APOC_LOAD_DIRECTORY_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.FileUtils.checkIfUrlEmptyAndGetFileUrl;
import static apoc.util.FileUtils.getPathDependingOnUseNeo4jConfig;
import static apoc.util.FileUtils.getPathFromUrlString;

@Extended
public class LoadDirectory {

    public static final String NOT_ENABLED_ERROR = "Load directory listeners have not been enabled." +
            " Set 'apoc.load.directory.enabled=true' in your apoc.conf file located in the $NEO4J_HOME/conf/ directory.";

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    @Context
    public LoadDirectoryHandler loadDirectoryHandler;

    @Context
    public Transaction tx;


    @Procedure(name="apoc.load.directory.async.add", mode = Mode.WRITE)
    @Description("apoc.load.directory.async.add(name, cypher, pattern, urlDir, {}) YIELD name, status, pattern, cypher, urlDir, config, error - Add or replace a folder listener with a specific name, pattern and url directory that execute the specified cypher query when an event is triggered and return listener list")
    public Stream<LoadDirectoryItem.LoadDirectoryResult> add(@Name("name") String name,
                                                             @Name("cypher") String cypher,
                                                             @Name(value = "pattern", defaultValue = "*") String pattern,
                                                             @Name(value = "urlDir", defaultValue = "") String urlDir,
                                                             @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        checkEnabled();

        LoadDirectoryItem.LoadDirectoryConfig conf = new LoadDirectoryItem.LoadDirectoryConfig(config);
        LoadDirectoryItem loadDirectoryItem = new LoadDirectoryItem(name, pattern, cypher, urlDir, conf);

        loadDirectoryHandler.add(loadDirectoryItem);
        return loadDirectoryHandler.list();
    }

    @Procedure("apoc.load.directory.async.remove")
    @Description("apoc.load.directory.async.remove(name) YIELD name, status, pattern, cypher, urlDir, config, error - Remove a folder listener by name and return remaining listener")
    public Stream<LoadDirectoryItem.LoadDirectoryResult> remove(@Name("name") String name) {
        checkEnabled();

        loadDirectoryHandler.remove(name);
        return loadDirectoryHandler.list();
    }

    @Procedure("apoc.load.directory.async.removeAll")
    @Description("apoc.load.directory.async.removeAll() - Remove all folder listeners")
    public Stream<LoadDirectoryItem.LoadDirectoryResult> removeAll() {
        checkEnabled();

        loadDirectoryHandler.removeAll();
        return Stream.empty();
    }

    @Procedure("apoc.load.directory.async.list")
    @Description("apoc.load.directory.async.list() YIELD name, status, pattern, cypher, urlDir, config, error - List of all folder listeners")
    public Stream<LoadDirectoryItem.LoadDirectoryResult> list() {
        checkEnabled();
        return loadDirectoryHandler.list();
    }

    @Procedure
    @Description("apoc.load.directory('pattern', 'urlDir', {config}) YIELD value - Loads list of all files in folder specified by urlDir or in import folder if urlDir string is empty or not specified")
    public Stream<StringResult> directory(@Name(value = "pattern", defaultValue = "*") String pattern, @Name(value = "urlDir", defaultValue = "") String urlDir, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws IOException {

        if (urlDir == null) {
            throw new IllegalArgumentException("Invalid (null) urlDir");
        }

        urlDir = checkIfUrlEmptyAndGetFileUrl(urlDir);

        boolean isRecursive = Util.toBoolean(config.getOrDefault("recursive", true));

        Collection<File> files = org.apache.commons.io.FileUtils.listFiles(
                getPathFromUrlString(urlDir).toFile(),
                new WildcardFileFilter(pattern),
                isRecursive ? TrueFileFilter.TRUE : null
        );

        return files.stream().map(i -> {
            String urlFile = i.toString();
            return new StringResult(getPathDependingOnUseNeo4jConfig(urlFile));
        });
    }


    public void checkEnabled() {
        if (!apocConfig().getBoolean(APOC_LOAD_DIRECTORY_ENABLED)) {
            throw new RuntimeException(NOT_ENABLED_ERROR);
        }
    }
}