package apoc.load;

import apoc.Extended;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.file.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ApocConfig.apocConfig;

@Extended
public class LoadFolder {

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    public static final HashMap<String, FolderListener> folderListenerList = new HashMap<>();


    public static class UrlResult {
        public final String url;

        public UrlResult(String url) {
            this.url = url;
        }
    }

    public static class FolderListenerResult {
        public final String name;
        public final String cypher;
        public final String pattern;

        public FolderListenerResult(String name, String cypher, String pattern) {
            this.name = name;
            this.cypher = cypher;
            this.pattern = pattern;
        }
    }


    @Procedure(name = "apoc.load.folder.add", mode = Mode.WRITE)
    @Description("apoc.load.folder.add(name, pattern, cypher, {}) YIELD name, pattern, cypher - Add or replace a WatchListener with a specific name, pattern and cypher")
    public Stream<FolderListenerResult> add(@Name("name") String name, @Name("pattern") String pattern, @Name("cypher") String cypher, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        FolderListenerResult folderListenerResult = new FolderListenerResult(name, cypher, pattern);

        // replace eventual listener with same name
        FolderListener folderListenerFromMap = folderListenerList.get(name);
        if (folderListenerFromMap != null) {
            folderListenerFromMap.stop();
        }

        FolderListener folderListener = new FolderListener((kind, file) -> {
            try (Transaction tx = db.beginTx()) {
                tx.execute(cypher);
                log.info("Executing query - " + cypher);
                tx.commit();
            } catch (Exception e) {
                log.info("Error while executing query - " + cypher);
                throw new RuntimeException(e);
            }
            return null;
        }, log, folderListenerResult);
        folderListener.start();
        folderListenerList.put(name, folderListener);

        return Stream.of(folderListenerResult);
    }


    @Procedure("apoc.load.folder.remove")
    @Description("apoc.load.folder.remove(name) YIELD name, pattern, cypher - Remove a WatchListener by name")
    public Stream<FolderListenerResult> remove(@Name("name") String name) {

        FolderListener folderListener = folderListenerList.remove(name);
        if (folderListener == null) {
            throw new RuntimeException("Listener with name: " + name + " doesn't exists");
        }

        folderListener.stop();
        log.info("Stopped listener with name: " + name);
        return Stream.of(folderListener.getFolderListenerResult());
    }


    @Procedure("apoc.load.folder.list")
    @Description("apoc.load.folder.list() YIELD name, pattern, cypher - List of all folder listener")
    public Stream<FolderListenerResult> list() {
        return folderListenerList.values().stream().map(FolderListener::getFolderListenerResult);
    }


    @Procedure("apoc.load.folder")
    @Description("apoc.load.folder('pattern',{config}) YIELD url - List of all files in import folder")
    public Stream<UrlResult> folder(@Name(value="pattern", defaultValue = "*") String pattern, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        log.info("Search files that match regular expression: " + pattern);

        File directory = Paths.get(apocConfig().getString("dbms.directories.import")).toFile();
        boolean isRecursive = (boolean) config.getOrDefault("recursive", true);

        IOFileFilter filter = new WildcardFileFilter(pattern);
        Collection<File> files = FileUtils.listFiles(directory, filter, isRecursive ? TrueFileFilter.TRUE : null);

        return files.stream().map(i -> new UrlResult(i.toString()));
    }

}
