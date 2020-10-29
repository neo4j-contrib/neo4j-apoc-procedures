package apoc.load;

import apoc.Extended;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.file.*;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ApocConfig.apocConfig;

@Extended
public class LoadFolder {

    @Context
    public Log log;

    public static class UrlResult {
        public final String url;

        public UrlResult(String url) {
            this.url = url;
        }
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
