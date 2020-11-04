package apoc.load;

import apoc.Extended;
import apoc.util.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;

import static apoc.ApocConfig.apocConfig;

@Extended
public class LoadDirectory {

    @Context
    public Log log;

    public static class UrlResult {
        public final String url;

        public UrlResult(String url) {
            this.url = url;
        }
    }

    @Procedure("apoc.load.directory")
    @Description("apoc.load.directory('pattern', 'urlDir', {config}) YIELD url - List of all files in folder specified by urlDir or in import folder if urlDir is null")
    public Stream<UrlResult> folder(@Name(value = "pattern", defaultValue = "*") String pattern, @Name(value = "urlDir", defaultValue = "") String urlDir, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws IOException, URISyntaxException {
        log.info("Search files that match regular expression: " + pattern);

        urlDir = urlDir == null ?
                apocConfig().getString("dbms.directories.import", "import")
                : FileUtils.changeFileUrlIfImportDirectoryConstrained(urlDir);

        boolean isRecursive = (boolean) config.getOrDefault("recursive", true);

        Collection<File> files = org.apache.commons.io.FileUtils.listFiles(
                Paths.get(URI.create(urlDir).getPath()).toFile(),
                new WildcardFileFilter(pattern),
                isRecursive ? TrueFileFilter.TRUE : null
        );

        return files.stream().map(i -> {
            String urlFile = i.toString();
            return new UrlResult(apocConfig().isImportFolderConfigured()
                    ? urlFile.replace(apocConfig().getString("dbms.directories.import") + "/", "")
                    : urlFile);
        });
    }


}
