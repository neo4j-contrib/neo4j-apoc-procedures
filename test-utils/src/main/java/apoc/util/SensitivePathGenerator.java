package apoc.util;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SensitivePathGenerator {

    private SensitivePathGenerator() {}

    /**
     * It will return an instance of Pair<String, String> where first is the relative path
     * and other the absolute path of "etc/passwd"
     * @param db
     * @return
     */
    public static Pair<String, String> etcPasswd(GraphDatabaseAPI db) {
        return base(db, "/etc/passwd");
    }

    private static Pair<String, String> base(GraphDatabaseAPI db, String path) {
        final Path dbPath = db.databaseLayout().databaseDirectory();
        final String relativeFileName = IntStream.range(0, dbPath.getNameCount())
                .mapToObj(i -> "..")
                .collect(Collectors.joining("/")) + path;
        final String absoluteFileName = Paths.get(relativeFileName)
                .toAbsolutePath().normalize().toString();
        return Pair.of(relativeFileName, absoluteFileName);
    }
}
