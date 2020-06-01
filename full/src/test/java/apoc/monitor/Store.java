package apoc.monitor;

import apoc.result.StoreInfoResult;
import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Procedure;

import java.io.File;
import java.util.stream.Stream;

public class Store  {

    @Context
    public GraphDatabaseService db;

    @Procedure
    @Description("apoc.monitor.store() returns informations about the sizes of the different parts of the neo4j graph store")
    public Stream<StoreInfoResult> store() {

        Database database = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(Database.class);
        DatabaseLayout databaseLayout = database.getDatabaseLayout();
        return Stream.of(new StoreInfoResult(
                getDirectorySize(databaseLayout.getTransactionLogsDirectory()),
                databaseLayout.propertyStringStore().length(),
                databaseLayout.propertyArrayStore().length(),
                databaseLayout.relationshipStore().length(),
                databaseLayout.propertyStore().length(),
                getDirectorySize(databaseLayout.databaseDirectory()), //databaseLayout.storeFiles().stream().mapToLong(File::length).sum(),
                databaseLayout.nodeStore().length()
        ));
    }

    private long getDirectorySize(File folder) {
        return folder.exists() ? FileUtils.sizeOfDirectory(folder) : 0;
    }


}
