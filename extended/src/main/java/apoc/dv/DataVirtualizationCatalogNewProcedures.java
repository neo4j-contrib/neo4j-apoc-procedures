package apoc.dv;

import apoc.util.SystemDbUtil;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

public class DataVirtualizationCatalogNewProcedures {
    @Context
    public Transaction tx;

    @Context
    public Log log;

    @Context
    public GraphDatabaseAPI db;

    private void checkIsValidDatabase(String databaseName) {
        SystemDbUtil.checkInSystemLeader(db);
        SystemDbUtil.checkTargetDatabase(tx, databaseName, "Data virtualization catalog");
    }

    @SystemProcedure
    @Admin
    @Procedure(name = "apoc.dv.catalog.install", mode = Mode.WRITE)
    @Description("Eventually adds a virtualized resource configuration")
    public Stream<VirtualizedResource.VirtualizedResourceDTO> install(
            @Name("name") String name,
            @Name(value = "databaseName", defaultValue = "neo4j") String databaseName,
            @Name(value = "config", defaultValue = "{}") Map<String,Object> config) {
        checkIsValidDatabase(databaseName);

        return Stream.of(new DataVirtualizationCatalogHandlerNewProcedures()
                        .install(databaseName, VirtualizedResource.from(name, config)))
                .map(VirtualizedResource::toDTO);
    }

    @SystemProcedure
    @Admin
    @Procedure(name = "apoc.dv.catalog.drop", mode = Mode.WRITE)
    @Description("Remove a virtualized resource config by name")
    public Stream<VirtualizedResource.VirtualizedResourceDTO> drop(
            @Name("name") String name,
            @Name(value = "databaseName", defaultValue = "neo4j") String databaseName
    ) {
        checkIsValidDatabase(databaseName);
        return new DataVirtualizationCatalogHandlerNewProcedures()
                .drop(databaseName, name)
                .map(VirtualizedResource::toDTO);
    }

    @SystemProcedure
    @Procedure(name = "apoc.dv.catalog.show", mode = Mode.READ)
    @Description("List all virtualized resource configuration")
    public Stream<VirtualizedResource.VirtualizedResourceDTO> show(
            @Name(value = "databaseName", defaultValue = "neo4j") String databaseName
    ) {
        checkIsValidDatabase(databaseName);
        return new DataVirtualizationCatalogHandlerNewProcedures()
                .show(databaseName)
                .map(VirtualizedResource::toDTO);
    }

}
