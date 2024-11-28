package apoc.dv;

import apoc.ApocConfig;
import apoc.Extended;
import apoc.result.NodeResult;
import apoc.result.PathResult;
import apoc.result.VirtualPath;
import apoc.result.VirtualRelationship;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;


// TODO - creare una classe DataVirtualizationCatalogNewProcedures con i cambiamenti sottostanti
@Extended
public class DataVirtualizationCatalog {

    @Context
    public Transaction tx;

    @Context
    public Log log;

    // todo - questo cambiarlo da GraphDatabaseService a GraphDatabaseAPI nella nuova classe
    @Context
    public GraphDatabaseService db;

    @Context
    public ApocConfig apocConfig;

    // TODO: mettere questo nella nuova classe
    /*
        private void checkIsValidDatabase(String databaseName) {
        SystemDbUtil.checkInSystemLeader(db);
        SystemDbUtil.checkTargetDatabase(tx, databaseName, "Data virtualization catalog");
    }
     */
    
    
    // TODO - nella nuova classe fare una procedura simile a questa, con queste annotation
    /*
        @SystemProcedure
        @Admin
        @Procedure(name = "apoc.dv.catalog.install", mode = Mode.WRITE)
        @Description("Eventually adds a virtualized resource configuration")
     */
    // todo - mentre questa deprecarla con @Deprecated e deprecatedBy
    @Procedure(name = "apoc.dv.catalog.add", mode = Mode.WRITE, deprecatedBy = "apoc.dv.catalog.install")
    @Description("Add a virtualized resource configuration")
    public Stream<VirtualizedResource.VirtualizedResourceDTO> add(
            // todo - mettere @Name(value = "databaseName", defaultValue = "neo4j") String databaseName, come primo parametro
            @Name("name") String name,
            @Name(value = "config", defaultValue = "{}") Map<String,Object> config) {
        // TODO - mettere metodo checkIsValidDatabase(db.databaseName()) solo nella nuova procedura, all'inizio
        
        // TODO cambiare DataVirtualizationCatalogHandler in DataVirtualizationCatalogHandlerNewProcedures
        return Stream.of(new DataVirtualizationCatalogHandler(db, apocConfig.getSystemDb(), log)
                        // TODO - rinominare il metodo da .add( a .install( 
                        .add(VirtualizedResource.from(name, config)))
                .map(VirtualizedResource::toDTO);
    }

    // TODO - come sopra, chiamarla @Procedure(name = "apoc.dv.catalog.drop", mode = Mode.WRITE)
    @Procedure(name = "apoc.dv.catalog.remove", mode = Mode.WRITE)
    @Description("Remove a virtualized resource config by name")
    public Stream<VirtualizedResource.VirtualizedResourceDTO> remove(
            // todo - mettere @Name(value = "databaseName", defaultValue = "neo4j") String databaseName, come primo parametro
            @Name("name") String name
    ) {
        // TODO - checkIsValidDatabase
        return new DataVirtualizationCatalogHandler(db, apocConfig.getSystemDb(), log)
                // TODO - rinominare il metodo da .remove( a .drop( 
                .remove(name)
                .map(VirtualizedResource::toDTO);
    }

    // TODO - come sopra, chiamarla @Procedure(name = "apoc.dv.catalog.show", mode = Mode.WRITE)
    @Procedure(name = "apoc.dv.catalog.list", mode = Mode.READ)
    @Description("List all virtualized resource configuration")
    public Stream<VirtualizedResource.VirtualizedResourceDTO> list(
            // todo - mettere @Name(value = "databaseName", defaultValue = "neo4j") String databaseName, come primo parametro
    ) {
        // TODO - checkIsValidDatabase
        return new DataVirtualizationCatalogHandler(db, apocConfig.getSystemDb(), log).list()
                .map(VirtualizedResource::toDTO);
    }

    // TODO - come sopra, chiamarla @Procedure(name = "apoc.dv.catalog.getQuery", mode = Mode.WRITE)
    @Procedure(name = "apoc.dv.query", mode = Mode.READ)
    @Description("Query a virtualized resource by name and return virtual nodes")
    public Stream<NodeResult> query(// todo - mettere @Name(value = "databaseName", defaultValue = "neo4j") String databaseName, come primo parametro
            @Name("name") String name,
                                    @Name(value = "params", defaultValue = "{}") Object params,
                                    @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        // TODO - checkIsValidDatabase

        // TODO - rinominare get in query
        VirtualizedResource vr = new DataVirtualizationCatalogHandler(db, apocConfig.getSystemDb(), log).get(name);
        final Pair<String, Map<String, Object>> procedureCallWithParams = vr.getProcedureCallWithParams(params, config);
        return tx.execute(procedureCallWithParams.getLeft(), procedureCallWithParams.getRight())
                .stream()
                .map(m -> (Node) m.get(("node")))
                .map(NodeResult::new);
    }

    // TODO - come sopra, chiamarla @Procedure(name = "apoc.dv.catalog.getQueryAndLink", mode = Mode.WRITE)
    @Procedure(name = "apoc.dv.queryAndLink", mode = Mode.READ)
    @Description("Query a virtualized resource by name and return virtual nodes linked using virtual rels to the node passed as first param")
    public Stream<PathResult> queryAndLink(
            // todo - mettere @Name(value = "databaseName", defaultValue = "neo4j") String databaseName, come primo parametro
            @Name("node") Node node,
                                           @Name("relName") String relName,
                                           @Name("name") String name,
                                           @Name(value = "params", defaultValue = "{}") Object params,
                                           @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        // TODO - checkIsValidDatabase

        // TODO - rinominare get in query
        VirtualizedResource vr = new DataVirtualizationCatalogHandler(db, apocConfig.getSystemDb(), null).get(name);
        final RelationshipType relationshipType = RelationshipType.withName(relName);
        final Pair<String, Map<String, Object>> procedureCallWithParams = vr.getProcedureCallWithParams(params, config);
        return tx.execute(procedureCallWithParams.getLeft(), procedureCallWithParams.getRight())
                .stream()
                .map(m -> (Node) m.get(("node")))
                .map(n -> new VirtualRelationship(node, n, relationshipType))
                .map(r -> {
                    VirtualPath virtualPath =  new VirtualPath(r.getStartNode());
                    virtualPath.addRel(r);
                    return virtualPath;
                })
                .map(PathResult::new);
    }
}
