package apoc.custom;

import apoc.Extended;
import apoc.util.SystemDbUtil;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.stream.Stream;

import static apoc.custom.CypherProceduresHandler.PREFIX;
import static apoc.util.SystemDbUtil.checkInSystemDb;


@Extended
public class CypherNewProcedures {

    @Context
    public GraphDatabaseAPI db;

    @Context
    public Log log;

    @Context
    public Transaction tx;

    private void checkIsValidDatabase(String databaseName) {
        SystemDbUtil.checkInSystemLeader(db);

        SystemDbUtil.checkTargetDatabase(tx, databaseName, "Custom procedures/functions");
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(value = "apoc.custom.installProcedure", mode = Mode.WRITE)
    @Description("Eventually registers a custom cypher procedure")
    public void installProcedure(@Name("signature") String signature,
                                 @Name("statement") String statement,
                                 @Name(value = "databaseName", defaultValue = "neo4j") String databaseName,
                                 @Name(value = "mode", defaultValue = "read") String mode,
                                 @Name(value = "description", defaultValue = "") String description) {
        checkIsValidDatabase(databaseName);

        Mode modeProcedure = CypherProceduresUtil.mode(mode);
        ProcedureSignature procedureSignature = new Signatures(PREFIX).asProcedureSignature(signature, description, modeProcedure);

        CypherHandlerNewProcedure.installProcedure(databaseName, procedureSignature, statement);
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(value = "apoc.custom.installFunction", mode = Mode.WRITE)
    @Description("Eventually registers a custom cypher function")
    public void installFunction(@Name("signature") String signature, @Name("statement") String statement,
                                @Name(value = "databaseName", defaultValue = "neo4j") String databaseName,
                                @Name(value = "forceSingle", defaultValue = "false") boolean forceSingle,
                                @Name(value = "description", defaultValue = "") String description) {
        checkIsValidDatabase(databaseName);

        UserFunctionSignature userFunctionSignature = new Signatures(PREFIX).asFunctionSignature(signature, description);
        CypherHandlerNewProcedure.installFunction(databaseName, userFunctionSignature, statement, forceSingle);
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(value = "apoc.custom.dropProcedure", mode = Mode.WRITE)
    @Description("Eventually drops the targeted custom procedure")
    public void dropProcedure(@Name("name") String name, @Name(value = "databaseName", defaultValue = "neo4j") String databaseName) {
        checkIsValidDatabase(databaseName);

        CypherHandlerNewProcedure.dropProcedure(databaseName, name);
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(value = "apoc.custom.dropFunction", mode = Mode.WRITE)
    @Description("Eventually drops the targeted custom function")
    public void dropFunction(@Name("name") String name, @Name(value = "databaseName", defaultValue = "neo4j") String databaseName) {
        checkIsValidDatabase(databaseName);

        CypherHandlerNewProcedure.dropFunction(databaseName, name);
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(value = "apoc.custom.dropAll", mode = Mode.WRITE)
    @Description("Eventually drops all previously added custom procedures/functions and returns info")
    public Stream<CustomProcedureInfo> dropAll(@Name(value = "databaseName", defaultValue = "neo4j") String databaseName) {
        checkIsValidDatabase(databaseName);

        return CypherHandlerNewProcedure.dropAll(databaseName)
                .stream();
    }

    // not to change with @SystemOnlyProcedure because this procedure can be executed in user dbs as well
    // since is a read-only operation
    @SystemProcedure
    @Admin
    @Procedure(value = "apoc.custom.show", mode = Mode.READ)
    @Description("Provides a list of custom procedures/function registered")
    public Stream<CustomProcedureInfo> show(@Name(value = "databaseName", defaultValue = "neo4j") String databaseName) {
        checkInSystemDb(db);
        return CypherHandlerNewProcedure.show(databaseName, tx);
    }
}

