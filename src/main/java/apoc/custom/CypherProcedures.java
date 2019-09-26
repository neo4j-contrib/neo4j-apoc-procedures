package apoc.custom;

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.List;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 18.08.18
 */
public class CypherProcedures {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public KernelTransaction ktx;

    @Context
    public Log log;

    @Context
    public CypherProceduresHandler cypherProceduresHandler;

    /*
     * store in graph properties, load at startup
     * allow to register proper params as procedure-params
     * allow to register proper return columns
     * allow to register mode
     */
    @Procedure(value = "apoc.custom.asProcedure",mode = Mode.WRITE)
    @Description("apoc.custom.asProcedure(name, statement, mode, outputs, inputs, description) - register a custom cypher procedure")
    public void asProcedure(@Name("name") String name, @Name("statement") String statement,
                            @Name(value = "mode",defaultValue = "read") String mode,
                            @Name(value= "outputs", defaultValue = "null") List<List<String>> outputs,
                            @Name(value= "inputs", defaultValue = "null") List<List<String>> inputs,
                            @Name(value= "description", defaultValue = "null") String description
    ) throws ProcedureException {
//        debug(name,"before", ktx);
        cypherProceduresHandler.storeProcedure(name, statement, mode, outputs, inputs, description);
//        debug(name, "after", ktx);
    }

    /*public static void debug(@Name("name") String name, String msg, KernelTransaction ktx) {
        try {
            org.neo4j.internal.kernel.api.Procedures procedures = ktx.procedures();
            // ProcedureHandle procedureHandle = procedures.procedureGet(CustomStatementRegistry.qualifiedName(name));
            // if (procedureHandle != null) System.out.printf("%s name: %s id %d%n", msg, procedureHandle.signature().name().toString(), procedureHandle.id());
        } catch (Exception e) {
        }
    }*/

    @Procedure(value = "apoc.custom.asFunction",mode = Mode.WRITE)
    @Description("apoc.custom.asFunction(name, statement, outputs, inputs, forceSingle, description) - register a custom cypher function")
    public void asFunction(@Name("name") String name, @Name("statement") String statement,
                           @Name(value= "outputs", defaultValue = "") String output,
                           @Name(value= "inputs", defaultValue = "null") List<List<String>> inputs,
                           @Name(value = "forceSingle", defaultValue = "false") boolean forceSingle,
                           @Name(value = "description", defaultValue = "null") String description) throws ProcedureException {
        cypherProceduresHandler.storeFunction(name, statement, output, inputs, forceSingle, description);
    }

    @Procedure(value = "apoc.custom.list", mode = Mode.READ)
    @Description("apoc.custom.list() - provide a list of custom procedures/function registered")
    public Stream<CustomProcedureInfo> list(){

        return cypherProceduresHandler.list().map(m -> new CustomProcedureInfo(
                (String) m.get("type"),
                (String) m.get("name"),
                (String) m.get("description"),
                (String) m.get("mode"),
                (String) m.get("statement"),
                (List<List<String>>) m.get("inputs"),
                m.get("output"),
                (Boolean) m.get("forceSingle")
        ));
    }

    public static class CustomProcedureInfo {
        public String type;
        public String name;
        public String description;
        public String mode;
        public String statement;
        public List<List<String>>inputs;
        public Object outputs;
        public Boolean forceSingle;

        public CustomProcedureInfo(String type, String name, String description, String mode,
                                   String statement, List<List<String>> inputs, Object outputs,
                                   Boolean forceSingle){
            this.type = type;
            this.name = name;
            this.description = description;
            this.statement = statement;
            this.outputs = outputs;
            this.inputs = inputs;
            this.forceSingle = forceSingle;
            this.mode = mode;
        }
    }

}
