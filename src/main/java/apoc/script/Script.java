package apoc.script;

import apoc.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

import javax.naming.Binding;
import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Victor Borja vborja@apache.org
 * @since 12.06.16
 */
public class Script {

    final static ScriptEngineManager scriptEngineManager = new ScriptEngineManager();

    @Procedure
    @Description("apoc.script.engines() YIELD name, version, languageName, languageVersion, names - List available scripting engines.")
    public Stream<EnginesResult> engines() {
        return scriptEngineManager.getEngineFactories()
                .stream()
                .map(EnginesResult::new);
    }

    @UserFunction
    @Description("apoc.script.eval(engineName, code, params) -- Evaluate a script on an engine")
    public Object eval(@Name("engineName") String engineName, @Name("code") String code, @Name("params") Map<String, Object> params) throws ScriptException {
        ScriptEngine engine = getEngine(engineName);
        Bindings bindings = createBindings(engine, params);
        return engine.eval(code, bindings);
    }

    @UserFunction
    @Description("apoc.script.evalToBindings(engineName, code, params, bindings) -- Evaluate a script on an engine")
    public Map<String, Object> evalToBindings(@Name("engineName") String engineName, @Name("code") String code, @Name("params") Map<String, Object> params, @Name("bindings") List<String> resultNames) throws ScriptException {
        ScriptEngine engine = getEngine(engineName);
        Bindings bindings = createBindings(engine, params);
        engine.eval(code, bindings);
        Map<String, Object> result = new HashMap();
        for (String resultName : resultNames) {
            result.put(resultName, bindings.get(resultName));
        }
        return result;
    }

    private ScriptEngine getEngine(String engineName) {
        ScriptEngine engine = scriptEngineManager.getEngineByName(engineName);
        if (engine == null) {
            throw new RuntimeException("No script engine with name "+engineName+" available");
        }
        return engine;
    }

    private Bindings createBindings(ScriptEngine engine, Map<String, Object> params) {
        Bindings bindings = engine.createBindings();
        bindings.putAll(params);
        return bindings;
    }

}
