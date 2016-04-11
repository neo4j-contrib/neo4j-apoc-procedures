package apoc.js;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;
import apoc.Description;
import apoc.result.ObjectResult;
import jdk.nashorn.api.scripting.NashornScriptEngine;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;


/**
 * @author tkroman
 * @since 11.04.2016
 */
public class Eval {
	private static final NashornScriptEngineFactory FACTORY = new NashornScriptEngineFactory();
	private static final NashornScriptEngine JS = (NashornScriptEngine) FACTORY.getScriptEngine("nashorn");

	@Context public GraphDatabaseService db;

	@Procedure
	@Description("eval(1 + 41) => 42")
	public Stream<ObjectResult> eval(final @Name("expr") String jsExpr) {
		return evalWithParams(jsExpr, Collections.emptyMap());
	}

	@Procedure
	@Description("evalWithParams(n.firstName + ' ' + n.lastName, { n: someNode }) => 'Jack Sparrow'")
	public Stream<ObjectResult> evalWithParams(final @Name("expr") String jsExpr, final @Name("params") Map<String, Object> params) {
		try {
			Object eval = JS.eval(jsExpr, freshLocalContext(params));
			return Stream.of(new ObjectResult(eval));
		} catch (ScriptException e) {
			throw new RuntimeException("Failed to evaluate " + jsExpr, e);
		}
	}

	@Procedure
	@Description("compile('function id(x) { return x; };')")
	public Stream<ObjectResult> compile(final @Name("function") String definition) {
		try {
			Object compiled = JS.compile(definition).eval(JS.getBindings(ScriptContext.GLOBAL_SCOPE));
			return Stream.of(new ObjectResult(compiled.toString()));
		} catch (ScriptException e) {
			throw new RuntimeException("Failed to evaluate " + definition, e);
		}
	}

	private ScriptContext freshLocalContext(final Map<String, Object> params) {
		final ScriptContext localCtx = new SimpleScriptContext();
		final Bindings engineScopeBindings = JS.createBindings();
		final Bindings globalScopeBindings = JS.createBindings();
		final Bindings presentEngineBindings = JS.getContext().getBindings(ScriptContext.GLOBAL_SCOPE);
		final Bindings presentGlobalBindings = JS.getContext().getBindings(ScriptContext.ENGINE_SCOPE);
		if (presentEngineBindings != null) {
			engineScopeBindings.putAll(presentEngineBindings);
		}
		if (presentGlobalBindings != null) {
			globalScopeBindings.putAll(presentGlobalBindings);
		}
		engineScopeBindings.putAll(params);
		localCtx.setBindings(globalScopeBindings, ScriptContext.GLOBAL_SCOPE);
		localCtx.setBindings(engineScopeBindings, ScriptContext.ENGINE_SCOPE);
		return localCtx;
	}
}
