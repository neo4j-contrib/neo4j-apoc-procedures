package apoc.js;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;
import apoc.Description;
import apoc.result.ObjectResult;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;


/**
 * @author tkroman
 * @since 11.04.2016
 */
public class Eval {
	private static final ScriptEngineManager scripting = new ScriptEngineManager();
	private static final ScriptEngine js = scripting.getEngineByName("nashorn");

	@Procedure
	@Description("eval(1 + 41) => 42")
	public Stream<ObjectResult> eval(final @Name("expr") String jsExpr) {
		return evalWithParams(jsExpr, Collections.emptyMap());
	}

	@Procedure
	@Description("evalWithParams(n.firstName + ' ' + n.lastName, { n: someNode }) => 'Jack Sparrow'")
	public Stream<ObjectResult> evalWithParams(
			final @Name("expr") String jsExpr,
			final @Name("params") Map<String, Object> params) {
		try {
			SimpleScriptContext localCtx = createLocalContext(params);
			Object eval = js.eval(jsExpr, localCtx);
			return Stream.of(new ObjectResult(eval));
		} catch (ScriptException e) {
			throw new RuntimeException("Failed to evaluate " + jsExpr, e);
		}
	}

	private SimpleScriptContext createLocalContext(final @Name("params") Map<String, Object> params) {
		SimpleScriptContext localCtx = new SimpleScriptContext();
		Bindings localBindings = js.createBindings();
		localBindings.putAll(params);
		localCtx.setBindings(localBindings, ScriptContext.ENGINE_SCOPE);
		return localCtx;
	}
}
