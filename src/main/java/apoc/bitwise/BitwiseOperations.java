package apoc.bitwise;

import java.util.stream.Stream;

import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import apoc.Description;
import apoc.result.LongResult;

/**
 * @author kv
 * @since 6.05.16
 */
public class BitwiseOperations {

	@Procedure("apoc.bitwise.cmp")
	@Description("apoc.bitwise.cmp(60,'&';,13) bitwise operations a & b, a | b, a ^ b, ~a, a >> b, a >>> b, a << b. returns the result of the bitwise operation" )
	public Stream<LongResult> cmp(@Name("a") final Long a, @Name("operator") final String operator, @Name("b") final Long b) throws Exception {
		if (a == null || operator == null || operator.isEmpty()) {
			return Stream.of(LongResult.NULL);
		}
		if (!operator.equals("~") && b == null) {
			return Stream.of(LongResult.NULL);
		}
		long r = -1;
		switch(operator) {
			case "&":  
				r = a & b;
			    break;
			case "|":
				r = a | b;
			    break;
			case "^":
				r = a ^ b;
			    break;
			case "~":
				r = ~a;
			    break;
			case ">>":
				r = a >> b;
			    break;
			case ">>>":
				r = a >>> b;
			    break;
			case "<<":
				r = a << b;
			    break;
			default:
				throw new Exception("Invalid bitwise operator :" + operator + "'");
		}
		return Stream.of(new LongResult(r));
	}

}
