package apoc.bitwise;

import java.util.stream.Stream;

import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import org.neo4j.procedure.Description;
import apoc.result.LongResult;

/**
 * @author kv
 * @since 6.05.16
 */
public class BitwiseOperations {

	@Procedure("apoc.bitwise.op")
	@Description("apoc.bitwise.op(60,'|',13) bitwise operations a & b, a | b, a ^ b, ~a, a >> b, a >>> b, a << b. returns the result of the bitwise operation" )
	public Stream<LongResult> op(@Name("a") final Long a, @Name("operator") final String operator, @Name("b") final Long b) throws Exception {
		if (a == null || operator == null || operator.isEmpty()) {
			return Stream.of(LongResult.NULL);
		}
		if (!operator.equals("~") && b == null) {
			return Stream.of(LongResult.NULL);
		}
		long r = -1;
		switch(operator.toLowerCase()) {
			case "&":  
			case "and":
				r = a & b;
			    break;
			case "|":
			case "or":
				r = a | b;
			    break;
			case "^":
			case "xor":
				r = a ^ b;
			    break;
			case "~":
			case "not":
				r = ~a;
			    break;
			case ">>":
			case "right shift":
				r = a >> b;
			    break;
			case ">>>":
			case "right shift unsigned":
				r = a >>> b;
			    break;
			case "<<":
			case "left shift":
				r = a << b;
			    break;
			default:
				throw new Exception("Invalid bitwise operator : '" + operator + "'");
		}
		return Stream.of(new LongResult(r));
	}

}
