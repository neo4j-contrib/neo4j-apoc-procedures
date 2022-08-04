package apoc.bitwise;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

/**
 * @author kv
 * @since 6.05.16
 */
public class BitwiseOperations {

    @UserFunction("apoc.bitwise.op")
    @Description("apoc.bitwise.op(60,'|',13) bitwise operations a & b, a | b, a ^ b, ~a, a >> b, a >>> b, a << b. returns the result of the bitwise operation")
    public Long op(@Name("a") final Long a, @Name("operator") final String operator, @Name("b") final Long b) throws Exception {
        if (a == null || operator == null || operator.isEmpty()) {
            return null;
        }
        if (!operator.equals("~") && b == null) {
            return null;
        }
        switch (operator.toLowerCase()) {
            case "&":
            case "and":
                return a & b;
            case "|":
            case "or":
                return a | b;
            case "^":
            case "xor":
                return a ^ b;
            case "~":
            case "not":
                return ~a;
            case ">>":
            case "right shift":
                return a >> b;
            case ">>>":
            case "right shift unsigned":
                return a >>> b;
            case "<<":
            case "left shift":
                return a << b;
            default:
                throw new Exception("Invalid bitwise operator : '" + operator + "'");
        }
    }
}
