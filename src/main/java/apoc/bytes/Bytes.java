package apoc.bytes;

import apoc.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.regex.Pattern;

import static java.lang.Integer.parseInt;
import static java.lang.Math.min;

public class Bytes {

    public static final Pattern HEXSTRING = Pattern.compile("^[0-9A-F]+$", Pattern.CASE_INSENSITIVE);

    @UserFunction("apoc.bytes.fromHexString")
    @Description("turns hex string into array of bytes, invalid values will result in null")
    public byte[] fromHexString(@Name("text") String text) {
        if (text == null || text.trim().isEmpty()) return null;

        if (!HEXSTRING.matcher(text).matches()) return null;

        int textLen = text.length();
        int bytesCount = textLen / 2 + textLen % 2;

        byte[] result = new byte[bytesCount];
        for (int pos = 0; pos < bytesCount; pos++) {
            int index = pos * 2;
            result[pos] = (byte) parseInt(text.substring(index, min(index + 2, textLen)), 16);
        }
        return result;
    }
}
