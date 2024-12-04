package apoc.data.email;

import apoc.util.UtilExtended;

import javax.mail.internet.*;
import java.util.Map;

/**
 * Separated class in order to throw MissingDependencyException if `javax.mail` is not present
 */
public class ExtractEmailHandler {
    public static Map<String,String> extractEmail(String value) {
        if (value == null || value.indexOf('@') == -1) {
            return null;
        }
        try {
            InternetAddress addr = new InternetAddress(value);
            String rawAddr = addr.getAddress();
            int idx = rawAddr.indexOf('@');

            return UtilExtended.map("personal", addr.getPersonal(), "user", rawAddr.substring(0, idx), "domain", rawAddr.substring(idx + 1));
        } catch(AddressException adr) {
            return null;
        }
    }
}
