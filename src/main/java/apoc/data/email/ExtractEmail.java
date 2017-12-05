package apoc.data.email;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import javax.mail.internet.AddressException;

// Validates RFC822 syntax
import javax.mail.internet.InternetAddress;

public class ExtractEmail {
    protected InternetAddress addressOrNull(String value) {
        if (value == null || value.indexOf('@') == -1) {
            return null;
        }
        try {
            return new InternetAddress(value);
        } catch(AddressException adr) {
            return null;
        }
    }

    @UserFunction
    @Description("apoc.data.email.personal('email_address') YIELD personal - extract the personal name from an email address like 'David <david.allen@neo4j.com>'")
    public String personal(final @Name("email_address") String value) {
        InternetAddress addr = addressOrNull(value);
        return addr != null ? addr.getPersonal() : null;
    }

    @UserFunction
    @Description("apoc.data.email.domain('email_address') YIELD domain - extract the domain name from an email address. If nothing was found, yield null.")
    public String domain(final @Name("email_address") String value) {
        InternetAddress addr = addressOrNull(value);

        if (addr == null) return null;

        // This naive parsing is only safe because of going through previous
        // validation.
        String rawAddr = addr.getAddress();
        return rawAddr.substring(rawAddr.indexOf('@') + 1);
    }

    @UserFunction
    @Description("apoc.data.email.user('email_address') YIELD user - extract the user from an email address")
    public String user(final @Name("email_address") String value) {
        InternetAddress addr = addressOrNull(value);
        if (addr == null) return null;

        String rawAddr = addr.getAddress();
        return rawAddr.substring(0, rawAddr.indexOf('@'));
    }
}
