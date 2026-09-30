package apoc.util;

import org.junit.Test;

import java.util.List;
import java.util.regex.Pattern;

import static apoc.util.CredentialEndpointGuard.isHttpsHostMatching;
import static apoc.util.CredentialEndpointGuard.isSameOrigin;
import static apoc.util.CredentialEndpointGuard.isTrusted;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CredentialEndpointGuardTest {

    @Test
    public void sameOrigin() {
        assertTrue(isSameOrigin("https://api.openai.com/v1/embeddings", "https://api.openai.com/v1"));
        assertTrue(isSameOrigin("https://API.openai.com:443/other", "https://api.openai.com"));
        assertTrue(isSameOrigin(" http://my_host:8000/v1/x", "http://my_host:8000"));
        assertTrue(isSameOrigin("file:///tmp/embeddings", "file:///var/other/"));
    }

    @Test
    public void differentOrigin() {
        assertFalse(isSameOrigin("https://attacker.com/v1", "https://api.openai.com/v1"));
        assertFalse(isSameOrigin("http://api.openai.com/v1", "https://api.openai.com/v1"));
        assertFalse(isSameOrigin("https://api.openai.com:8443/v1", "https://api.openai.com/v1"));
        assertFalse(isSameOrigin("https://api.openai.com@attacker.com/v1", "https://api.openai.com/v1"));
        assertFalse(isSameOrigin("https://api.openai.com.attacker.com/v1", "https://api.openai.com/v1"));
        assertFalse(isSameOrigin("https://attacker.com#api.openai.com", "https://api.openai.com"));
    }

    @Test
    public void invalidOrMissingUrlsAreNeverTrusted() {
        assertFalse(isSameOrigin(null, "https://api.openai.com"));
        assertFalse(isSameOrigin("https://api.openai.com", null));
        assertFalse(isSameOrigin("", ""));
        assertFalse(isSameOrigin("api.openai.com/v1", "api.openai.com"));
        assertFalse(isSameOrigin("https://api.openai.com/ v1 with spaces", "https://api.openai.com"));
    }

    @Test
    public void trustedAmongMany() {
        List<String> trusted = List.of("https://api.anthropic.com/v1", "http://localhost:1234");
        assertTrue(isTrusted("http://localhost:1234/messages", trusted));
        assertFalse(isTrusted("http://127.0.0.1:1234/messages", trusted));
        assertFalse(isTrusted("http://localhost:1234/messages", List.of()));
    }

    @Test
    public void httpsHostMatching() {
        Pattern pattern = Pattern.compile("bedrock\\.[a-z0-9-]+\\.amazonaws\\.com");
        assertTrue(isHttpsHostMatching("https://bedrock.us-east-1.amazonaws.com/foundation-models", pattern));
        assertTrue(isHttpsHostMatching("https://BEDROCK.us-east-1.amazonaws.com:443/", pattern));
        assertFalse(isHttpsHostMatching("http://bedrock.us-east-1.amazonaws.com/", pattern));
        assertFalse(isHttpsHostMatching("https://bedrock.us-east-1.amazonaws.com:8443/", pattern));
        assertFalse(isHttpsHostMatching("https://bedrock.us-east-1.amazonaws.com.attacker.com/", pattern));
        assertFalse(isHttpsHostMatching("https://bedrock.us-east-1.amazonaws.com@attacker.com/", pattern));
        assertFalse(isHttpsHostMatching(null, pattern));
    }
}
