package apoc.load;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import apoc.util.TestUtil;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.containers.GenericContainer;

public class LoadLdapContainerTest {
    private static final int LDAP_DEFAULT_PORT = 389;
    private static final int LDAP_DEFAULT_SSL_PORT = 636;

    private static GenericContainer ldap;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void beforeClass() {
        ldap = new GenericContainer("osixia/openldap:1.5.0")
                .withEnv("LDAP_TLS_VERIFY_CLIENT", "try")
                .withExposedPorts(LDAP_DEFAULT_PORT, LDAP_DEFAULT_SSL_PORT);
        ldap.start();
        TestUtil.registerProcedure(db, LoadLdap.class);
    }

    @AfterClass
    public static void tearDown() {
        ldap.stop();
        db.shutdown();
    }

    @Test
    public void testLoadLDAPWithSSLPort() {
        int port = ldap.getMappedPort(LDAP_DEFAULT_SSL_PORT);
        testLoadLDAPCommon(port, true);
    }

    @Test
    public void testLoadLDAPWithDefaultPort() {
        int port = ldap.getMappedPort(LDAP_DEFAULT_PORT);
        testLoadLDAPCommon(port, false);
    }

    private static void testLoadLDAPCommon(int port, boolean ssl) {
        Map<String, Object> conn = Map.of(
                "ldapHost",
                "localhost:" + port,
                "loginDN",
                "cn=admin,dc=example,dc=org",
                "loginPW",
                "admin",
                "ssl",
                ssl);

        Map<String, Object> searchBase = Map.of("searchBase", "dc=example,dc=org", "searchScope", "SCOPE_BASE");
        testCall(db, "call apoc.load.ldap($conn, $search)", Map.of("conn", conn, "search", searchBase), r -> {
            Map<String, Object> entry = (Map<String, Object>) r.get("entry");

            String[] expectedObjectClass = {"top", "dcObject", "organization"};
            assertArrayEquals(expectedObjectClass, (String[]) entry.get("objectClass"));
            assertEquals("dc=example,dc=org", entry.get("dn"));
            assertEquals("Example Inc.", entry.get("o"));
            assertEquals("example", entry.get("dc"));
        });
    }
}
