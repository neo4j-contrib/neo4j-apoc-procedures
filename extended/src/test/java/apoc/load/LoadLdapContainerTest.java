package apoc.load;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class LoadLdapContainerTest {
    private static final int LDAP_DEFAULT_PORT = 389;
    private static final int LDAP_DEFAULT_SSL_PORT = 636;
    
    private static GenericContainer ldap;

    @ClassRule
    public static TemporaryFolder storeDir = new TemporaryFolder();

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void beforeClass() throws IOException {
        ldap = new GenericContainer("osixia/openldap:1.5.0")
                .withEnv("LDAP_TLS_VERIFY_CLIENT", "try")
                .withExposedPorts(LDAP_DEFAULT_PORT, LDAP_DEFAULT_SSL_PORT)
                .waitingFor( Wait.forListeningPort() );

        addVolumes();
        ldap.start();
        TestUtil.registerProcedure(db, LoadLdap.class);
    }

    /**
     * Added volumes to solve the issue: https://github.com/osixia/docker-openldap/issues/400,
     * without these we can have the following error during startup if we execute the container multiple times: 
     *  [Errno 17] File exists: '/container/service/:ssl-tools/startup.sh' -> '/container/run/startup/:ssl-tools'
     * and, even if the container is started, we have error during apoc.load.ldap(...), i.e. :
     *  IOException(LDAPException(resultCode=91 (connect error), errorMessage='An error occurred while attempting to establish a connection to server localhost/127.0.0.1:56860
     */
    private static void addVolumes() throws IOException {
        ldap.withFileSystemBind(storeDir.newFolder("data/ldap").getAbsolutePath(), "/var/lib/ldap")
            .withFileSystemBind(storeDir.newFolder("data/slapd").getAbsolutePath(), "/etc/ldap/slapd.d")
            .withFileSystemBind(storeDir.newFolder("tmp").getAbsolutePath(), "/tmp");
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
        Map<String, Object> conn = Map.of("ldapHost", "localhost:" + port, 
                "loginDN", "cn=admin,dc=example,dc=org", 
                "loginPW", "admin", 
                "ssl", ssl);
        
        Map<String, Object> searchBase = Map.of("searchBase", "dc=example,dc=org",
                "searchScope", "SCOPE_BASE");
        testCall(db, "call apoc.load.ldap($conn, $search)",
                Map.of("conn", conn, "search", searchBase),
                r -> {
                    Map<String, Object> entry = (Map<String, Object>) r.get("entry");

                    String[] expectedObjectClass = {"top", "dcObject", "organization"};
                    assertArrayEquals(expectedObjectClass, (String[]) entry.get("objectClass"));
                    assertEquals("dc=example,dc=org", entry.get("dn"));
                    assertEquals("Example Inc.", entry.get("o"));
                    assertEquals("example", entry.get("dc"));
                });
    }
}
