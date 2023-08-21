package apoc.load;


import apoc.util.FileUtils;
import apoc.util.TestUtil;
import com.novell.ldap.LDAPEntry;
import com.novell.ldap.LDAPSearchResults;
import com.unboundid.ldap.sdk.LDAPConnection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.zapodot.junit.ldap.EmbeddedLdapRule;
import org.zapodot.junit.ldap.EmbeddedLdapRuleBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LoadLdapTest {
    public static final String BIND_DSN = "uid=admin,cn=users,cn=accounts,dc=demo1,dc=freeipa";
    public static final String BIND_PWD = "testPwd";
    public static LDAPConnection ldapConnection;
    public static Map<String, Object> connParams;
    public static Map<String, Object> searchParams;

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    private static GraphDatabaseService db;

    @ClassRule
    public static EmbeddedLdapRule embeddedLdapRule = EmbeddedLdapRuleBuilder
            .newInstance()
            .usingBindDSN(BIND_DSN)
            .usingBindCredentials(BIND_PWD)
            .importingLdifs("ldap/example.ldif")
            .build();

    @BeforeClass
    public static void beforeClass() throws Exception {
        DatabaseManagementService dbms = new TestDatabaseManagementServiceBuilder(tempFolder.getRoot().toPath()).build();
        db = dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        TestUtil.registerProcedure(db, LoadLdap.class);

        ldapConnection = embeddedLdapRule.unsharedLdapConnection();

        connParams = Map.of("ldapHost", "localhost:" + ldapConnection.getConnectedPort(),
                "loginDN", BIND_DSN,
                "loginPW", BIND_PWD);

        searchParams = Map.of("searchBase", "dc=example,dc=com",
                "searchScope", "SCOPE_ONE",
                "searchFilter", "(objectClass=*)",
                "attributes", List.of("uid") );
    }

    @AfterClass
    public static void afterClass()  {
        ldapConnection.close();
    }

    @Test
    public void testLoadLDAPWithApocConfig() {
        String key = "apoc.loadldap.myldap.config";
        testWithStringConfigCommon(key);

        // the config with dot after loadldap shouldn't print a log warn
        String logWarn = "Not to cause breaking-change, the current config `apoc.loadldap.myldap.config` is valid";
        assertFalse(getLogFileContent().contains(logWarn));
    }

    @Test
    public void testLoadLDAPWithApocConfigWithoutDotBeforeLdapKey() {
        // analogous to `testLoadLDAPWithApocConfig`, but without dot between `loadldap` and `myldap`
        // it still works not to cause a breaking change
        String key = "apoc.loadldapmyldap.config";
        testWithStringConfigCommon(key);

        // the config without dot after loadldap should print a log warn
        String logWarn = "Not to cause breaking-change, the current config `apoc.loadldapmyldap.config` is valid";
        assertTrue(getLogFileContent().contains(logWarn));
    }

    private static String getLogFileContent() {
        try {
            File logFile = new File(FileUtils.getLogDirectory(), "debug.log");
            return Files.readString(logFile.toPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void testWithStringConfigCommon(String key) {
        // set a config `key=localhost:port dns pwd`
        String ldapValue = "%s %s %s".formatted(
                "localhost:" + ldapConnection.getConnectedPort(),
                BIND_DSN,
                BIND_PWD);
        apocConfig().setProperty(key, ldapValue);

        testCall(db, "call apoc.load.ldap($conn, $search)",
                Map.of("conn", "myldap", "search", searchParams),
                this::testLoadAssertionCommon);

        // remove current config to prevent multiple confs in other tests
        apocConfig().getConfig().clearProperty(key);
    }

    @Test
    public void testLoadLDAPWithWrongApocConfig() {
        apocConfig().setProperty("apoc.loadldap.mykey.config", "host logindn pwd");

        String expected = "No apoc.loadldap.wrongKey.config ldap access configuration specified";
        try {
            testCall(db, "call apoc.load.ldap('wrongKey', {})",
                    r -> fail("Should fail due to: " + expected));
        } catch (RuntimeException e) {
            String actual = e.getMessage();
            assertTrue("Current err. message is: " + actual, actual.contains(expected));
        }
    }

    @Test
    public void testLoadLDAP() {
        testCall(db, "call apoc.load.ldap($conn, $search)",
                Map.of("conn", connParams, "search", searchParams),
                this::testLoadAssertionCommon);
    }

    private void testLoadAssertionCommon(Map<String, Object> r) {
        final Map<String, String> expected = Map.of("uid", "training",
                "dn", "uid=training,dc=example,dc=com");
        assertEquals(expected, r.get("entry"));
    }

    @Test
    public void testLoadLDAPConfig() throws Exception {
        LoadLdap.LDAPManager mgr = new LoadLdap.LDAPManager(LoadLdap.getConnectionMap(connParams, null));
        
        LDAPSearchResults results = mgr.doSearch(searchParams);
        LDAPEntry le = results.next();
        assertEquals("uid=training,dc=example,dc=com", le.getDN());
        assertEquals("training", le.getAttribute("uid").getStringValue());

    }

}

