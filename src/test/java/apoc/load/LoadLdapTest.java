package apoc.load;

import apoc.ApocConfiguration;
import apoc.util.TestUtil;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.core.integ.FrameworkRunner;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(FrameworkRunner.class)
@CreateDS(name = "testDS",
        partitions = {
            @CreatePartition(name = "test", suffix = "dc=neo4j,dc=test")
        })
@CreateLdapServer(
        transports = {
                @CreateTransport(protocol = "LDAP", address = "localhost")
        },
        allowAnonymousAccess = true
)
@ApplyLdifFiles({"users.ldif"})
public class LoadLdapTest extends AbstractLdapTestUnit {
    private GraphDatabaseService db;
    private int ldapServerPort = ldapServer.getPort();

    private List<Map<String, Object>> consumeResults(Result rows) {
        List<Map<String, Object>> results = new ArrayList<>();
        while (rows.hasNext()) {
            Map<String, Object> row = rows.next();
            results.add(row);
        }
        return results;
    }

    @Before
    public void setUp() throws Exception {
        db = TestUtil.apocGraphDatabaseBuilder().newGraphDatabase();
        ApocConfiguration.initialize((GraphDatabaseAPI) db);
        String unsecuredUrl = String.format("ldap://localhost:%d/dc=neo4j,dc=test?cn?sub?(&(objectClass=person)(cn=Agent Smith))", ldapServerPort);
        ApocConfiguration.addToConfig(map("ldap.localhost_noauth.url", unsecuredUrl));
        ApocConfiguration.addToConfig(map("ldap.localhost_auth.url", unsecuredUrl));
        ApocConfiguration.addToConfig(map("ldap.localhost_auth.user", "uid=admin,ou=system"));
        ApocConfiguration.addToConfig(map("ldap.localhost_auth.password", "secret"));
        TestUtil.registerProcedure(db, LoadLdap.class);
    }

    @Test
    public void testLoadNoAuth() {
        Map<String, Object> agentSmith = new HashMap<>();
        agentSmith.put("dn", "cn=Agent Smith,ou=Users,dc=neo4j,dc=test");
        agentSmith.put("cn", "Agent Smith");
        testCall(db, "CALL apoc.load.ldapfromconfig('localhost_noauth')", (row) -> assertEquals(row, map("entry", agentSmith)));
    }

    @Test
    public void testLoadAuth() {
        Map<String, Object> agentSmith = new HashMap<>();
        agentSmith.put("dn", "cn=Agent Smith,ou=Users,dc=neo4j,dc=test");
        agentSmith.put("cn", "Agent Smith");
        testCall(db, "CALL apoc.load.ldapfromconfig('localhost_auth')", (row) -> assertEquals(row, map("entry", agentSmith)));
    }

    @Test
    public void testInlineConfig() {
        Map<String, Object> agentSmith = new HashMap<>();
        agentSmith.put("dn", "cn=Agent Smith,ou=Users,dc=neo4j,dc=test");
        agentSmith.put("cn", "Agent Smith");
        testCall(db, String.format("CALL apoc.load.ldap('ldap://localhost:%d/dc=neo4j,dc=test?cn?sub?(&(objectClass=person)(cn=Agent Smith))', ", ldapServerPort) +
                        "{credentials: {user: 'uid=admin,ou=system', password: 'secret'}})",
                (row) -> assertEquals(row, map("entry", agentSmith)));
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    /*
    @Test
    public void testPagingSupport() {
        testResult(db, "CALL apoc.load.ldap('ldaps://db.debian.org/dc=debian,dc=org?uid?sub?(objectclass=inetorgperson)')", (row)->assertTrue(consumeResults(row).size()>1000));
    }*/
}
