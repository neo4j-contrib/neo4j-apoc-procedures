package apoc.load;

import apoc.ApocConfiguration;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
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
import static org.junit.Assert.assertTrue;

public class LoadLdapTest {
    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = TestUtil.apocGraphDatabaseBuilder().newGraphDatabase();
        ApocConfiguration.initialize((GraphDatabaseAPI) db);
        ApocConfiguration.addToConfig(map("ldap.forumsys_noauth.url", "ldap://ldap.forumsys.com:389/dc=example,dc=com?uid?one?(&(objectClass=*)(uid=training))"));
        ApocConfiguration.addToConfig(map("ldap.forumsys_auth.url", "ldap://ldap.forumsys.com:389/dc=example,dc=com?uid?one?(&(objectClass=*)(uid=training))"));
        ApocConfiguration.addToConfig(map("ldap.forumsys_auth.username", "cn=read-only-admin,dc=example,dc=com"));
        ApocConfiguration.addToConfig(map("ldap.forumsys_auth.password", "password"));
        ApocConfiguration.addToConfig(map("ldap.freeipa_demo.url", "ldap://ipa.demo1.freeipa.org/dc=demo1,dc=freeipa,dc=org?uid?sub?(objectClass=posixAccount)"));
        ApocConfiguration.addToConfig(map("ldap.freeipa_demo.username", "uid=admin,cn=users,cn=accounts,dc=demo1,dc=freeipa,dc=org"));
        ApocConfiguration.addToConfig(map("ldap.freeipa_demo.password", "Secret123"));
        ApocConfiguration.addToConfig(map("ldap.debian.url", "ldaps://db.debian.org/dc=debian,dc=org?uid?sub?(uid=dbharris)"));
        TestUtil.registerProcedure(db, LoadLdap.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    private List<Map<String, Object>> consumeResults(Result rows) {
        List<Map<String, Object>> results = new ArrayList<>();
        while (rows.hasNext()) {
            Map<String, Object> row = rows.next();
            System.out.println(row);
            results.add(row);
        }
        return results;
    }

    @Test
    public void testLoadNoAuth() {
        Map<String, Object> training = new HashMap<>();
        training.put("dn", "uid=training,dc=example,dc=com");
        training.put("uid", "training");
        testCall(db, "CALL apoc.load.ldapfromconfig('forumsys_noauth')", (row) -> assertEquals(row, map("entry", training)));
    }

    @Test
    public void testLoadAuth() {
        Map<String, Object> training = new HashMap<>();
        training.put("dn", "uid=training,dc=example,dc=com");
        training.put("uid", "training");
        testCall(db, "CALL apoc.load.ldapfromconfig('forumsys_auth')", (row) -> assertEquals(row, map("entry", training)));
    }

    @Test
    public void testNonconformantSchema() {
        // if we got results back, then the search succeeded
        testResult(db, "CALL apoc.load.ldapfromconfig('freeipa_demo')", (row) -> assertTrue(row.hasNext()));
    }

    @Test
    public void testInlineConfig() {
        Map<String, Object> training = new HashMap<>();
        training.put("dn", "uid=training,dc=example,dc=com");
        training.put("uid", "training");
        testCall(db, "CALL apoc.load.ldap('ldap://ldap.forumsys.com:389/dc=example,dc=com?uid?one?(&(objectClass=*)(uid=training))', " +
                "{credentials: {user: 'cn=read-only-admin,dc=example,dc=com', password: 'password'}})",
                (row) -> assertEquals(row, map("entry", training)));
    }

    @Test
    public void testPagingSupport() {
        testResult(db, "CALL apoc.load.ldap('ldaps://db.debian.org/dc=debian,dc=org?uid?sub?(objectclass=inetorgperson)')", (row)->assertTrue(consumeResults(row).size()>1000));
    }
}
