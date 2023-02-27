package apoc.load;


import apoc.util.TestUtil;
import com.novell.ldap.LDAPEntry;
import com.novell.ldap.LDAPSearchResults;
import com.unboundid.ldap.sdk.LDAPConnection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.zapodot.junit.ldap.EmbeddedLdapRule;
import org.zapodot.junit.ldap.EmbeddedLdapRuleBuilder;

import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class LoadLdapTest {
    public static final String BIND_DSN = "uid=admin,cn=users,cn=accounts,dc=demo1,dc=freeipa";
    public static final String BIND_PWD = "testPwd";
    public static LDAPConnection ldapConnection;
    public static Map<String, Object> connParams;
    public static Map<String, Object> searchParams;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();
    
    @ClassRule
    public static EmbeddedLdapRule embeddedLdapRule = EmbeddedLdapRuleBuilder
            .newInstance()
            .usingBindDSN(BIND_DSN)
            .usingBindCredentials(BIND_PWD)
            .importingLdifs("ldap/example.ldif")
            .build();

    @BeforeClass
    public static void beforeClass() throws Exception {
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
    public void testLoadLDAP() {
        testCall(db, "call apoc.load.ldap($conn, $search)",
                Map.of("conn", connParams, "search", searchParams), r -> {
                    final Map<String, String> expected = Map.of("uid", "training",
                            "dn", "uid=training,dc=example,dc=com");
                    assertEquals(expected, r.get("entry"));
                });
    }

    @Test
    public void testLoadLDAPConfig() throws Exception {
        LoadLdap.LDAPManager mgr = new LoadLdap.LDAPManager(LoadLdap.getConnectionMap(connParams));
        
        LDAPSearchResults results = mgr.doSearch(searchParams);
        LDAPEntry le = results.next();
        assertEquals("uid=training,dc=example,dc=com", le.getDN());
        assertEquals("training", le.getAttribute("uid").getStringValue());

    }

}

