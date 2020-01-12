package apoc.load;


import com.novell.ldap.LDAPEntry;
import com.novell.ldap.LDAPException;
import com.novell.ldap.LDAPSearchResults;
import org.junit.Test;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class LoadLdapTest {
    @Test(expected = MalformedURLException.class)
    public void testBadURL() throws MalformedURLException {
        Map<String, String> connParms = new HashMap<>();
        connParms.put("ldapURL", "ldap://bad.example.com/?????");
        connParms.put("loginDN", "cn=someuser");
        connParms.put("loginPW", "password");
        LoadLdap.LDAPManager mgr = new LoadLdap.LDAPManager(connParms);
    }

    @Test(expected = RuntimeException.class)
    public void testEmptyURL() throws RuntimeException, MalformedURLException {
        Map<String, String> connParms = new HashMap<>();
        connParms.put("ldapURL", "");
        connParms.put("loginDN", "");
        connParms.put("loginPW", "");
        LoadLdap.LDAPManager mgr = new LoadLdap.LDAPManager(connParms);
    }

    @Test
    public void testAnonBindViaNoDNorPW() throws MalformedURLException, LDAPException {
        Map<String, String> connParms = new HashMap<>();
        connParms.put("ldapURL", "ldap://ldap.forumsys.com:389/dc=example,dc=com?uid?one?(&(objectClass=*)(uid=training))");
        //connParms.put("loginDN", "");
        //connParms.put("loginPW", "");
        LoadLdap.LDAPManager mgr = new LoadLdap.LDAPManager(connParms);
        LDAPSearchResults results = mgr.doSearch();
        LDAPEntry le = results.next();
        assertEquals("uid=training,dc=example,dc=com", le.getDN());
        assertEquals("training", le.getAttribute("uid").getStringValue());
    }

    @Test
    public void testLoadLDAP() throws Exception {
        Map<String, String> connParms = new HashMap<>();
        connParms.put("ldapURL", "ldap://ldap.forumsys.com:389/dc=example,dc=com?uid?one?(&(objectClass=*)(uid=training))");
        connParms.put("loginDN", "cn=read-only-admin,dc=example,dc=com");
        connParms.put("loginPW", "password");
        LoadLdap.LDAPManager mgr = new LoadLdap.LDAPManager(connParms);
        LDAPSearchResults results = mgr.doSearch();
        LDAPEntry le = results.next();
        assertEquals("uid=training,dc=example,dc=com", le.getDN());
        assertEquals("training", le.getAttribute("uid").getStringValue());
    }

    @Test
    public void testLDAPAggregation() throws MalformedURLException {
        Map<String, String> connParms = new HashMap<>();
        connParms.put("ldapURL", "ldap://ipa.demo1.freeipa.org/dc=demo1,dc=freeipa,dc=org?uid?sub?(objectClass=posixAccount)");
        connParms.put("loginDN", "uid=admin,cn=users,cn=accounts,dc=demo1,dc=freeipa,dc=org");
        connParms.put("loginPW", "Secret123");
        LoadLdap.LDAPManager mgr = new LoadLdap.LDAPManager(connParms);
        Stream<LDAPResult> result = mgr.executeSearch();
        /*
            Note, the expect result is prone to changing as the server is refreshed
            every so often. Best to manually verify the count using an LDAP client
            before running the test
         */
        assertEquals(13, result.count());
    }

    @Test
    public void testLDAPSConnection() throws MalformedURLException {
        // Assume that we're using a truststore that contains the Let's Encrypt CA (which is in the Java default)
        Map<String, String> connParms = new HashMap<>();
        connParms.put("ldapURL", "ldaps://db.debian.org/dc=debian,dc=org?uid?sub?(uid=dbharris)");
        connParms.put("loginDN", "");
        connParms.put("loginPW", "");
        LoadLdap.LDAPManager mgr = new LoadLdap.LDAPManager(connParms);
        Stream<LDAPResult> result = mgr.executeSearch();
        assertEquals(1, result.count());
    }
}

