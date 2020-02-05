package apoc.load;

import org.apache.directory.api.ldap.model.cursor.CursorException;
import org.apache.directory.api.ldap.model.cursor.SearchCursor;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.exception.LdapURLEncodingException;
import org.apache.directory.api.ldap.model.message.SearchResultEntry;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class LoadLdapTest {
    @Test
    public void testAnonBindViaNoDNorPW() throws LdapException, CursorException {
        Map<String, String> connParms = new HashMap<>();
        connParms.put("ldapURL", "ldap://ldap.forumsys.com:389/dc=example,dc=com?uid?one?(&(objectClass=*)(uid=training))");
        LoadLdap.LDAPManager mgr = new LoadLdap.LDAPManager(connParms, log);
        SearchCursor results = mgr.doSearch();
        results.next();
        SearchResultEntry le = (SearchResultEntry) results.get();
        assertEquals("uid=training,dc=example,dc=com", le.getEntry().getDn().toString());
        assertEquals("training", le.getEntry().get("uid").get().toString());
    }

    @Test
    public void testLoadLDAP() throws Exception {
        Map<String, String> connParms = new HashMap<>();
        connParms.put("ldapURL", "ldap://ldap.forumsys.com:389/dc=example,dc=com?uid?one?(&(objectClass=*)(uid=training))");
        connParms.put("loginDN", "cn=read-only-admin,dc=example,dc=com");
        connParms.put("loginPW", "password");
        LoadLdap.LDAPManager mgr = new LoadLdap.LDAPManager(connParms, log);
        SearchCursor results = mgr.doSearch();
        results.next();
        SearchResultEntry le = (SearchResultEntry) results.get();
        assertEquals("uid=training,dc=example,dc=com", le.getEntry().getDn().toString());
        assertEquals("training", le.getEntry().get("uid").get().toString());
    }

    @Test
    public void testLDAPAggregation() throws LdapURLEncodingException {
        Map<String, String> connParms = new HashMap<>();
        connParms.put("ldapURL", "ldap://ipa.demo1.freeipa.org/dc=demo1,dc=freeipa,dc=org?uid?sub?(objectClass=posixAccount)");
        connParms.put("loginDN", "uid=admin,cn=users,cn=accounts,dc=demo1,dc=freeipa,dc=org");
        connParms.put("loginPW", "Secret123");
        LoadLdap.LDAPManager mgr = new LoadLdap.LDAPManager(connParms, log);
        Stream<LDAPResult> result = mgr.executeSearch();
        /*
            Note, the expect result is prone to changing as the server is refreshed
            every so often. Best to manually verify the count using an LDAP client
            before running the test
         */
        assertEquals(9, result.count());
    }

    @Test
    public void testLDAPSConnection() throws LdapURLEncodingException {
        // Assume that we're using a truststore that contains the Let's Encrypt CA (which is in the Java default)
        Map<String, String> connParms = new HashMap<>();
        connParms.put("ldapURL", "ldaps://db.debian.org/dc=debian,dc=org?uid?sub?(uid=dbharris)");
        connParms.put("loginDN", "");
        connParms.put("loginPW", "");
        LoadLdap.LDAPManager mgr = new LoadLdap.LDAPManager(connParms, log);
        Stream<LDAPResult> result = mgr.executeSearch();
        assertEquals(1, result.count());
    }
}
