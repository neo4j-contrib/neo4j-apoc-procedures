package apoc.load;


import com.novell.ldap.LDAPEntry;
import com.novell.ldap.LDAPSearchResults;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LoadLdapTest {

    @Test
    public void testLoadLDAP() throws Exception {
        Map<String, Object> connParms = new HashMap<>();
        connParms.put("ldapHost", "ipa.demo1.freeipa.org");
        connParms.put("ldapPort", 389l);
        connParms.put("loginDN", "uid=admin,cn=users,cn=accounts,dc=demo1,dc=freeipa,dc=org");
        connParms.put("loginPW", "Secret123");
        LoadLdap.LDAPManager mgr = new LoadLdap.LDAPManager(LoadLdap.getConnectionMap(connParms));
        Map<String, Object> searchParms = new HashMap<>();
        searchParms.put("searchBase", "dc=demo1,dc=freeipa,dc=org");
        searchParms.put("searchScope", "SCOPE_ONE");
        searchParms.put("searchFilter", "(&(objectclass=*)(cn=alt))");
        ArrayList<String> ats = new ArrayList<>();
        final String attrName = "cn";
        ats.add(attrName);
        searchParms.put("attributes", ats);
        LDAPSearchResults results = mgr.doSearch(searchParms);
        LDAPEntry le = results.next();
        assertEquals("cn=alt,dc=demo1,dc=freeipa,dc=org", le.getDN());
        assertEquals("alt", le.getAttribute(attrName).getStringValue());
    }

}

