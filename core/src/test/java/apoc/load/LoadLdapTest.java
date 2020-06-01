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
        connParms.put("ldapHost", "ldap.forumsys.com");
        connParms.put("ldapPort", 389l);
        connParms.put("loginDN", "cn=read-only-admin,dc=example,dc=com");
        connParms.put("loginPW", "password");
        LoadLdap.LDAPManager mgr = new LoadLdap.LDAPManager(LoadLdap.getConnectionMap(connParms));
        Map<String, Object> searchParms = new HashMap<>();
        searchParms.put("searchBase", "dc=example,dc=com");
        searchParms.put("searchScope", "SCOPE_ONE");
        searchParms.put("searchFilter", "(&(objectClass=*)(uid=training))");
        ArrayList<String> ats = new ArrayList<>();
        ats.add("uid");
        searchParms.put("attributes", ats);
        LDAPSearchResults results = mgr.doSearch(searchParms);
        LDAPEntry le = results.next();
        assertEquals("uid=training,dc=example,dc=com", le.getDN());
        assertEquals("training", le.getAttribute("uid").getStringValue());
    }

}

