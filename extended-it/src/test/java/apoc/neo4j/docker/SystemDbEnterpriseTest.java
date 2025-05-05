package apoc.neo4j.docker;

import apoc.util.ExtendedITUtil;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Session;
import org.neo4j.driver.types.Node;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static apoc.systemdb.SystemDb.REMOTE_SENSITIVE_PROP;
import static apoc.systemdb.SystemDb.USER_SENSITIVE_PROP;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static org.junit.Assert.assertTrue;

public class SystemDbEnterpriseTest {
    private static final String USERNAME = "nonadmin";
    private static final String PASSWORD = "keystore-password";

    private static final String KEYSTORE_NAME_PKCS_12 = "keystore-name.pkcs12";
    // we put the keystore file in the import folder for simplicity (because it's bound during the creation of the container) 
    private static final File KEYSTORE_FILE = new File(ExtendedITUtil.EXTENDED_PATH + "import", KEYSTORE_NAME_PKCS_12);
    
    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;
    
    
    @BeforeClass
    public static void beforeClass() throws Exception {
        final String randomKeyAlias = UUID.randomUUID().toString();
        
        // certificate file creation
        final String[] args = new String[] { "keytool", 
                "-genseckey", "-keyalg", "aes", "-keysize", "256", "-storetype", "pkcs12", 
                "-keystore", KEYSTORE_FILE.getCanonicalPath(), 
                "-alias", randomKeyAlias, 
                "-storepass", PASSWORD};

        Process proc = new ProcessBuilder(args).start();
        proc.waitFor();
        
        // We build the project, the artifact will be placed into ./build/libs
        final String pathPwdValue = "/var/lib/neo4j/import/" + KEYSTORE_FILE.getName();
        
        // we add config useful to create a remote db alias 
        neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.EXTENDED), true)
                .withNeo4jConfig("dbms.security.keystore.path", pathPwdValue)
                .withNeo4jConfig("dbms.security.keystore.password", PASSWORD)
                .withNeo4jConfig("dbms.security.key.name", randomKeyAlias);

        neo4jContainer.start();
        session = neo4jContainer.getSession();

    }
    
    @AfterClass
    public static void afterClass() throws IOException {
        FileUtils.forceDelete(KEYSTORE_FILE);
        
        session.close();
        neo4jContainer.close();
    }

    @Test
    public void systemDbGraphProcedureShouldObfuscateSensitiveData() {

        // user creation
        session.run(String.format("CREATE USER %s SET PASSWORD '%s' SET PASSWORD CHANGE NOT REQUIRED",
                USERNAME, PASSWORD));

        // steps for remote database alias creation
        session.run("CREATE ROLE remote");
        session.run("GRANT ACCESS ON DATABASE neo4j TO remote");
        session.run("GRANT MATCH {*} ON GRAPH neo4j TO remote");
        session.run(String.format("GRANT ROLE remote TO %s", USERNAME));
        // mock remote database alias creation
        session.run(String.format("CREATE ALIAS `remote-neo4j` FOR DATABASE `neo4j` AT \"neo4j+s://location:7687\" USER %s PASSWORD '%s'",
                USERNAME, PASSWORD));
        
        // get all apoc.systemdb.graph nodes
        final List<Object> nodes = session.run("call apoc.systemdb.graph")
                .single()
                .get("nodes")
                .asList();

        // check that there aren't sensitive properties
        final boolean noSensitiveProps = nodes.stream()
                .map(node -> (Node) node)
                .noneMatch(node -> node.containsKey(REMOTE_SENSITIVE_PROP) 
                                || node.containsKey(USER_SENSITIVE_PROP));
        assertTrue(noSensitiveProps);
    }
}

