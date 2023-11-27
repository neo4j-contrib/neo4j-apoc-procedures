package apoc.export.parquet;

import apoc.util.ExtendedTestContainerUtil;
import apoc.util.Neo4jContainerExtension;
import apoc.util.collection.Iterators;
import apoc.util.s3.S3BaseTest;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Session;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.export.parquet.ParquetTest.MAPPING_ALL;
import static apoc.export.parquet.ParquetUtil.FIELD_LABELS;
import static apoc.export.parquet.ParquetUtil.FIELD_TYPE;
import static apoc.util.TestContainerUtil.*;
import static apoc.util.TestContainerUtil.ApocPackage.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This test verify that Parquet works well 
 * with `apoc-hadoop-dependencies-{version}-all.jar` and `apoc-aws-dependencies-{version}.jar` jars
 */
public class ParquetS3Test extends S3BaseTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeClass() {
        S3BaseTest.baseBeforeClass();

        neo4jContainer = createEnterpriseDB(List.of(CORE, EXTENDED), true)
                .withEnv(APOC_IMPORT_FILE_ENABLED, "true")
                .withEnv(APOC_EXPORT_FILE_ENABLED, "true");

        Collection<File> files = FileUtils.listFiles(pluginsFolder, FileFileFilter.INSTANCE, FileFileFilter.INSTANCE);
        System.out.println("files = " + files);
        
        ExtendedTestContainerUtil.addExtraDependencies();
        neo4jContainer.start();
        
        session = neo4jContainer.getSession();
        
        session.writeTransaction(tx -> tx.run("CREATE (:Start {name:'Franco'})-[:REL {idRel: 1}]->(:End {name: 'Ciccio'})"));
    }

    @AfterClass
    public static void afterClass() {
        S3BaseTest.tearDown();
        
        session.close();
        neo4jContainer.close();
    }

    @Test
    public void testFileRoundtripParquetAll() {
        String url = s3Container.getUrl("test.parquet");
        
        // export
        String file = session.run("CALL apoc.export.parquet.all($url) YIELD file",
                        Map.of("url", url))
                .single().get("file").asString();

        System.out.println("file = " + file);
        
        testCall(session, "CALL apoc.import.parquet($file, $config)",
                Map.of("file", file, "config", Map.of("idRel", "long")),
                r -> {
            assertEquals(4L, r.get("nodes"));
            assertEquals(1L, r.get("relationships"));
        });
        
        // load
        final String query = "CALL apoc.load.parquet($file, $config) YIELD value " +
                             "RETURN value";

        testResult(session, query, Map.of("file", file,  "config", MAPPING_ALL),
                r -> {
                    Map<String, Object> row = (Map<String, Object>) r.next().get("value");
                    assertTrue(row.containsKey(FIELD_LABELS));
                    row = (Map<String, Object>) r.next().get("value");
                    assertTrue(row.containsKey(FIELD_LABELS));
                    row = (Map<String, Object>) r.next().get("value");
                    assertTrue(row.containsKey(FIELD_TYPE));
                    assertFalse(r.hasNext());
                });

        // import
        testResult(session, "MATCH (:Start {name:'Franco')-[:REL {id: 1}]->(:End {name: 'Ciccio'})", 
                r -> assertEquals(2, Iterators.count(r)));


    }

}
