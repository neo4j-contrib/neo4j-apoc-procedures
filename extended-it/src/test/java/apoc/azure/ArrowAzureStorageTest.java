package apoc.azure;

import apoc.export.arrow.ArrowTestUtil;
import apoc.export.arrow.ExportArrow;
import apoc.export.arrow.ImportArrow;
import apoc.load.LoadArrow;
import apoc.meta.Meta;
import apoc.util.TestUtil;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.time.OffsetDateTime;
import java.util.Map;

import static apoc.export.arrow.ArrowTestUtil.testImportCommon;
import static apoc.export.parquet.ParquetTest.MAPPING_ALL;
import static apoc.export.parquet.ParquetTestUtil.beforeClassCommon;
import static apoc.export.parquet.ParquetTestUtil.beforeCommon;
import static apoc.export.parquet.ParquetTestUtil.testImportAllCommon;
import static apoc.util.ExtendedITUtil.EXTENDED_PATH;

public class ArrowAzureStorageTest extends AzureStorageBaseTest {

    private final String EXPORT_FILENAME = "test_all.parquet";

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseInternalSettings.enable_experimental_cypher_versions, true);

    @BeforeClass
    public static void beforeClass() {
        TestUtil.registerProcedure(db, ExportArrow.class, ImportArrow.class, LoadArrow.class, Meta.class);
        
        beforeClassCommon(db);
    }

    @Before
    public void before() {
        beforeCommon(db);
    }

    @Test
    public void testRoundtripArrow() {
        String fileName = "test_all.arrow";
        String blobFileUrl = containerClient.getBlobContainerUrl() + "/" + fileName;
        // optional: if url is azb://
        blobFileUrl = blobFileUrl.replaceFirst("http://", "azb://");

        String file = db.executeTransactionally("CYPHER 25 CALL apoc.export.arrow.all($url) YIELD file",
                Map.of("url", blobFileUrl),
                ArrowTestUtil::extractFileName);

        String fileImport = file + "?" + getSasToken(fileName);

        testImportCommon(db, fileImport, ArrowTestUtil.MAPPING_ALL);
    }
    
    private static String getSasToken(String blobName) {
        BlobClient blobClient = containerClient.getBlobClient(blobName);
        BlobSasPermission permission = new BlobSasPermission().setReadPermission(true);
        OffsetDateTime expiryTime = OffsetDateTime.now().plusHours(1);
        return blobClient.generateSas(new BlobServiceSasSignatureValues(expiryTime, permission), new Context("Azure-Storage-Log-String-To-Sign", "true"));
    }

    @Test
    public void testImportParquet() {
        String url = putToAzureStorageAndGetUrl(EXTENDED_PATH + "src/test/resources/" + EXPORT_FILENAME);

        Map<String, Object> params = Map.of("file", url, "config", MAPPING_ALL);
        testImportAllCommon(db, params);
    }
}
