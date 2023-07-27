package apoc.export;

import apoc.ApocConfig;
import apoc.util.FileUtils;
import apoc.util.TestUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import org.neo4j.graphdb.QueryExecutionException;

import static apoc.export.SecurityTestUtil.ERROR_KEY;
import static apoc.export.SecurityTestUtil.PROCEDURE_KEY;
import static apoc.export.SecurityTestUtil.assertPathTraversalError;
import static apoc.util.TestUtil.assertError;

public class TestCypherSchema extends ExportCoreSecurityTestHarness
{
    private final String apocProcedure = "CALL apoc.export.cypher.schema(%s)";

    @Test
    public void testIllegalFSAccessExportCypherSchema() {
        setFileExport(false);
        QueryExecutionException e = Assert.assertThrows( QueryExecutionException.class,
                                                         () -> TestUtil.testCall( db, String.format( apocProcedure, "'./hello', {}"), ( r) -> {
                                                         })
        );
        assertError( e, ApocConfig.EXPORT_TO_FILE_ERROR, RuntimeException.class, apocProcedure);
    }

    @Test
    public void testIllegalExternalFSAccessExportCypherSchema() {
        setFileExport(true);
        assertPathTraversalError( db, String.format(apocProcedure, "'../hello', {}"), Map.of(),
                                  e -> assertError( (Exception) e.get(ERROR_KEY), FileUtils.ACCESS_OUTSIDE_DIR_ERROR, IOException.class, (String) e.get( PROCEDURE_KEY)));
    }
}