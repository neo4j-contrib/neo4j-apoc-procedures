/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.core.it;

import apoc.util.CompressionAlgo;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import org.apache.commons.io.FileUtils;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Session;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.LongStream;

import static apoc.ApocConfig.APOC_MAX_DECOMPRESSION_RATIO;
import static apoc.ApocConfig.DEFAULT_MAX_DECOMPRESSION_RATIO;
import static apoc.export.util.LimitedSizeInputStream.SIZE_EXCEEDED_ERROR;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


public class LoadCoreEnterpriseTest {
    private static final File directory = new File("target/import");
    public static final String COMPRESSED_JSON_FILE = "compressedFile.";
    public static final String COMPRESSED_XML_FILE = "compressedXmlFile.";

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    static {
        //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    private static Neo4jContainerExtension createNeo4jWithMaxCompressionRatio(int ratio) {
        Neo4jContainerExtension container = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.CORE), true)
                .withNeo4jConfig("dbms.memory.heap.max_size", "1GB")
                .withEnv(APOC_MAX_DECOMPRESSION_RATIO, String.valueOf(ratio));
        container.start();

        assertTrue(container.isRunning());

        return container;
    }

    @BeforeClass
    public static void beforeClass() {
        neo4jContainer = createNeo4jWithMaxCompressionRatio(DEFAULT_MAX_DECOMPRESSION_RATIO);
        session = neo4jContainer.getSession();

        loopAllCompressionAlgos(algo -> {
            writeCompressedFile(COMPRESSED_JSON_FILE + algo.name(), algo, writer -> {
                writer.write("{\"test\":\"");
                LongStream.range(0, 99999L)
                        .forEach(__ -> writer.write("000000000000000000000000000000000000000000000000000000000000"));
                writer.write("\"}");
            });
        });
        loopAllCompressionAlgos(algo -> {
            writeCompressedFile(COMPRESSED_XML_FILE + algo.name(), algo, writer -> {
                writer.write("<?xml version=\"1.0\"?><catalog>");
                LongStream.range(0, 999999L)
                        .forEach(__ -> writer.write("000000000000000000000000000000000000000000000000000000000000"));
                writer.write("</catalog>");
            });
        });
    }

    private static void writeCompressedFile(String fileName, CompressionAlgo algo, Consumer<PrintWriter> supplier) {
        try {
            File file = new File(directory, fileName);
            FileOutputStream fileOutputStream = new FileOutputStream(file);
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(algo.getOutputStream(fileOutputStream));
            PrintWriter writer = new PrintWriter(bufferedOutputStream);

            supplier.accept(writer);

            writer.close();
            bufferedOutputStream.close();
            fileOutputStream.close();

            moveFileToContainer(fileName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void moveFileToContainer(String name) throws IOException {
        File url = new File(directory, name).getCanonicalFile();
        FileUtils.copyFile(url, new File(TestContainerUtil.importFolder, name));
    }

    @AfterClass
    public static void afterClass() {
        session.close();
        neo4jContainer.close();
    }

    @Test
    public void testLoadJsonShouldPreventZipBombAttack() {
        RuntimeException e = assertThrows(RuntimeException.class,
                () -> testCall( session, "CALL apoc.load.json($url)",
                        Map.of("url", "https://github.com/iamtraction/ZOD/raw/master/42.zip"),
                        r -> {} )
        );

        Assertions.assertThat(e.getMessage()).contains("Invalid UTF-8 start byte");
    }

    @Test
    public void testLoadXmlShouldPreventZipBombAttack() {
        RuntimeException e = assertThrows(RuntimeException.class,
                () -> testCall( session, "CALL apoc.load.xml($url)",
                        Map.of("url", "https://github.com/iamtraction/ZOD/raw/master/42.zip"),
                        r -> {} )
        );

        Assertions.assertThat(e.getMessage()).contains("Content is not allowed in prolog.");
    }

    @Test
    public void testLoadJsonShouldPreventCompressionBombAttack() {
        loopAllCompressionAlgos(algo -> {
            String algoName = algo.name();
            String fileName = COMPRESSED_JSON_FILE + algoName;
            testMaxSizeExceeded( session,
                                 "CALL apoc.load.json($file, '', {compression: $compression})",
                                 Map.of("file", fileName, "compression", algoName),
                                 new File(directory, fileName).length(),
                                 DEFAULT_MAX_DECOMPRESSION_RATIO );
        });
    }

    @Test
    public void testLoadXmlShouldPreventCompressionBombAttack() {
        loopAllCompressionAlgos(algo -> {
            try {
                String algoName = algo.name();
                byte[] bytes = bytesFromFile(COMPRESSED_XML_FILE, algoName);

                testMaxSizeExceeded( session,
                                     "CALL apoc.load.xml($file, null, {compression: $compression})",
                                     Map.of("file", bytes, "compression", algoName),
                                     bytes.length,
                                     DEFAULT_MAX_DECOMPRESSION_RATIO );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testLoadJsonShouldPreventBinaryCompressionBombAttack() {
        loopAllCompressionAlgos(algo -> {
            try {
                String algoName = algo.name();
                byte[] bytes = bytesFromFile(COMPRESSED_JSON_FILE, algoName);

                testMaxSizeExceeded( session,
                                     "CALL apoc.load.json($file, '', {compression: $compression})",
                                     Map.of("file", bytes, "compression", algoName),
                                     bytes.length,
                                     DEFAULT_MAX_DECOMPRESSION_RATIO );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testLoadXmlShouldPreventBinaryCompressionBombAttack() {
        loopAllCompressionAlgos(algo -> {
            String algoName = algo.name();
            String fileName = COMPRESSED_XML_FILE + algoName;
            testMaxSizeExceeded( session,
                                 "CALL apoc.load.xml($file, null, {compression: $compression})",
                                 Map.of("file", fileName, "compression", algoName),
                                 new File(directory, fileName).length(), DEFAULT_MAX_DECOMPRESSION_RATIO );
        });
    }

    @Test
    public void testLoadShouldPreventZipBombAttackUtilDecompress() {
        loopAllCompressionAlgos(algo -> {
            try {
                String algoName = algo.name();
                byte[] bytes = bytesFromFile(COMPRESSED_JSON_FILE, algoName);

                testMaxSizeExceeded( session,
                                     "RETURN apoc.util.decompress($file, {compression: $algo})",
                                     Map.of("file", bytes, "algo", algoName), bytes.length, DEFAULT_MAX_DECOMPRESSION_RATIO );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testLoadWorksWithIncreasedCompressionRatio() {
        Neo4jContainerExtension neo4jContainer = createNeo4jWithMaxCompressionRatio(100000);
        Session session = neo4jContainer.getSession();

        loopAllCompressionAlgos(algo -> {
            String algoName = algo.name();
            String fileName = COMPRESSED_JSON_FILE + algoName;
            testCall( session, "CALL apoc.load.json($file, '', {compression: $compression})",
                      Map.of("file", fileName, "compression", algoName),
                      r -> assertFalse(r.isEmpty()));
        });

        neo4jContainer.close();
        session.close();
    }

    @Test
    public void testLoadZipBombFailsWithNonDefaultRatio() {
        int compressionRatio = 101;
        Neo4jContainerExtension neo4jContainer = createNeo4jWithMaxCompressionRatio(compressionRatio);
        Session session = neo4jContainer.getSession();

        var compressionAlgorithm = CompressionAlgo.GZIP;
        String algoName = compressionAlgorithm.name();
        String fileName = COMPRESSED_JSON_FILE + algoName;
        testMaxSizeExceeded( session,
                            "CALL apoc.load.json($file, '', {compression: $compression})",
                             Map.of("file", fileName, "compression", algoName),
                             new File(directory, fileName).length(),
                             compressionRatio);

        neo4jContainer.close();
        session.close();
    }

    private static void loopAllCompressionAlgos(Consumer<CompressionAlgo> compressionAlgoConsumer) {
        Arrays.stream(CompressionAlgo.values())
                // ignored `FRAMED_SNAPPY` since it does not have efficient compression like the others
                //  and causes heap space instead of the error given by `LimitedSizeInputStream`
                .filter(algo -> !algo.equals(CompressionAlgo.NONE) && !algo.equals(CompressionAlgo.FRAMED_SNAPPY))
                .forEach(compressionAlgoConsumer);
    }

    private static byte[] bytesFromFile(String compressedXmlFile, String algoName) throws IOException {
        Path path = new File(directory, compressedXmlFile + algoName).toPath();
        return Files.readAllBytes(path);
    }

    private void testMaxSizeExceeded(Session session, String query, Map<String, Object> params, long fileSize, int compressionRatio) {
        RuntimeException e = assertThrows(RuntimeException.class,
                () -> testCall( session, query, params, r -> {} )
        );

        String sizeExceededError = String.format(SIZE_EXCEEDED_ERROR,
                fileSize * compressionRatio, compressionRatio);
        Assertions.assertThat(e.getMessage()).contains(sizeExceededError);
    }
}
