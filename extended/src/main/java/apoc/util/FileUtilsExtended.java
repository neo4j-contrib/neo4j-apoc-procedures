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
package apoc.util;

import apoc.ExtendedApocConfig;
import apoc.export.util.CountingInputStreamExtended;
import apoc.export.util.CountingReaderExtended;
import apoc.export.util.ExportConfigExtended;
import apoc.util.hdfs.HDFSUtilsExtended;
import apoc.util.s3.S3URLConnectionExtended;
import apoc.util.s3.S3UploadUtilsExtended;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.graphdb.security.URLAccessValidationError;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import static apoc.ExtendedApocConfig.APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM;
import static apoc.ExtendedApocConfig.extendedApocConfig;
import static apoc.util.UtilExtended.ERROR_BYTES_OR_STRING;
import static apoc.util.UtilExtended.REDIRECT_LIMIT;
import static apoc.util.UtilExtended.readHttpInputStream;

/**
 * @author mh
 * @since 22.05.16
 */
public class FileUtilsExtended {

    public static String getLoadFileUrl(String fileName, URLAccessChecker urlAccessChecker)
            throws MalformedURLException, URLAccessValidationError {
        URL url;
        try {
            url = URI.create(fileName).toURL();
        } catch (MalformedURLException | IllegalArgumentException e) {
            String withFile = "file:///" + fileName;
            url = URI.create(withFile).toURL();
        }
        return urlAccessChecker.checkURL(url).getFile();
    }

    public static String getFileUrl(String fileName) throws MalformedURLException {
        try {
            return new URL(fileName).getFile();
        } catch (MalformedURLException e) {
            if (e.getMessage().contains("no protocol")) {
                return fileName;
            }
            throw e;
        }
    }

    public static StreamConnectionExtended getStreamConnection(
            SupportedProtocolsExtended protocol,
            String urlAddress,
            Map<String, Object> headers,
            String payload,
            URLAccessChecker urlAccessChecker)
            throws IOException, URLAccessValidationError, URISyntaxException {
        switch (protocol) {
            case s3:
                return FileUtilsExtended.openS3InputStream(urlAddress);
            case hdfs:
                return FileUtilsExtended.openHdfsInputStream(urlAddress);
            case ftp:
            case http:
            case https:
            case gs:
                return readHttpInputStream(urlAddress, headers, payload, REDIRECT_LIMIT, urlAccessChecker);
            default:
                try {
                    URL url = urlAccessChecker.checkURL(URI.create(urlAddress).toURL());
                    return new StreamConnectionExtended.FileStreamConnection(url.toURI());
                } catch (IllegalArgumentException iae) {
                    return new StreamConnectionExtended.FileStreamConnection(getLoadFileUrl(urlAddress, urlAccessChecker));
                }
        }
    }

    public static URLStreamHandler createURLStreamHandler(SupportedProtocolsExtended protocol) {
        URLStreamHandler handler = Optional.ofNullable(protocol.getUrlStreamHandlerClassName())
                .map(UtilExtended::createInstanceOrNull)
                .map(urlStreamHandlerFactory ->
                        ((URLStreamHandlerFactory) urlStreamHandlerFactory).createURLStreamHandler(protocol.name()))
                .orElse(null);
        return handler;
    }

    public static SupportedProtocolsExtended of(String name) {
        try {
            return SupportedProtocolsExtended.valueOf(name);
        } catch (Exception e) {
            return SupportedProtocolsExtended.file;
        }
    }

    public static SupportedProtocolsExtended from(URL url) {
        return of(url.getProtocol());
    }

    public static SupportedProtocolsExtended from(String source) {
        try {
            final URL url = new URL(source);
            return from(url);
        } catch (MalformedURLException e) {
            if (!e.getMessage().contains("no protocol")) {
                try {
                    // in case new URL(source) throw e.g. unknown protocol: hdfs, because of missing jar,
                    // we retrieve the related enum and throw the associated MissingDependencyException(..)
                    // otherwise we return unknown protocol: yyyyy
                    return SupportedProtocolsExtended.valueOf(new URI(source).getScheme());
                } catch (Exception ignored) {
                }

                // in case a Windows user write an url like `C:/User/...`
                if (e.getMessage().contains("unknown protocol") && UtilExtended.isWindows()) {
                    throw new RuntimeException(e.getMessage()
                            + "\n Please note that for Windows absolute paths they have to be explicit by prepending `file:` or supplied without the drive, "
                            + "\n e.g. `file:C:/my/path/file` or `/my/path/file`, instead of `C:/my/path/file`");
                }
                throw new RuntimeException(e);
            }
            return SupportedProtocolsExtended.file;
        }
    }

    public static final String ERROR_READ_FROM_FS_NOT_ALLOWED = "Import file %s not enabled, please set "
            + APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM + "=true in your neo4j.conf";
    public static final String ACCESS_OUTSIDE_DIR_ERROR =
            "You're providing a directory outside the import directory " + "defined into `server.directories.import`";

    public static CountingReaderExtended readerFor(Object input, String compressionAlgo, URLAccessChecker urlAccessChecker)
            throws IOException, URISyntaxException, URLAccessValidationError {
        return readerFor(input, null, null, compressionAlgo, urlAccessChecker);
    }

    public static CountingReaderExtended readerFor(
            Object input,
            Map<String, Object> headers,
            String payload,
            String compressionAlgo,
            URLAccessChecker urlAccessChecker)
            throws IOException, URISyntaxException, URLAccessValidationError {
        return inputStreamFor(input, headers, payload, compressionAlgo, urlAccessChecker)
                .asReader();
    }

    public static CountingInputStreamExtended inputStreamFor(
            Object input,
            Map<String, Object> headers,
            String payload,
            String compressionAlgo,
            URLAccessChecker urlAccessChecker)
            throws IOException, URISyntaxException, URLAccessValidationError {
        if (input == null) return null;
        if (input instanceof String) {
            String fileName = (String) input;
            fileName = changeFileUrlIfImportDirectoryConstrained(fileName, urlAccessChecker);
            return UtilExtended.openInputStream(fileName, headers, payload, compressionAlgo, urlAccessChecker);
        } else if (input instanceof byte[]) {
            return getInputStreamFromBinary((byte[]) input, compressionAlgo);
        } else {
            throw new RuntimeException(ERROR_BYTES_OR_STRING);
        }
    }

    public static String changeFileUrlIfImportDirectoryConstrained(String url, URLAccessChecker urlAccessChecker)
            throws IOException, URLAccessValidationError {
        extendedApocConfig().checkReadAllowed(url, urlAccessChecker);
        if (isFile(url) && isImportUsingNeo4jConfig()) {
            if (!extendedApocConfig().getBoolean(APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM)) {
                throw new RuntimeException(String.format(ERROR_READ_FROM_FS_NOT_ALLOWED, url));
            }
            getLoadFileUrl(url, urlAccessChecker);
            final Path resolvedPath = resolvePath(url);
            return resolvedPath.normalize().toUri().toString();
        }
        return url;
    }

    private static Path resolvePath(String url) throws IOException {
        Path urlPath = getPath(url);
        final Path resolvedPath;
        if (extendedApocConfig().isImportFolderConfigured() && isImportUsingNeo4jConfig()) {
            Path basePath = Paths.get(extendedApocConfig().getImportDir());
            urlPath = relativizeIfSamePrefix(urlPath, basePath);
            resolvedPath = basePath.resolve(urlPath).toAbsolutePath().normalize();
            if (!pathStartsWithOther(resolvedPath, basePath)) {
                throw new IOException(ACCESS_OUTSIDE_DIR_ERROR);
            }
        } else {
            resolvedPath = urlPath;
        }
        return resolvedPath;
    }

    private static Path relativizeIfSamePrefix(Path urlPath, Path basePath) {
        if (FilenameUtils.getPrefixLength(urlPath.toString()) > 0 && !urlPath.startsWith(basePath.toAbsolutePath())) {
            // if the import folder is configured to be used as root folder we consider
            // it as root directory in order to reproduce the same LOAD CSV behaviour
            urlPath = urlPath.getRoot().relativize(urlPath);
        }
        return urlPath;
    }

    private static Path getPath(String url) {
        Path urlPath;
        URL toURL = null;
        try {
            final URI uri = URI.create(url.trim()).normalize();
            toURL = uri.toURL();
            urlPath = Paths.get(uri);
        } catch (Exception e) {
            if (toURL != null) {
                urlPath = Paths.get(StringUtils.isBlank(toURL.getFile()) ? toURL.getHost() : toURL.getFile());
            } else {
                urlPath = Paths.get(url);
            }
        }
        return urlPath;
    }

    private static boolean pathStartsWithOther(Path resolvedPath, Path basePath) throws IOException {
        try {
            return resolvedPath.toFile().getCanonicalFile().toPath().startsWith(basePath.toRealPath());
        } catch (Exception e) {
            if (e instanceof NoSuchFileException) { // If we're about to create a file this exception has been thrown
                return resolvedPath.toFile().getCanonicalFile().toPath().startsWith(basePath);
            }
            return false;
        }
    }

    public static boolean isFile(String fileName) {
        return from(fileName) == SupportedProtocolsExtended.file;
    }

    public static OutputStream getOutputStream(String fileName) {
        return getOutputStream(fileName, ExportConfigExtended.EMPTY);
    }

    public static OutputStream getOutputStream(String fileName, ExportConfigExtended config) {
        if (fileName.equals("-")) {
            return null;
        }
        return getOutputStream(from(fileName), fileName, config);
    }

    public static OutputStream getOutputStream(SupportedProtocolsExtended protocol, String fileName, ExportConfigExtended config) {
        if (fileName == null) return null;
        final CompressionAlgoExtended compressionAlgo = CompressionAlgoExtended.valueOf(config.getCompressionAlgo());
        final OutputStream outputStream;
        try {
            switch (protocol) {
                case s3 -> outputStream = S3UploadUtilsExtended.writeFile(fileName);
                case hdfs -> outputStream = HDFSUtilsExtended.writeFile(fileName);
                default -> {
                    final File file = isImportUsingNeo4jConfig()
                            ? resolvePath(fileName).toFile()
                            : new File(getFileUrl(fileName));
                    outputStream = new FileOutputStream(file);
                }
            }
            return new BufferedOutputStream(compressionAlgo.getOutputStream(outputStream));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isImportUsingNeo4jConfig() {
        return extendedApocConfig().getBoolean(ExtendedApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG);
    }

    public static StreamConnectionExtended openS3InputStream(String urlAddress) throws IOException {
        if (!SupportedProtocolsExtended.s3.isEnabled()) {
            throw new MissingDependencyExceptionExtended(
                    "Cannot find the S3 jars in the plugins folder. \n"
                            + "Please put these files into the plugins folder :\n\n"
                            + "aws-java-sdk-core-x.y.z.jar\n"
                            + "aws-java-sdk-s3-x.y.z.jar\n"
                            + "httpclient-x.y.z.jar\n"
                            + "httpcore-x.y.z.jar\n"
                            + "joda-time-x.y.z.jar\n"
                            + "\nSee the documentation: https://neo4j.com/docs/apoc/current/import/web-apis/#_using_google_cloud_storage");
        }
        return S3URLConnectionExtended.openS3InputStream(new URL(urlAddress));
    }

    public static StreamConnectionExtended openHdfsInputStream(String urlAddress) throws IOException {
        if (!SupportedProtocolsExtended.hdfs.isEnabled()) {
            throw new MissingDependencyExceptionExtended(
                    "Cannot find the HDFS/Hadoop jars in the plugins folder. \n"
                            + "\nPlease, see the documentation: https://neo4j.com/docs/apoc/current/import/web-apis/#_using_google_cloud_storage");
        }
        return HDFSUtilsExtended.readFile(new URL(urlAddress));
    }

    /**
     * @return a File pointing to Neo4j's log directory, if it exists and is readable, null otherwise.
     */
    public static File getLogDirectory() {
        String neo4jHome = extendedApocConfig().getString("server.directories.neo4j_home", "");
        String logDir = extendedApocConfig().getString("server.directories.logs", "");

        File logs = logDir.isEmpty() ? new File(neo4jHome, "logs") : new File(logDir);

        if (logs.exists() && logs.canRead() && logs.isDirectory()) {
            return logs;
        }

        return null;
    }

    public static CountingInputStreamExtended getInputStreamFromBinary(byte[] urlOrBinary, String compressionAlgo) {
        return CompressionAlgoExtended.valueOf(compressionAlgo).toInputStream(urlOrBinary);
    }
}
