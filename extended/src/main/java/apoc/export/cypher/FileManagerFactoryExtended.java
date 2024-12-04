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
package apoc.export.cypher;

import apoc.export.util.ExportConfigExtended;
import apoc.util.UtilExtended;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static apoc.util.FileUtilsExtended.getOutputStream;

/**
 * @author mh
 * @since 06.12.17
 */
public class FileManagerFactoryExtended {
    private static final String DOT = ".";

    public static ExportFileManagerExtended createFileManager(String fileName, boolean separatedFiles) {
        return createFileManager(fileName, separatedFiles, ExportConfigExtended.EMPTY);
    }

    public static ExportFileManagerExtended createFileManager(String fileName, boolean separatedFiles, ExportConfigExtended config) {
        if (fileName == null || "".equals(fileName)) {
            return new StringExportCypherFileManager(separatedFiles, config);
        }
        fileName = fileName.trim();

        String fileType = FilenameUtils.getExtension(fileName);
        return new PhysicalExportFileManager(fileType, fileName, separatedFiles, config);
    }

    private static class PhysicalExportFileManager implements ExportFileManagerExtended {

        private final String fileName;
        private final String fileType;
        private final boolean separatedFiles;
        private final Map<String, PrintWriter> writerCache;
        private ExportConfigExtended config;

        public PhysicalExportFileManager(
                String fileType, String fileName, boolean separatedFiles, ExportConfigExtended config) {
            this.fileType = StringUtils.isBlank(fileType) ? "" : fileType;
            this.fileName = fileName;
            this.separatedFiles = separatedFiles;
            this.config = config;
            this.writerCache = new ConcurrentHashMap<>();
        }

        @Override
        public PrintWriter getPrintWriter(String type) {
            String newFileName =
                    this.separatedFiles ? normalizeFileName(fileName, type) : normalizeFileName(fileName, null);
            return writerCache.computeIfAbsent(newFileName, (key) -> {
                OutputStream outputStream = getOutputStream(newFileName, config);
                return outputStream == null ? null : new PrintWriter(outputStream);
            });
        }

        @Override
        public StringWriter getStringWriter(String type) {
            return null;
        }

        private String normalizeFileName(final String fileName, String suffix) {
            if (StringUtils.isBlank(suffix)) {
                return fileName;
            }
            // in case of file without dot extension, we just add the suffix
            if (StringUtils.isEmpty(fileType)) {
                return fileName + DOT + suffix;
            }
            // TODO check if this should be follow the same rules of FileUtils.readerFor
            return fileName.replace(fileType, suffix + DOT + fileType);
        }

        @Override
        public String drain(String type) {
            return null;
        }

        @Override
        public Boolean separatedFiles() {
            return this.separatedFiles;
        }
    }

    private static class StringExportCypherFileManager implements ExportFileManagerExtended {

        private final boolean separatedFiles;
        private final ExportConfigExtended config;
        private final ConcurrentMap<String, StringWriter> writers = new ConcurrentHashMap<>();

        public StringExportCypherFileManager(boolean separatedFiles, ExportConfigExtended config) {
            this.separatedFiles = separatedFiles;
            this.config = config;
        }

        @Override
        public PrintWriter getPrintWriter(String type) {
            if (!this.separatedFiles) {
                switch (type) {
                    case "csv":
                    case "json":
                    case "graphml":
                        break;
                    default:
                        type = "cypher";
                }
            }
            return new PrintWriter(getStringWriter(type));
        }

        @Override
        public StringWriter getStringWriter(String type) {
            return writers.computeIfAbsent(type, (key) -> new StringWriter());
        }

        @Override
        public synchronized Object drain(String type) {
            StringWriter writer = writers.get(type);
            if (writer != null) {
                return UtilExtended.getStringOrCompressedData(writer, config);
            } else return null;
        }

        @Override
        public Boolean separatedFiles() {
            return this.separatedFiles;
        }
    }
}
