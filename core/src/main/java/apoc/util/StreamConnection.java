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

import apoc.export.util.CountingInputStream;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLConnection;
import java.util.zip.DeflaterInputStream;
import java.util.zip.GZIPInputStream;

/**
 * @author mh
 * @since 26.01.18
 */
public interface StreamConnection {
    InputStream getInputStream() throws IOException;
    String getEncoding();
    long getLength();
    String getName();

    default CountingInputStream toCountingInputStream(String algo) throws IOException {
        if ("gzip".equals(getEncoding()) || getName().endsWith(".gz")) {
            return new CountingInputStream(new GZIPInputStream(getInputStream()), getLength());
        }
        if ("deflate".equals(getName())) {
            return new CountingInputStream(new DeflaterInputStream(getInputStream()), getLength());
        }
        try {
            final InputStream inputStream = CompressionAlgo.valueOf(algo == null ? CompressionAlgo.NONE.name() : algo)
                    .getInputStream(getInputStream());
            return new CountingInputStream(inputStream, getLength());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static class UrlStreamConnection implements StreamConnection {
        private final URLConnection con;

        public UrlStreamConnection(URLConnection con) {
            this.con = con;
        }

        @Override
        public InputStream getInputStream() throws IOException {
            return con.getInputStream();
        }

        @Override
        public String getEncoding() {
            return con.getContentEncoding();
        }

        @Override
        public long getLength() {
            return con.getContentLength();
        }

        @Override
        public String getName() {
            return con.getURL().toString();
        }
    }

    class FileStreamConnection implements StreamConnection {
        public static final String CANNOT_OPEN_FILE_FOR_READING = "Cannot open file %s for reading.";
        private final File file;

        public FileStreamConnection(File file) throws IOException {
            this.file = file;
            if (!file.exists() || !file.isFile() || !file.canRead()) {
                throw new IOException(String.format(CANNOT_OPEN_FILE_FOR_READING, file.getAbsolutePath()));
            }
        }

        public FileStreamConnection(URI fileName) throws IOException {
            this(new File(fileName));
        }

        public FileStreamConnection(String fileName) throws IOException {
            this(new File(fileName));
        }

        @Override
        public InputStream getInputStream() throws IOException {
            return FileUtils.openInputStream(file);
        }

        @Override
        public String getEncoding() {
            return "UTF-8";
        }

        @Override
        public long getLength() {
            return file.length();
        }

        @Override
        public String getName() {
            return file.getName();
        }
    }
}
