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

import java.io.InputStream;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;

public enum ArchiveType {
    NONE(null, null),
    ZIP(ZipArchiveInputStream.class, CompressionAlgo.NONE),
    TAR(TarArchiveInputStream.class, CompressionAlgo.NONE),
    TAR_GZ(TarArchiveInputStream.class, CompressionAlgo.GZIP);

    private final Class<?> stream;
    private final CompressionAlgo algo;

    ArchiveType(Class<?> stream, CompressionAlgo algo) {
        this.stream = stream;
        this.algo = algo;
    }

    public static ArchiveType from(String urlAddress) {
        if (!urlAddress.contains("!")) {
            return NONE;
        }
        if (urlAddress.contains(".zip")) {
            return ZIP;
        } else if (urlAddress.contains(".tar.gz") || urlAddress.contains(".tgz")) {
            return TAR_GZ;
        } else if (urlAddress.contains(".tar")) {
            return TAR;
        }
        return NONE;
    }

    public ArchiveInputStream getInputStream(InputStream is) {
        try {
            final InputStream compressionStream = algo.getInputStream(is);
            return (ArchiveInputStream) stream.getConstructor(InputStream.class).newInstance(compressionStream);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isArchive() {
        return stream != null;
    }
}
