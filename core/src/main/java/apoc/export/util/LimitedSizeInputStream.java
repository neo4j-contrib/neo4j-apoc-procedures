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
package apoc.export.util;

import java.io.IOException;
import java.io.InputStream;

public class LimitedSizeInputStream extends InputStream {
    public static final String SIZE_EXCEEDED_ERROR = "The file dimension exceeded maximum size in bytes, %s,\n" +
                                     "which is %s times the width of the original file.\n" +
                                     "The InputStream has been blocked because the file could be a compression bomb attack.";

    public static final int SIZE_MULTIPLIER = 100;

    private final InputStream stream;
    private final long maxSize;
    private long total;

    public LimitedSizeInputStream(InputStream stream, long maxSize) {
        this.stream = stream;
        this.maxSize = maxSize;
    }

    @Override
    public int read() throws IOException {
        int i = stream.read();
        if (i >= 0) incrementCounter(1);
        return i;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        int i = stream.read(b, off, len);
        if (i >= 0) incrementCounter(i);
        return i;
    }

    private void incrementCounter(int size) throws IOException {
        // in some test cases, e.g. UtilIT.redirectShouldWorkWhenProtocolNotChangesWithUrlLocation,
        // the StreamConnection.getLength() returns `-1` because of content length not known,
        // therefore we skip these cases
        if (maxSize < 0) {
            return;
        }
        total += size;
        if (total > maxSize) {
            close();
            String msgError = String.format(SIZE_EXCEEDED_ERROR,
                    maxSize, SIZE_MULTIPLIER);
            throw new IOException(msgError);
        }
    }

    public static InputStream toLimitedIStream(InputStream stream, long total) {
        // to prevent potential bomb attack
        return new LimitedSizeInputStream(stream, total * SIZE_MULTIPLIER);
    }

}
