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
package apoc.load;

import org.apache.commons.lang3.ArrayUtils;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * filter a inputstream and skip leading whitespace on each line
 */
class SkipWhitespaceInputStream extends FilterInputStream {

    boolean onlyWhitespaceSoFar;
    final byte[] blacklist = {0x0a, 0x0d, 0x09, 0x20};

    public SkipWhitespaceInputStream(InputStream inputStream) {
        super(inputStream);
        onlyWhitespaceSoFar = true;
    }

    @Override
    public int read() throws IOException {
        int read;
        do {
            read=super.read();
        } while (shouldSkipByte((byte) read));
        return read;
    }

    private boolean shouldSkipByte(byte b) {
        boolean shouldSkip = false;
        if (ArrayUtils.contains(blacklist, b)) {
            if (b == 0x0a) { // drop LF and flag beginning of new line
                onlyWhitespaceSoFar = true;
                shouldSkip = true;
            } if (b== 0x0d) { // drop CR unconditionally
                shouldSkip = true;
            } else {  // other whitespace: skip if it occurs at beginning of line
                if (onlyWhitespaceSoFar) {
                    shouldSkip = true;
                }
            }
        } else {
            onlyWhitespaceSoFar = false;
        }
        return shouldSkip;
    }

    @Override
    public int read(byte[] buffer, int offset, int len) throws IOException {
        byte[] localBuffer = new byte[len];
        int bytesRead = super.read(localBuffer, 0, len);
        if (bytesRead == -1 ) return -1;
        int bufferIndex = offset;

        for (int index=0; index < bytesRead; index++) {
            byte current = localBuffer[index];
            if (!shouldSkipByte(current)) {
                buffer[bufferIndex++] = current;
            }
        }
        return bufferIndex - offset;
    }
}
