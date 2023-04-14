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

import java.io.StringWriter;
import java.util.Arrays;

public class FixedSizeStringWriter extends StringWriter {

    public static final int MAX_SIZE = 1000;

    private final int fixedSize;

    private boolean exceeded = false;

    public FixedSizeStringWriter() {
        this(MAX_SIZE);
    }

    public FixedSizeStringWriter(int maxSize) {
        super(maxSize);
        this.fixedSize = maxSize;
    }

    @Override
    public void write(int c) {
        throw new UnsupportedOperationException("Method unsupported");
    }

    @Override
    public void write(char cbuf[], int off, int len) {
        cbuf = Arrays.copyOfRange(cbuf, off, Math.min(off + len, cbuf.length));
        if (exceedFixedSize(cbuf)) {
            cbuf = Arrays.copyOf(cbuf, fixedSize - getBuffer().length());
        }
        super.write(cbuf, 0, cbuf.length);
    }

    private boolean exceedFixedSize(char[] cbuf) {
        exceeded = getBuffer().length() + cbuf.length > fixedSize;
        return exceeded;
    }

    @Override
    public void write(String str) {
        write(str.toCharArray(), 0, str.length());
    }

    @Override
    public void write(String str, int off, int len)  {
        write(str.toCharArray(), off, len);
    }

    public boolean isExceeded() {
        return exceeded;
    }
}
