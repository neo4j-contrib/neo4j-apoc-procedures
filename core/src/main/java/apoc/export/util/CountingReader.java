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

import java.io.*;

/**
 * @author mh
 * @since 22.05.16
 */
public class CountingReader extends FilterReader implements SizeCounter {
    public static final int BUFFER_SIZE = 1024 * 1024;
    private final long total;
    private long count=0;
    private long newLines;

    public CountingReader(File file) throws FileNotFoundException {
        super(new BufferedReader(new FileReader(file), BUFFER_SIZE));
        this.total = file.length();
    }
    public CountingReader(Reader reader, long total) throws FileNotFoundException {
        super(new BufferedReader(reader, BUFFER_SIZE));
        this.total = total;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        int read = super.read(cbuf, off, len);
        count+=read;
        for (int i=off;i<off+len;i++) {
            if (cbuf[i] == '\n') newLines++;
        }
        return read;
    }

    @Override
    public int read() throws IOException {
        count++;
        int read = super.read();
        if (read == '\n') newLines++;
        return read;
    }

    @Override
    public long skip(long n) throws IOException {
        count += n;
        return super.skip(n);
    }

    public long getCount() {
        return count;
    }

    public long getNewLines() {
        return newLines;
    }

    public long getTotal() {
        return total;
    }

    @Override
    public long getPercent() {
        if (total <= 0) return 0;
        return count*100 / total;
    }
}
