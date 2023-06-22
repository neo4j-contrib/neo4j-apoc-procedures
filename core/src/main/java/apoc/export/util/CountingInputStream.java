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

import org.apache.commons.compress.utils.SeekableInMemoryByteChannel;
import org.apache.commons.io.input.BOMInputStream;

import java.io.*;
import java.nio.channels.SeekableByteChannel;

import static apoc.export.util.LimitedSizeInputStream.toLimitedIStream;

/**
 * @author mh
 * @since 22.05.16
 */
public class CountingInputStream extends FilterInputStream implements SizeCounter {

    public static final int BUFFER_SIZE = 1024 * 1024;
    private final long total;
    private long count=0;
    private long newLines;

    public CountingInputStream(InputStream stream, long total) {
        super(toBufferedStream(stream, total));
        this.total = total;
    }

    private static BufferedInputStream toBufferedStream(InputStream stream, long total) {
        final BOMInputStream bomInputStream = new BOMInputStream(stream);

        InputStream sizeInputStream = toLimitedIStream(bomInputStream, total);
        return new BufferedInputStream(sizeInputStream, BUFFER_SIZE);
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        int read = super.read(buf, off, len);
        count+=read;
        for (int i=off;i<off+len;i++) {
            if (buf[i] == '\n') newLines++;
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

    public InputStream getStream() {
	   return in;
    }

	public CountingReader asReader() throws IOException {
		Reader reader = new InputStreamReader(in,"UTF-8");
        return new CountingReader(reader,total);
	}

    public SeekableByteChannel asChannel() throws IOException {
        return new SeekableInMemoryByteChannel(this.readAllBytes());
    }
}
