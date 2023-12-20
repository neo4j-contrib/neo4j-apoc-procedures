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

import static apoc.ApocConfig.apocConfig;
import static apoc.export.util.LimitedSizeInputStream.toLimitedIStream;

import apoc.export.util.CountingInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorInputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorOutputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorInputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorOutputStream;
import org.apache.commons.io.IOUtils;

public enum CompressionAlgo {
    NONE(null, null),
    GZIP(GzipCompressorOutputStream.class, GzipCompressorInputStream.class),
    BZIP2(BZip2CompressorOutputStream.class, BZip2CompressorInputStream.class),
    DEFLATE(DeflateCompressorOutputStream.class, DeflateCompressorInputStream.class),
    BLOCK_LZ4(BlockLZ4CompressorOutputStream.class, BlockLZ4CompressorInputStream.class),
    FRAMED_SNAPPY(FramedSnappyCompressorOutputStream.class, FramedSnappyCompressorInputStream.class);

    private final Class<?> compressor;
    private final Class<?> decompressor;

    CompressionAlgo(Class<?> compressor, Class<?> decompressor) {
        this.compressor = compressor;
        this.decompressor = decompressor;
    }

    public byte[] compress(String string, Charset charset) throws Exception {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            try (OutputStream outputStream = getOutputStream(stream)) {
                outputStream.write(string.getBytes(charset));
            }
            return stream.toByteArray();
        }
    }

    public OutputStream getOutputStream(OutputStream stream) throws Exception {
        return isNone()
                ? stream
                : (OutputStream) compressor.getConstructor(OutputStream.class).newInstance(stream);
    }

    public String decompress(byte[] byteArray, Charset charset) throws Exception {
        try (ByteArrayInputStream stream = new ByteArrayInputStream(byteArray);
                InputStream inputStream = toLimitedIStream(getInputStream(stream), byteArray.length)) {
            return IOUtils.toString(inputStream, charset);
        }
    }

    public InputStream getInputStream(InputStream stream) throws Exception {
        return isNone()
                ? stream
                : (InputStream) decompressor.getConstructor(InputStream.class).newInstance(stream);
    }

    public boolean isNone() {
        return compressor == null;
    }

    public CountingInputStream toInputStream(byte[] data) {
        apocConfig().isImportFileEnabled();

        try {
            ByteArrayInputStream stream = new ByteArrayInputStream(data);
            InputStream inputStream = toLimitedIStream(getInputStream(stream), data.length);
            return new CountingInputStream(inputStream, stream.available());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
