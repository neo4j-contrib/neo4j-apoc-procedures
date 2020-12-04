package apoc.util;

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.nio.charset.Charset;
import java.util.List;

import static apoc.util.Util.convertFromBytesToList;
import static apoc.util.Util.convertFromListToBytes;

public enum CompressionAlgo {

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

    public List<Long> compress(String string, Charset charset) throws Exception {
        Constructor<?> constructor = compressor.getConstructor(OutputStream.class);
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            try (OutputStream outputStream = (OutputStream) constructor.newInstance((OutputStream) stream)) {
                outputStream.write(string.getBytes(charset));
            }
            return convertFromBytesToList(stream.toByteArray());
        }
    }

    public String decompress(List<Long> byteArray, Charset charset) throws Exception {
        try (ByteArrayInputStream stream = new ByteArrayInputStream(convertFromListToBytes(byteArray))) {
            Constructor<?> constructor = decompressor.getConstructor(InputStream.class);
            try (InputStream inputStream = (InputStream) constructor.newInstance((InputStream) stream)) {
                return IOUtils.toString(inputStream, charset);
            }
        }
    }
}
