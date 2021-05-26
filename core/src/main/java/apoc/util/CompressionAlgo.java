package apoc.util;

import apoc.export.util.CountingInputStream;
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
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;

import static apoc.ApocConfig.apocConfig;

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

    private OutputStream getOutputStream(ByteArrayOutputStream stream) throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        return compressor == null ? stream : (OutputStream) compressor.getConstructor(OutputStream.class).newInstance(stream);
    }

    public String decompress(byte[] byteArray, Charset charset) throws Exception {
        try (ByteArrayInputStream stream = new ByteArrayInputStream(byteArray);
                InputStream inputStream = getInputStream(stream)) {
            return IOUtils.toString(inputStream, charset);
        }
    }

    private InputStream getInputStream( ByteArrayInputStream stream) throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        return decompressor == null ? stream : (InputStream) decompressor.getConstructor(InputStream.class).newInstance(stream);
    }

    public CountingInputStream toInputStream(byte[] data) {
        apocConfig().isImportFileEnabled();

        try {
            ByteArrayInputStream stream = new ByteArrayInputStream(data);
            InputStream inputStream = getInputStream(stream);
            return new CountingInputStream(inputStream, stream.available());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
