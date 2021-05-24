package apoc.util;

import apoc.export.util.CountingInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorInputStream;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;

import static apoc.ApocConfig.apocConfig;

public enum BinaryFileType {
    
    BYTES(null),
    GZIP(GzipCompressorInputStream.class),
    BZIP2(BZip2CompressorInputStream.class),
    DEFLATE(DeflateCompressorInputStream.class),
    BLOCK_LZ4(BlockLZ4CompressorInputStream.class),
    FRAMED_SNAPPY(FramedSnappyCompressorInputStream.class);
   
    private final Class<?> decompressor;

    BinaryFileType(Class<?> decompressor) {
        this.decompressor = decompressor;
    }
    
    public CountingInputStream toInputStream(Object data, Charset charset) {
        apocConfig().isImportFileEnabled();
        
        byte[] binary;
        if (data instanceof String) {
            binary = ((String) data).getBytes(charset);
        } else if (data instanceof byte[]) {
            binary = (byte[]) data;
        } else {
            throw new RuntimeException("Invalid data, only byte[] and String are allowed");
        }
        
        try {
            final boolean isDecompressorNull = decompressor == null;
            
            ByteArrayInputStream stream = new ByteArrayInputStream(binary);
            InputStream inputStream = isDecompressorNull 
                    ? stream
                    : (InputStream) decompressor.getConstructor(InputStream.class).newInstance(stream);
            return new CountingInputStream(inputStream, stream.available());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
