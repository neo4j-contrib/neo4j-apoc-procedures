package apoc.export.parquet;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.io.OutputStream;

public record ParquetBufferedWriter(OutputStream out) implements OutputFile {

    @Override
    public PositionOutputStream create(long blockSizeHint) {
        return createPositionOutputstream();
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
        return createPositionOutputstream();
    }

    private PositionOutputStream createPositionOutputstream() {
        return new PositionOutputStream() {

            int pos = 0;

            @Override
            public long getPos() {
                return pos;
            }

            @Override
            public void flush() throws IOException {
                out.flush();
            }

            @Override
            public void close() throws IOException {
                out.close();
            }

            @Override
            public void write(int b) throws IOException {
                out.write(b);
                pos++;
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                out.write(b, off, len);
                pos += len;
            }
        };
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }
}
