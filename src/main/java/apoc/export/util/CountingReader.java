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
