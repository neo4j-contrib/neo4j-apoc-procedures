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
