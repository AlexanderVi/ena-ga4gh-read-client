package uk.ac.ebi.ena.sra.client;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by vadim on 12/05/2016.
 */
class LimitedInputStream extends InputStream {
    InputStream delegate;
    long size;
    long read = 0;

    public LimitedInputStream(InputStream delegate, long size) {
        this.delegate = delegate;
        this.size = size;
    }

    @Override
    public int read() throws IOException {
        if (read >= size) {
            return -1;
        }
        read++;
        return delegate.read();
    }

    @Override
    public String toString() {
        return String.format("Limited stream: size=%d, read=%d, delegate=%s", size, read, delegate);
    }
}
