package uk.ac.ebi.ena.sra.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;

/**
 * Created by vadim on 12/05/2016.
 */
class MultiInputStream extends InputStream {
    LinkedList<InputStream> streams = new LinkedList<>();
    long counter = 0;

    public MultiInputStream(LinkedList<InputStream> streams) {
        this.streams = streams;
    }

    @Override
    public int read() throws IOException {
        if (streams.isEmpty()) {
            return -1;
        }

        int read = streams.getFirst().read();
        counter++;
        if (read == -1) {
            streams.removeFirst();
            counter = 0;
            return read();
        }
        return read;
    }
}
