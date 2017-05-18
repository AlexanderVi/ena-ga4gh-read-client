package uk.ac.ebi.ena.sra.client;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Created by vadim on 18/05/2017.
 */
public class TestLimitedInputStream {

    @Test
    public void testEmpty() throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(new byte[0]);
        LimitedInputStream lais = new LimitedInputStream(bais, 0);
        Assert.assertEquals(lais.read(), -1);
    }

    @Test(expected = IOException.class)
    public void testIncomplete() throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(new byte[0]);
        LimitedInputStream lais = new LimitedInputStream(bais, 1);
        lais.read();
    }

    @Test
    public void testLimit() throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream("123".getBytes());
        LimitedInputStream lais = new LimitedInputStream(bais, 1);
        Assert.assertEquals(lais.read(), '1');
        Assert.assertEquals(lais.read(), -1);
    }
}
