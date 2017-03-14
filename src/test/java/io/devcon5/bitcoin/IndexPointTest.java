package io.devcon5.bitcoin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.ByteBuffer;

import org.junit.Test;

/**
 *
 */
public class IndexPointTest {

    @Test
    public void of_Bod() throws Exception {
        ByteBuffer buf = ByteBuffer.wrap("1234,0.123,0.456\nxxx".getBytes());
        BufferedOffsetDatapoint bod = new BufferedOffsetDatapoint(16, buf);
        IndexPoint ip = IndexPoint.of(bod);

        assertNotNull(ip);
        assertEquals(1234L, ip.getTimestamp());
        assertEquals(16L, ip.getOffset()); //1024 + 16
    }

    @Test
    public void of_initialOffsetAndBod() throws Exception {
        ByteBuffer buf = ByteBuffer.wrap("1234,0.123,0.456\nxxx".getBytes());
        BufferedOffsetDatapoint bod = new BufferedOffsetDatapoint(16, buf);
        IndexPoint ip = IndexPoint.of(1024,bod);

        assertNotNull(ip);
        assertEquals(1234L, ip.getTimestamp());
        assertEquals(1040L, ip.getOffset()); //1024 + 16
    }

    @Test
    public void getOffset() throws Exception {

        IndexPoint ip = new IndexPoint(1234L, 5678L);
        assertEquals(5678L, ip.getOffset());
    }

    @Test
    public void getTimestamp() throws Exception {
        IndexPoint ip = new IndexPoint(1234L, 5678L);
        assertEquals(1234L, ip.getTimestamp());
    }

}
