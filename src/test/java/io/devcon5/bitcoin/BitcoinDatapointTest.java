package io.devcon5.bitcoin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.ByteBuffer;

import org.junit.Test;

/**
 *
 */
public class BitcoinDatapointTest {

    @Test
    public void fromBuffer() throws Exception {

        ByteBuffer buf = ByteBuffer.wrap("1234567890,1.234,5.678".getBytes());
        BitcoinDatapoint bd = BitcoinDatapoint.fromBuffer(buf,10, 16);

        assertNotNull(bd);
        assertEquals(1234567890L, bd.getTimestamp());
        assertEquals(1.234, bd.getPrice(), 0.01);
        assertEquals(5.678, bd.getVolume(), 0.01);

    }

    @Test
    public void fromOffsetBuffer() throws Exception {

        ByteBuffer buf = ByteBuffer.wrap("1234567890,1.234,5.678".getBytes());
        BufferedOffsetDatapoint bod = new BufferedOffsetDatapoint(0, buf);
        BitcoinDatapoint bd = BitcoinDatapoint.fromOffsetBuffer(bod);

        assertNotNull(bd);
        assertEquals(1234567890L, bd.getTimestamp());
        assertEquals(1.234, bd.getPrice(), 0.01);
        assertEquals(5.678, bd.getVolume(), 0.01);
    }

    @Test
    public void getTimestamp() throws Exception {

        BitcoinDatapoint bd = new BitcoinDatapoint(1234L, 1.3f, 2.6f);
        assertEquals(1234L, bd.getTimestamp());
    }

    @Test
    public void getPrice() throws Exception {
        BitcoinDatapoint bd = new BitcoinDatapoint(1234L, 1.3f, 2.6f);
        assertEquals(1.3f, bd.getPrice(), 0.01);
    }

    @Test
    public void getVolume() throws Exception {
        BitcoinDatapoint bd = new BitcoinDatapoint(1234L, 1.3f, 2.6f);
        assertEquals(2.6f, bd.getVolume(), 0.01);
    }

}
