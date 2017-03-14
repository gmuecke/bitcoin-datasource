package io.devcon5.bitcoin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.nio.ByteBuffer;

import org.junit.Test;

/**
 *
 */
public class BufferedOffsetDatapointTest {


    @Test
    public void getOffset() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        BufferedOffsetDatapoint subject = new BufferedOffsetDatapoint(1024, buffer);
        assertEquals(1024, subject.getOffset());
    }

    @Test
    public void getBuffer() throws Exception {

        ByteBuffer buffer = ByteBuffer.allocate(16);
        BufferedOffsetDatapoint subject = new BufferedOffsetDatapoint(0, buffer);
        assertSame(buffer, subject.getBuffer());
    }

}
