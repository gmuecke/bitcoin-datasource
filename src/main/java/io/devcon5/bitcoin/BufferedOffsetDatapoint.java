package io.devcon5.bitcoin;

import java.nio.ByteBuffer;

/**
 * Represents a general datapoint at an offset position in a datasource. The datapoint is associated with a buffer
 * containing the raw data of that datapoint.
 */
public class BufferedOffsetDatapoint {

    /**
     * The offset position of the buffer in the original datafile
     */
    private final int offset;
    private final ByteBuffer buffer;

    public BufferedOffsetDatapoint(final int offset, final ByteBuffer byteBuffer) {

        this.offset = offset;
        this.buffer = byteBuffer;
    }

    /**
     * The global offset position to the beginning of the actual dataset
     * @return
     */
    public int getOffset() {

        return offset;
    }

    public ByteBuffer getBuffer() {

        return buffer;
    }
}
