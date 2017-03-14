package io.devcon5.bitcoin;

import java.nio.ByteBuffer;

/**
 * Represent a point in the datafile with a timestamp and a byte-offset. So when a timestamp is searched for,
 * the corresponding byte offset can be returned.
 */
class IndexPoint {

    final long timestamp;
    final long offset;

    public IndexPoint(final long timestamp, final long offset) {

        this.timestamp = timestamp;
        this.offset = offset;
    }

    public static IndexPoint of(final BufferedOffsetDatapoint dp) {
        return of(0L, dp);
    }
    public static IndexPoint of(long initialOffset, final BufferedOffsetDatapoint dp) {

        final ByteBuffer buf = dp.getBuffer();

        int start = buf.position();
        buf.mark();
        while (buf.hasRemaining() && buf.get() != ',') {
        }
        int end = buf.position() - 1;
        buf.reset();
        buf.mark();
        byte[] data = new byte[end-start];
        buf.get(data);
        buf.reset();

        long timestamp = Long.parseLong(new String(data)) * 1000;

        return new IndexPoint(timestamp, initialOffset + dp.getOffset());
    }

    /**
     * The byte-position offset in the source datafile. This is the global offset relative to the datafile and
     * not within the underlying buffer.
     *
     * @return the number of bytes this datapoint is located from the beginning of the datafile
     */
    public long getOffset() {

        return offset;
    }

    /**
     * Returns the timestamp associated with this index point.
     * @return
     *  the timestamp of this index point
     */
    public long getTimestamp() {

        return timestamp;
    }

    @Override
    public String toString() {

        final StringBuilder sb = new StringBuilder("IndexPoint{");
        sb.append("timestamp=").append(timestamp);
        sb.append(", offset=").append(offset);
        sb.append('}');
        return sb.toString();
    }
}
