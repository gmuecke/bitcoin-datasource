package io.devcon5.bitcoin;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

/**
 * A single datapoint.
 */
public class BitcoinDatapoint {

    private final long offset;
    private final Buffer buffer;
    private final int delim1;
    private final int delim2;

    public BitcoinDatapoint(final long offset, final Buffer buffer, final int delim1, final int delim2) {

        this.offset = offset;
        this.buffer = buffer;
        this.delim1 = delim1;
        this.delim2 = delim2;
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
     * Creates a Bitcoin BitcoinDatapoint from this datapoint
     *
     * @return
     */
    public JsonObject toJson() {
        return new JsonObject().put("ts", getTimestamp()).put("price", getPrice()).put("volume", getVolume());
    }

    /**
     * The timestamp of this datapoint.
     *
     * @return
     */
    public long getTimestamp() {

        return Long.parseLong(buffer.getString(0, delim1)) * (delim1 == 10 ? 1000 : 1);
    }

    /**
     * The price information of the datapoint
     *
     * @return
     */
    public double getPrice() {

        return Double.parseDouble(buffer.getString(delim1 + 1, delim2));
    }

    /**
     * The transaction volume of the datapoint
     *
     * @return
     */
    public double getVolume() {

        return Double.parseDouble(buffer.getString(delim2 + 1, buffer.length()));
    }

    @Override
    public String toString() {

        final StringBuilder sb = new StringBuilder("BitcoinDatapoint{");
        sb.append("timestamp=").append(getTimestamp());
        sb.append(", price=").append(getPrice());
        sb.append(", volume=").append(getVolume());
        sb.append('}');
        return sb.toString();
    }
}
