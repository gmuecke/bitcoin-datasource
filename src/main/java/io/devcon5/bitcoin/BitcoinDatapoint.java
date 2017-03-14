package io.devcon5.bitcoin;

import java.nio.ByteBuffer;

/**
 * A single datapoint.
 */
public class BitcoinDatapoint {

    private final long timestamp;
    private final float price;
    private final float volume;

    public BitcoinDatapoint(final long timestamp, final float price, final float volume) {
        this.timestamp = timestamp;
        this.price = price;
        this.volume = volume;
    }

    public static BitcoinDatapoint fromBuffer(final ByteBuffer buffer, final int priceSeparator, final int volumeSeparator) {

        int volumeEnd = buffer.limit(); //subtract 2 for the 2x','

        byte[] tsData = new byte[priceSeparator];
        byte[] pData = new byte[volumeSeparator - priceSeparator - 1];
        byte[] vData = new byte[volumeEnd - volumeSeparator - 1];

        buffer.get(tsData);
        buffer.get();
        buffer.get(pData);
        buffer.get();
        buffer.get(vData);

        long ts = Long.parseLong(new String(tsData)) * 1000;
        float price = Float.parseFloat(new String(pData));
        float volume = Float.parseFloat(new String(vData));

        return new BitcoinDatapoint(ts, price, volume);
    }

    /**
     *
     * @param bod
     *  the BufferedOffsetDatapoint to parse the data. The buffer of the BOD must start at the beginning of the
     *  actual data (no lead-in) and must end at some point with the '\n' (newline) character. The remainder
     *  is ignored.
     * @return
     */
    public static BitcoinDatapoint fromOffsetBuffer(BufferedOffsetDatapoint bod){

        ByteBuffer buf = bod.getBuffer();
        int priceSeparator = -1;
        int volumeSeparator = -1;
        int begin = buf.position();
        byte c = -1;

        /*
            search the separators. There are two separators ','
             - between timestamp and price
             - between price and volume
             as the separator positions will be used relative to the start of the line, the offset between
             the initial offset and the beginning of the line is substracted
         */
        buf.mark();
        while(buf.hasRemaining() && (c = buf.get()) != '\n'){
            if(c == ','){
                if(priceSeparator == -1 ){
                    priceSeparator = buf.position() -1;
                } else if(volumeSeparator == -1){
                    volumeSeparator = buf.position() -1;
                }
            }
        }
        int end = buf.position() - (c == '\n' ? 1 : 0);
        buf.reset();

        return fromBuffer(ByteBuffer.wrap(buf.array(), begin, end-begin), priceSeparator, volumeSeparator);
    }

    /**
     * The timestamp of this datapoint.
     *
     * @return
     */
    public long getTimestamp() {

        return timestamp;
    }

    /**
     * The price information of the datapoint
     *
     * @return
     */
    public float getPrice() {

        return price;
    }

    /**
     * The transaction volume of the datapoint
     *
     * @return
     */
    public float getVolume() {

        return volume;
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
