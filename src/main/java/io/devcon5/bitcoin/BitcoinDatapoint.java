package io.devcon5.bitcoin;

/**
 * Represents a datapoint at a specific point in time conting the price and the volume at that time.
 */
public class BitcoinDatapoint {

    private final long timestamp;
    private final double price;
    private final double volume;

    public BitcoinDatapoint(final long timestamp, final double price, final double volume) {

        this.timestamp = timestamp;
        this.price = price;
        this.volume = volume;
    }

    public long getTimestamp() {

        return timestamp;
    }

    public double getPrice() {

        return price;
    }

    public double getVolume() {

        return volume;
    }

    @Override
    public String toString() {

        final StringBuilder sb = new StringBuilder("BitcoinDatapoint{");
        sb.append("timestamp=").append(timestamp);
        sb.append(", price=").append(price);
        sb.append(", volume=").append(volume);
        sb.append('}');
        return sb.toString();
    }
}
