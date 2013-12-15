package kafka.log;

import java.nio.ByteBuffer;

public abstract class OffsetMap {
    public abstract int slots();

    public abstract void put(ByteBuffer key, long offset);

    public abstract long get(ByteBuffer key);

    public abstract void clear();

    public abstract int size();

    public double utilization() {
        return (double) size() / slots();
    }
}
