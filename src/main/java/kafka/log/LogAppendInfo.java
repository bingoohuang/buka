package kafka.log;

import kafka.message.CompressionCodec;

public class LogAppendInfo {
    public long firstOffset;
    public long lastOffset;
    public CompressionCodec codec;
    public int shallowCount;
    public boolean offsetsMonotonic;

    /** Struct to hold various quantities we compute about each message set before appending to the log
     * @param firstOffset The first offset in the message set
     * @param lastOffset The last offset in the message set
     * @param codec The codec used in the message set
     * @param offsetsMonotonic Are the offsets in this message set monotonically increasing
     */
    public LogAppendInfo(long firstOffset, long lastOffset, CompressionCodec codec, int shallowCount, boolean offsetsMonotonic) {
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.codec = codec;
        this.shallowCount = shallowCount;
        this.offsetsMonotonic = offsetsMonotonic;
    }

    @Override
    public String toString() {
        return "LogAppendInfo{" +
                "firstOffset=" + firstOffset +
                ", lastOffset=" + lastOffset +
                ", codec=" + codec +
                ", shallowCount=" + shallowCount +
                ", offsetsMonotonic=" + offsetsMonotonic +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LogAppendInfo that = (LogAppendInfo) o;

        if (firstOffset != that.firstOffset) return false;
        if (lastOffset != that.lastOffset) return false;
        if (offsetsMonotonic != that.offsetsMonotonic) return false;
        if (shallowCount != that.shallowCount) return false;
        if (codec != null ? !codec.equals(that.codec) : that.codec != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (firstOffset ^ (firstOffset >>> 32));
        result = 31 * result + (int) (lastOffset ^ (lastOffset >>> 32));
        result = 31 * result + (codec != null ? codec.hashCode() : 0);
        result = 31 * result + shallowCount;
        result = 31 * result + (offsetsMonotonic ? 1 : 0);
        return result;
    }
}
