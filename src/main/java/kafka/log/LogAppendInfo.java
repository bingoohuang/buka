package kafka.log;

import kafka.message.CompressionCodec;

public class LogAppendInfo {
    public Long firstOffset;
    public Long lastOffset;
    public CompressionCodec codec;
    public Integer shallowCount;
    public Boolean offsetsMonotonic;

    /** Struct to hold various quantities we compute about each message set before appending to the log
     * @param firstOffset The first offset in the message set
     * @param lastOffset The last offset in the message set
     * @param codec The codec used in the message set
     * @param offsetsMonotonic Are the offsets in this message set monotonically increasing
     */
    public LogAppendInfo(Long firstOffset, Long lastOffset, CompressionCodec codec, Integer shallowCount, Boolean offsetsMonotonic) {
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

        if (codec != null ? !codec.equals(that.codec) : that.codec != null) return false;
        if (firstOffset != null ? !firstOffset.equals(that.firstOffset) : that.firstOffset != null) return false;
        if (lastOffset != null ? !lastOffset.equals(that.lastOffset) : that.lastOffset != null) return false;
        if (offsetsMonotonic != null ? !offsetsMonotonic.equals(that.offsetsMonotonic) : that.offsetsMonotonic != null)
            return false;
        if (shallowCount != null ? !shallowCount.equals(that.shallowCount) : that.shallowCount != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = firstOffset != null ? firstOffset.hashCode() : 0;
        result = 31 * result + (lastOffset != null ? lastOffset.hashCode() : 0);
        result = 31 * result + (codec != null ? codec.hashCode() : 0);
        result = 31 * result + (shallowCount != null ? shallowCount.hashCode() : 0);
        result = 31 * result + (offsetsMonotonic != null ? offsetsMonotonic.hashCode() : 0);
        return result;
    }
}
