package kafka.api;

public class PartitionOffsetRequestInfo {
    public long time;
    public int maxNumOffsets;

    public PartitionOffsetRequestInfo(long time, int maxNumOffsets) {
        this.time = time;
        this.maxNumOffsets = maxNumOffsets;
    }

    @Override
    public String toString() {
        return "PartitionOffsetRequestInfo{" +
                "time=" + time +
                ", maxNumOffsets=" + maxNumOffsets +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PartitionOffsetRequestInfo that = (PartitionOffsetRequestInfo) o;

        if (maxNumOffsets != that.maxNumOffsets) return false;
        if (time != that.time) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (time ^ (time >>> 32));
        result = 31 * result + maxNumOffsets;
        return result;
    }
}
