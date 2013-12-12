package kafka.api;

public class PartitionFetchInfo {
    public long offset;
    public int fetchSize;

    public PartitionFetchInfo(long offset, int fetchSize) {
        this.offset = offset;
        this.fetchSize = fetchSize;
    }
}
