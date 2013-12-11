package kafka.controller;

public class PartitionAndReplica {
    public final String topic;
    public final int partition;
    public final int replica;

    public PartitionAndReplica(String topic, int partition, int replica) {
        this.topic = topic;
        this.partition = partition;
        this.replica = replica;
    }

    @Override
    public String toString() {
        return "PartitionAndReplica{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", replica=" + replica +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PartitionAndReplica that = (PartitionAndReplica) o;

        if (partition != that.partition) return false;
        if (replica != that.replica) return false;
        if (topic != null ? !topic.equals(that.topic) : that.topic != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + partition;
        result = 31 * result + replica;
        return result;
    }
}
