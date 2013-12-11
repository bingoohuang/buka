package kafka.common;

import kafka.cluster.Partition;
import kafka.cluster.Replica;
import kafka.utils.Tuple2;

public class TopicAndPartition {
    public final String topic;
    public final int partition;

    public TopicAndPartition(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    public TopicAndPartition(Tuple2<String, Integer> tuple) {
        this(tuple._1, tuple._2);
    }

    public TopicAndPartition(Partition partition) {
        this(partition.topic, partition.partitionId);
    }

    public TopicAndPartition(Replica replica) {
        this(replica.topic, replica.partitionId);
    }

    public Tuple2<String, Integer> asTuple() {
        return Tuple2.make(topic, partition);
    }

    @Override
    public String toString() {
        return String.format("[%s,%d]", topic, partition);
    }
}
