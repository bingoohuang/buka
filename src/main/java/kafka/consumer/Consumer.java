package kafka.consumer;

public class Consumer {
    /**
     * Create a ConsumerConnector
     *
     * @param config at the minimum, need to specify the groupid of the consumer and the zookeeper
     *               connection string zookeeper.connect.
     */
    public static ConsumerConnector create(ConsumerConfig config) {
        return new ZookeeperConsumerConnector(config);
    }

}
