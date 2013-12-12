package kafka.consumer;

import kafka.utils.VerifiableProperties;
import kafka.utils.ZKConfig;

import java.util.Properties;

public class ConsumerConfig extends ZKConfig {

    public ConsumerConfig(VerifiableProperties props) {
        super(props);
        init();
    }

    public ConsumerConfig(Properties originalProps) {
        this(new VerifiableProperties(originalProps));
        props.verify();
    }

    private void init() {
        groupId = props.getString("group.id");

        /** consumer id: generated automatically if not set.
         *  Set this explicitly for only testing purpose. */
        consumerId = props.getString("consumer.id", null);

        /** the socket timeout for network requests. The actual timeout set will be max.fetch.wait + socket.timeout.ms. */
        socketTimeoutMs = props.getInt("socket.timeout.ms", ConsumerConfigs.SocketTimeout);

        /** the socket receive buffer for network requests */
        socketReceiveBufferBytes = props.getInt("socket.receive.buffer.bytes", ConsumerConfigs.SocketBufferSize);

        /** the number of byes of messages to attempt to fetch */
        fetchMessageMaxBytes = props.getInt("fetch.message.max.bytes", ConsumerConfigs.FetchSize);

        /** if true, periodically commit to zookeeper the offset of messages already fetched by the consumer */
        autoCommitEnable = props.getBoolean("auto.commit.enable", ConsumerConfigs.AutoCommit);

        /** the frequency in ms that the consumer offsets are committed to zookeeper */
        autoCommitIntervalMs = props.getInt("auto.commit.interval.ms", ConsumerConfigs.AutoCommitInterval);

        /** max number of message chunks buffered for consumption, each chunk can be up to fetch.message.max.bytes*/
        queuedMaxMessages = props.getInt("queued.max.message.chunks", ConsumerConfigs.MaxQueuedChunks);

        /** max number of retries during rebalance */
        rebalanceMaxRetries = props.getInt("rebalance.max.retries", ConsumerConfigs.MaxRebalanceRetries);

        /** the minimum amount of data the server should return for a fetch request. If insufficient data is available the request will block */
        fetchMinBytes = props.getInt("fetch.min.bytes", ConsumerConfigs.MinFetchBytes);

        /** the maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy fetch.min.bytes */
        fetchWaitMaxMs = props.getInt("fetch.wait.max.ms", ConsumerConfigs.MaxFetchWaitMs);

        /** backoff time between retries during rebalance */
        rebalanceBackoffMs = props.getInt("rebalance.backoff.ms", zkSyncTimeMs);

        /** backoff time to refresh the leader of a partition after it loses the current leader */
        refreshLeaderBackoffMs = props.getInt("refresh.leader.backoff.ms", ConsumerConfigs.RefreshMetadataBackoffMs);

  /* what to do if an offset is out of range.
     smallest : automatically reset the offset to the smallest offset
     largest : automatically reset the offset to the largest offset
     anything else: throw exception to the consumer */
        autoOffsetReset = props.getString("auto.offset.reset", ConsumerConfigs.AutoOffsetReset);

        /** throw a timeout exception to the consumer if no message is available for consumption after the specified inter*/
        consumerTimeoutMs = props.getInt("consumer.timeout.ms", ConsumerConfigs.ConsumerTimeoutMs);

        /**
         * Client id is specified by the kafka consumer client, used to distinguish different clients
         */
        clientId = props.getString("client.id", groupId);

        ConsumerConfigs.validate(this);
    }

    /**
     * a string that uniquely identifies a set of consumers within the same consumer group
     */
    public String groupId;

    /**
     * consumer id: generated automatically if not set.
     * Set this explicitly for only testing purpose.
     */
    public String consumerId; // nullable

    /**
     * the socket timeout for network requests. The actual timeout set will be max.fetch.wait + socket.timeout.ms.
     */
    public int socketTimeoutMs;

    /**
     * the socket receive buffer for network requests
     */
    public int socketReceiveBufferBytes;

    /**
     * the number of byes of messages to attempt to fetch
     */
    public int fetchMessageMaxBytes;

    /**
     * if true, periodically commit to zookeeper the offset of messages already fetched by the consumer
     */
    public boolean autoCommitEnable;

    /**
     * the frequency in ms that the consumer offsets are committed to zookeeper
     */
    public int autoCommitIntervalMs;

    /**
     * max number of message chunks buffered for consumption, each chunk can be up to fetch.message.max.bytes
     */
    public int queuedMaxMessages;

    /**
     * max number of retries during rebalance
     */
    public int rebalanceMaxRetries;

    /**
     * the minimum amount of data the server should return for a fetch request. If insufficient data is available the request will block
     */
    public int fetchMinBytes;

    /**
     * the maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy fetch.min.bytes
     */
    public int fetchWaitMaxMs;

    /**
     * backoff time between retries during rebalance
     */
    public int rebalanceBackoffMs;

    /**
     * backoff time to refresh the leader of a partition after it loses the current leader
     */
    public int refreshLeaderBackoffMs;

    /* what to do if an offset is out of range.
       smallest : automatically reset the offset to the smallest offset
       largest : automatically reset the offset to the largest offset
       anything else: throw exception to the consumer */
    public String autoOffsetReset;

    /**
     * throw a timeout exception to the consumer if no message is available for consumption after the specified interpublic final int
     */
    public int consumerTimeoutMs;

    /**
     * Client id is specified by the kafka consumer client, used to distinguish different clients
     */
    public String clientId;

}
