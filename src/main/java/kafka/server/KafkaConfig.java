package kafka.server;

import kafka.consumer.ConsumerConfigs;
import kafka.message.MessageSets;
import kafka.message.Messages;
import kafka.utils.Range;
import kafka.utils.Utils;
import kafka.utils.VerifiableProperties;
import kafka.utils.ZKConfig;

import java.util.List;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;

/**
 * Configuration settings for the kafka server
 */
public class KafkaConfig extends ZKConfig {
    public VerifiableProperties props;

    public KafkaConfig(Properties originalProps) {
        this(new VerifiableProperties(originalProps));
        props.verify();
    }

    private long getLogRetentionTimeMillis() {
        long millisInMinute = 60L * 1000L;
        long millisInHour = 60L * millisInMinute;
        if (props.containsKey("log.retention.minutes")) {
            return millisInMinute * props.getIntInRange("log.retention.minutes", Range.make(1, Integer.MAX_VALUE));
        } else {
            return millisInHour * props.getIntInRange("log.retention.hours", 24 * 7, Range.make(1, Integer.MAX_VALUE));
        }
    }


    public KafkaConfig(VerifiableProperties props) {
        super(props);
        this.props = props;

        brokerId = props.getIntInRange("broker.id", Range.make(0, Integer.MAX_VALUE));
        messageMaxBytes = props.getIntInRange("message.max.bytes", 1000000 + MessageSets.LogOverhead, Range.make(0, Integer.MAX_VALUE));
        numNetworkThreads = props.getIntInRange("num.network.threads", 3, Range.make(1, Integer.MAX_VALUE));

           /* the number of network threads that the server uses for handling network requests */
        numNetworkThreads = props.getIntInRange("num.network.threads", 3, Range.make(1, Integer.MAX_VALUE));

    /* the number of io threads that the server uses for carrying out network requests */
        numIoThreads = props.getIntInRange("num.io.threads", 8, Range.make(1, Integer.MAX_VALUE));

    /* the number of threads to use for various background processing tasks */
        backgroundThreads = props.getIntInRange("background.threads", 4, Range.make(1, Integer.MAX_VALUE));

    /* the number of queued requests allowed before blocking the network threads */
        queuedMaxRequests = props.getIntInRange("queued.max.requests", 500, Range.make(1, Integer.MAX_VALUE));

        /*********** Socket Server Configuration ***********/

  /* the port to listen and accept connections on */
        port = props.getInt("port", 6667);

    /* hostname of broker. If this is set, it will only bind to this address. If this is not set,
     * it will bind to all interfaces */
        hostName = props.getString("host.name", null);

    /* hostname to publish to ZooKeeper for clients to use. In IaaS environments, this may
     * need to be different from the interface to which the broker binds. If this is not set,
     * it will use the for "host.name" if configured. Otherwise
     * it will use the returned from java.net.InetAddress.getCanonicalHostName(). */
        advertisedHostName = props.getString("advertised.host.name", hostName);

    /* the port to publish to ZooKeeper for clients to use. In IaaS environments, this may
     * need to be different from the port to which the broker binds. If this is not set,
     * it will publish the same port that the broker binds to. */
        advertisedPort = props.getInt("advertised.port", port);

    /* the SO_SNDBUFF buffer of the socket sever sockets */
        socketSendBufferBytes = props.getInt("socket.send.buffer.bytes", 100 * 1024);

    /* the SO_RCVBUFF buffer of the socket sever sockets */
        socketReceiveBufferBytes = props.getInt("socket.receive.buffer.bytes", 100 * 1024);

    /* the maximum number of bytes in a socket request */
        socketRequestMaxBytes = props.getIntInRange("socket.request.max.bytes", 100 * 1024 * 1024, Range.make(1, Integer.MAX_VALUE));

        /*********** Log Configuration ***********/

  /* the default number of log partitions per topic */
        numPartitions = props.getIntInRange("num.partitions", 1, Range.make(1, Integer.MAX_VALUE));

    /* the directories in which the log data is kept */
        logDirs = Utils.parseCsvList(props.getString("log.dirs", props.getString("log.dir", "/tmp/kafka-logs")));
        checkState(logDirs.size() > 0);

    /* the maximum size of a single log file */
        logSegmentBytes = props.getIntInRange("log.segment.bytes", 1 * 1024 * 1024 * 1024, Range.make(Messages.MinHeaderSize, Integer.MAX_VALUE));

    /* the maximum time before a new log segment is rolled out */
        logRollHours = props.getIntInRange("log.roll.hours", 24 * 7, Range.make(1, Integer.MAX_VALUE));

    /* the number of hours to keep a log file before deleting it */
        logRetentionTimeMillis = getLogRetentionTimeMillis();

    /* the maximum size of the log before deleting it */
        logRetentionBytes = props.getLong("log.retention.bytes", -1);

    /* the frequency in minutes that the log cleaner checks whether any log is eligible for deletion */
        logCleanupIntervalMs = props.getLongInRange("log.retention.check.interval.ms", 5 * 60 * 1000L, Range.make(1L, Long.MAX_VALUE));

    /* the default cleanup policy for segments beyond the retention window, must be either "delete" or "dedupe" */
        logCleanupPolicy = props.getString("log.cleanup.policy", "delete");

    /* the number of background threads to use for log cleaning */
        logCleanerThreads = props.getIntInRange("log.cleaner.threads", 1, Range.make(0, Integer.MAX_VALUE));

    /* the log cleaner will be throttled so that the sum of its read and write i/o will be less than this value on average */
        logCleanerIoMaxBytesPerSecond = props.getDouble("log.cleaner.io.max.bytes.per.second", Double.MAX_VALUE);

    /* the total memory used for log deduplication across all cleaner threads */
        logCleanerDedupeBufferSize = props.getLongInRange("log.cleaner.dedupe.buffer.size", 500 * 1024 * 1024L, Range.make(0L, Long.MAX_VALUE));
        checkState(logCleanerDedupeBufferSize / logCleanerThreads > 1024 * 1024, "log.cleaner.dedupe.buffer.size must be at least 1MB per cleaner thread.");

    /* the total memory used for log cleaner I/O buffers across all cleaner threads */
        logCleanerIoBufferSize = props.getIntInRange("log.cleaner.io.buffer.size", 512 * 1024, Range.make(0, Integer.MAX_VALUE));

    /* log cleaner dedupe buffer load factor. The percentage full the dedupe buffer can become. A higher value
     * will allow more log to be cleaned at once but will lead to more hash collisions */
        logCleanerDedupeBufferLoadFactor = props.getDouble("log.cleaner.io.buffer.load.factor", 0.9d);

    /* the amount of time to sleep when there are no logs to clean */
        logCleanerBackoffMs = props.getLongInRange("log.cleaner.backoff.ms", 30 * 1000, Range.make(0L, Long.MAX_VALUE));

    /* the minimum ratio of dirty log to total log for a log to eligible for cleaning */
        logCleanerMinCleanRatio = props.getDouble("log.cleaner.min.cleanable.ratio", 0.5);

    /* should we enable log cleaning? */
        logCleanerEnable = props.getBoolean("log.cleaner.enable", false);

    /* how long are delete records retained? */
        logCleanerDeleteRetentionMs = props.getLong("log.cleaner.delete.retention.ms", 24 * 60 * 60 * 1000L);

    /* the maximum size in bytes of the offset index */
        logIndexSizeMaxBytes = props.getIntInRange("log.index.size.max.bytes", 10 * 1024 * 1024, Range.make(4, Integer.MAX_VALUE));

    /* the interwith which we add an entry to the offset index */
        logIndexIntervalBytes = props.getIntInRange("log.index.interval.bytes", 4096, Range.make(0, Integer.MAX_VALUE));

    /* the number of messages accumulated on a log partition before messages are flushed to disk */
        logFlushIntervalMessages = props.getLongInRange("log.flush.interval.messages", Long.MAX_VALUE, Range.make(1L, Long.MAX_VALUE));

    /* the amount of time to wait before deleting a file from the filesystem */
        logDeleteDelayMs = props.getLongInRange("log.segment.delete.delay.ms", 60000, Range.make(0L, Long.MAX_VALUE));

    /* the frequency in ms that the log flusher checks whether any log needs to be flushed to disk */
        logFlushSchedulerIntervalMs = props.getLong("log.flush.scheduler.interval.ms", Long.MAX_VALUE);

    /* the maximum time in ms that a message in any topic is kept in memory before flushed to disk */
        logFlushIntervalMs = props.getLong("log.flush.interval.ms", logFlushSchedulerIntervalMs);

    /* the frequency with which we update the persistent record of the last flush which acts as the log recovery point */
        logFlushOffsetCheckpointIntervalMs = props.getIntInRange("log.flush.offset.checkpoint.interval.ms", 60000, Range.make(0, Integer.MAX_VALUE));

    /* enable auto creation of topic on the server */
        autoCreateTopicsEnable = props.getBoolean("auto.create.topics.enable", true);

        /*********** Replication configuration ***********/

  /* the socket timeout for controller-to-broker channels */
        controllerSocketTimeoutMs = props.getInt("controller.socket.timeout.ms", 30000);

    /* the buffer size for controller-to-broker-channels */
        controllerMessageQueueSize = props.getInt("controller.message.queue.size", 10);

    /* default replication factors for automatically created topics */
        defaultReplicationFactor = props.getInt("default.replication.factor", 1);

    /* If a follower hasn't sent any fetch requests during this time, the leader will remove the follower from isr */
        replicaLagTimeMaxMs = props.getLong("replica.lag.time.max.ms", 10000);

    /* If the lag in messages between a leader and a follower exceeds this number, the leader will remove the follower from isr */
        replicaLagMaxMessages = props.getLong("replica.lag.max.messages", 4000);

    /* the socket timeout for network requests */
        replicaSocketTimeoutMs = props.getInt("replica.socket.timeout.ms", ConsumerConfigs.SocketTimeout);

    /* the socket receive buffer for network requests */
        replicaSocketReceiveBufferBytes = props.getInt("replica.socket.receive.buffer.bytes", ConsumerConfigs.SocketBufferSize);

    /* the number of byes of messages to attempt to fetch */
        replicaFetchMaxBytes = props.getIntInRange("replica.fetch.max.bytes", ConsumerConfigs.FetchSize, Range.make(messageMaxBytes, Integer.MAX_VALUE));

    /* max wait time for each fetcher request issued by follower replicas*/
        replicaFetchWaitMaxMs = props.getInt("replica.fetch.wait.max.ms", 500);

    /* minimum bytes expected for each fetch response. If not enough bytes, wait up to replicaMaxWaitTimeMs */
        replicaFetchMinBytes = props.getInt("replica.fetch.min.bytes", 1);

    /* number of fetcher threads used to replicate messages from a source broker.
     * Increasing this value can increase the degree of I/O parallelism in the follower broker. */
        numReplicaFetchers = props.getInt("num.replica.fetchers", 1);

    /* the frequency with which the high watermark is saved out to disk */
        replicaHighWatermarkCheckpointIntervalMs = props.getLong("replica.high.watermark.checkpoint.interval.ms", 5000L);

    /* the purge inter(in number of requests) of the fetch request purgatory */
        fetchPurgatoryPurgeIntervalRequests = props.getInt("fetch.purgatory.purge.interval.requests", 10000);

    /* the purge interpublic int (in number of requests) of the producer request purgatory */
        producerPurgatoryPurgeIntervalRequests = props.getInt("producer.purgatory.purge.interval.requests", 10000);

        /*********** Controlled shutdown configuration ***********/

        /** Controlled shutdown can fail for multiple reasons. This determines the number of retries when such failure happens */
        controlledShutdownMaxRetries = props.getInt("controlled.shutdown.max.retries", 3);

        /** Before each retry, the system needs time to recover from the state that caused the previous failure (Controller
         * fail over, replica lag etc). This config determines the amount of time to wait before retrying. */
        controlledShutdownRetryBackoffMs = props.getInt("controlled.shutdown.retry.backoff.ms", 5000);

    /* enable controlled shutdown of the server */
        controlledShutdownEnable = props.getBoolean("controlled.shutdown.enable", false);

        /*********** Misc configuration ***********/

  /* the maximum size for a metadata entry associated with an offset commit */
        offsetMetadataMaxSize = props.getInt("offset.metadata.max.bytes", 1024);
    }

    /**
     * ******** General Configuration **********
     */

  /* the broker id for this server */
    public int brokerId;

    /* the maximum size of message that the server can receive */
    public int messageMaxBytes;

    /* the number of network threads that the server uses for handling network requests */
    public int numNetworkThreads;

    /* the number of io threads that the server uses for carrying out network requests */
    public int numIoThreads;

    /* the number of threads to use for various background processing tasks */
    public int backgroundThreads;

    /* the number of queued requests allowed before blocking the network threads */
    public int queuedMaxRequests;

    /**
     * ******** Socket Server Configuration **********
     */

  /* the port to listen and accept connections on */
    public int port;

    /* hostname of broker. If this is set, it will only bind to this address. If this is not set,
     * it will bind to all interfaces */
    public String hostName;

    /* hostname to publish to ZooKeeper for clients to use. In IaaS environments, this may
     * need to be different from the interface to which the broker binds. If this is not set,
     * it will use the public intue for "host.name" if configured. Otherwise
     * it will use the public intue returned from java.net.InetAddress.getCanonicalHostName(). */
    public String advertisedHostName;

    /* the port to publish to ZooKeeper for clients to use. In IaaS environments, this may
     * need to be different from the port to which the broker binds. If this is not set,
     * it will publish the same port that the broker binds to. */
    public int advertisedPort;

    /* the SO_SNDBUFF buffer of the socket sever sockets */
    public int socketSendBufferBytes;

    /* the SO_RCVBUFF buffer of the socket sever sockets */
    public int socketReceiveBufferBytes;

    /* the maximum number of bytes in a socket request */
    public int socketRequestMaxBytes;

    /**
     * ******** Log Configuration **********
     */

  /* the default number of log partitions per topic */
    public int numPartitions;

    /* the directories in which the log data is kept */
    public List<String> logDirs;

    /* the maximum size of a single log file */
    public int logSegmentBytes;

    /* the maximum time before a new log segment is rolled out */
    public int logRollHours;

    /* the number of hours to keep a log file before deleting it */
    public long logRetentionTimeMillis;

    /* the maximum size of the log before deleting it */
    public long logRetentionBytes;

    /* the frequency in minutes that the log cleaner checks whether any log is eligible for deletion */
    public long logCleanupIntervalMs;

    /* the default cleanup policy for segments beyond the retention window, must be either "delete" or "dedupe" */
    public String logCleanupPolicy;

    /* the number of background threads to use for log cleaning */
    public int logCleanerThreads;

    /* the log cleaner will be throttled so that the sum of its read and write i/o will be less than this value on average */
    public double logCleanerIoMaxBytesPerSecond;

    /* the total memory used for log deduplication across all cleaner threads */
    public long logCleanerDedupeBufferSize;

    /* the total memory used for log cleaner I/O buffers across all cleaner threads */
    public int logCleanerIoBufferSize;

    /* log cleaner dedupe buffer load factor. The percentage full the dedupe buffer can become. A higher value
     * will allow more log to be cleaned at once but will lead to more hash collisions */
    public double logCleanerDedupeBufferLoadFactor;

    /* the amount of time to sleep when there are no logs to clean */
    public long logCleanerBackoffMs;

    /* the minimum ratio of dirty log to total log for a log to eligible for cleaning */
    public double logCleanerMinCleanRatio;

    /* should we enable log cleaning? */
    public boolean logCleanerEnable;

    /* how long are delete records retained? */
    public long logCleanerDeleteRetentionMs;

    /* the maximum size in bytes of the offset index */
    public int logIndexSizeMaxBytes;

    /* the interpublic int with which we add an entry to the offset index */
    public int logIndexIntervalBytes;

    /* the number of messages accumulated on a log partition before messages are flushed to disk */
    public long logFlushIntervalMessages;

    /* the amount of time to wait before deleting a file from the filesystem */
    public long logDeleteDelayMs;

    /* the frequency in ms that the log flusher checks whether any log needs to be flushed to disk */
    public long logFlushSchedulerIntervalMs;

    /* the maximum time in ms that a message in any topic is kept in memory before flushed to disk */
    public long logFlushIntervalMs;

    /* the frequency with which we update the persistent record of the last flush which acts as the log recovery point */
    public int logFlushOffsetCheckpointIntervalMs;

    /* enable auto creation of topic on the server */
    public boolean autoCreateTopicsEnable;

    /**
     * ******** Replication configuration **********
     */

  /* the socket timeout for controller-to-broker channels */
    public int controllerSocketTimeoutMs;

    /* the buffer size for controller-to-broker-channels */
    public int controllerMessageQueueSize;

    /* default replication factors for automatically created topics */
    public int defaultReplicationFactor;

    /* If a follower hasn't sent any fetch requests during this time, the leader will remove the follower from isr */
    public long replicaLagTimeMaxMs;

    /* If the lag in messages between a leader and a follower exceeds this number, the leader will remove the follower from isr */
    public long replicaLagMaxMessages;

    /* the socket timeout for network requests */
    public int replicaSocketTimeoutMs;

    /* the socket receive buffer for network requests */
    public int replicaSocketReceiveBufferBytes;

    /* the number of byes of messages to attempt to fetch */
    public int replicaFetchMaxBytes;

    /* max wait time for each fetcher request issued by follower replicas*/
    public int replicaFetchWaitMaxMs;

    /* minimum bytes expected for each fetch response. If not enough bytes, wait up to replicaMaxWaitTimeMs */
    public int replicaFetchMinBytes;

    /* number of fetcher threads used to replicate messages from a source broker.
     * Increasing this value can increase the degree of I/O parallelism in the follower broker. */
    public int numReplicaFetchers;

    /* the frequency with which the high watermark is saved out to disk */
    public long replicaHighWatermarkCheckpointIntervalMs;

    /* the purge interpublic int (in number of requests) of the fetch request purgatory */
    public int fetchPurgatoryPurgeIntervalRequests;

    /* the purge interpublic int (in number of requests) of the producer request purgatory */
    public int producerPurgatoryPurgeIntervalRequests;

    /*********** Controlled shutdown configuration ***********/

    /**
     * Controlled shutdown can fail for multiple reasons. This determines the number of retries when such failure happens
     */
    public int controlledShutdownMaxRetries;

    /**
     * Before each retry, the system needs time to recover from the state that caused the previous failure (Controller
     * fail over, replica lag etc). This config determines the amount of time to wait before retrying.
     */
    public int controlledShutdownRetryBackoffMs;

    /* enable controlled shutdown of the server */
    public boolean controlledShutdownEnable;

    /**
     * ******** Misc configuration **********
     */

  /* the maximum size for a metadata entry associated with an offset commit */
    public int offsetMetadataMaxSize;
}
