package kafka.producer;

import kafka.common.Config;
import kafka.common.InvalidConfigException;
import kafka.message.CompressionCodec;
import kafka.message.CompressionCodecs;
import kafka.message.NoCompressionCodec;
import kafka.producer.async.AsyncProducerConfig;
import kafka.utils.Utils;
import kafka.utils.VerifiableProperties;

import java.util.List;
import java.util.Properties;

public class ProducerConfig extends AsyncProducerConfig {
    public static void validate(ProducerConfig config) {
        validateClientId(config.clientId);
        validateBatchSize(config.batchNumMessages, config.queueBufferingMaxMessages);
        validateProducerType(config.producerType);
    }

    public static void validateClientId(String clientId) {
        Config.validateChars("client.id", clientId);
    }

    public static void validateBatchSize(int batchSize, int queueSize) {
        if (batchSize > queueSize)
            throw new InvalidConfigException("Batch size = " + batchSize + " can't be larger than queue size = " + queueSize);
    }

    public static void validateProducerType(String producerType) {
        if (producerType.equals("sync") || producerType.equals("async")) {
        } else {
            throw new InvalidConfigException("Invalid value " + producerType + " for producer.type, valid values are sync/async");
        }
    }


    public ProducerConfig(VerifiableProperties props) {
        super(props);
        init();
    }

    public ProducerConfig(Properties originalProps) {
        this(new VerifiableProperties(originalProps));
        props.verify();
    }

    public String brokerList;
    public String partitionerClass;
    public String producerType;
    public CompressionCodec compressionCodec;
    public List<String> compressedTopics;
    public int messageSendMaxRetries;
    public int retryBackoffMs;
    public int topicMetadataRefreshIntervalMs;

    private void init() {
        /** This is for bootstrapping and the producer will only use it for getting metadata
         * (topics, partitions and replicas). The socket connections for sending the actual data
         * will be established based on the broker information returned in the metadata. The
         * format is host1:port1,host2:port2, and the list can be a subset of brokers or
         * a VIP pointing to a subset of brokers.
         */
        brokerList = props.getString("metadata.broker.list");

        /** the partitioner class for partitioning events amongst sub-topics */
        partitionerClass = props.getString("partitioner.class", "kafka.producer.DefaultPartitioner");

        /** this parameter specifies whether the messages are sent asynchronously *
         * or not. Valid values are - async for asynchronous send                 *
         *                            sync for synchronous send                   */
        producerType = props.getString("producer.type", "sync");

        /**
         * This parameter allows you to specify the compression codec for all data generated *
         * by this producer. The default is NoCompressionCodec
         */
        {
            String prop = props.getString("compression.codec", NoCompressionCodec.instance.name());
            try {
                compressionCodec = CompressionCodecs.getCompressionCodec(Integer.parseInt(prop));
            } catch (NumberFormatException e) {
                compressionCodec = CompressionCodecs.getCompressionCodec(prop);
            }
        }

        /** This parameter allows you to set whether compression should be turned *
         *  on for particular topics
         *
         *  If the compression codec is anything other than NoCompressionCodec,
         *
         *    Enable compression only for specified topics if any
         *
         *    If the list of compressed topics is empty, then enable the specified compression codec for all topics
         *
         *  If the compression codec is NoCompressionCodec, compression is disabled for all topics
         */
        compressedTopics = Utils.parseCsvList(props.getString("compressed.topics", null));

        /** The leader may be unavailable transiently, which can fail the sending of a message.
         *  This property specifies the number of retries when such failures occur.
         */
        messageSendMaxRetries = props.getInt("message.send.max.retries", 3);

        /** Before each retry, the producer refreshes the metadata of relevant topics. Since leader
         * election takes a bit of time, this property specifies the amount of time that the producer
         * waits before refreshing the metadata.
         */
        retryBackoffMs = props.getInt("retry.backoff.ms", 100);

        /**
         * The producer generally refreshes the topic metadata from brokers when there is a failure
         * (partition missing, leader not available...). It will also poll regularly (default: every 10min
         * so 600000ms). If you set this to a negative value, metadata will only get refreshed on failure.
         * If you set this to zero, the metadata will get refreshed after each message sent (not recommended)
         * Important note: the refresh happen only AFTER the message is sent, so if the producer never sends
         * a message the metadata is never refreshed
         */
        topicMetadataRefreshIntervalMs = props.getInt("topic.metadata.refresh.interval.ms", 600000);

        validate(this);
    }

}
