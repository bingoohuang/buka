package kafka.consumer;

import com.google.common.collect.Maps;
import kafka.api.*;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.metrics.KafkaTimer;
import kafka.network.BlockingChannel;
import kafka.network.Receive;
import kafka.utils.Function0;
import kafka.utils.ThreadSafe;
import kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


/**
 * A consumer of kafka messages
 */
@ThreadSafe
public class SimpleConsumer {
    public String host;
    public int port, soTimeout, bufferSize;
    public String clientId;

    public SimpleConsumer(String host, int port, int soTimeout, int bufferSize, String clientId) {
        this.host = host;
        this.port = port;
        this.soTimeout = soTimeout;
        this.bufferSize = bufferSize;
        this.clientId = clientId;

        init();
    }

    private void init() {
        ConsumerConfigs.validateClientId(clientId);
        blockingChannel = new BlockingChannel(host, port, bufferSize, BlockingChannel.UseDefaultBufferSize, soTimeout);
        brokerInfo = "host_%s-port_%s".format(host, port);
        fetchRequestAndResponseStats = FetchRequestAndResponseStatsRegistry.getFetchRequestAndResponseStats(clientId);
    }

    private Object lock = new Object();
    private BlockingChannel blockingChannel;
    public String brokerInfo;
    private FetchRequestAndResponseStatsRegistry.FetchRequestAndResponseStats fetchRequestAndResponseStats;
    private boolean isClosed = false;
    Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);

    private BlockingChannel connect() {
        close();
        blockingChannel.connect();
        return blockingChannel;
    }

    private void disconnect() {
        if (blockingChannel.isConnected()) {
            logger.debug("Disconnecting from {}:{}", host, port);
            blockingChannel.disconnect();
        }
    }

    private void reconnect() {
        disconnect();
        connect();
    }

    public void close() {
        synchronized (lock) {
            disconnect();
            isClosed = true;
        }
    }

    private Receive sendRequest(RequestOrResponse request) {
        synchronized (lock) {
            getOrMakeConnection();
            Receive response = null;
            try {
                blockingChannel.send(request);
                response = blockingChannel.receive();
            } catch (RuntimeException e) {
                logger.info("Reconnect due to socket error: {}", e.getMessage());
                // retry once
                try {
                    reconnect();
                    blockingChannel.send(request);
                    response = blockingChannel.receive();
                } catch (RuntimeException ioe) {
                    disconnect();
                    throw ioe;
                }
            }
            return response;
        }
    }

    public TopicMetadataResponse send(TopicMetadataRequest request) {
        Receive response = sendRequest(request);
        return TopicMetadataResponse.readFrom(response.buffer());
    }

    /**
     * Fetch a set of messages from a topic.
     *
     * @param request specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.
     * @return a set of fetched messages
     */
    public FetchResponse fetch(final FetchRequest request) {
        final Receive[] response = {null};
        final KafkaTimer specificTimer = fetchRequestAndResponseStats.getFetchRequestAndResponseStats(brokerInfo).requestTimer;
        KafkaTimer aggregateTimer = fetchRequestAndResponseStats.getFetchRequestAndResponseAllBrokersStats().requestTimer;
        aggregateTimer.time(new Function0<Object>() {
            @Override
            public Object apply() {
                specificTimer.time(new Function0<Object>() {
                    @Override
                    public Object apply() {
                        response[0] = sendRequest(request);
                        return null;
                    }
                });

                return null;
            }
        });

        FetchResponse fetchResponse = FetchResponse.readFrom(response[0].buffer());
        int fetchedSize = fetchResponse.sizeInBytes();
        fetchRequestAndResponseStats.getFetchRequestAndResponseStats(brokerInfo).requestSizeHist.update(fetchedSize);
        fetchRequestAndResponseStats.getFetchRequestAndResponseAllBrokersStats().requestSizeHist.update(fetchedSize);
        return fetchResponse;
    }

    /**
     * Get a list of valid offsets (up to maxSize) before the given time.
     *
     * @param request a [[kafka.api.OffsetRequest]] object.
     * @return a [[kafka.api.OffsetResponse]] object.
     */
    public OffsetResponse getOffsetsBefore(OffsetRequest request) {
        return OffsetResponse.readFrom(sendRequest(request).buffer());
    }

    /**
     * Commit offsets for a topic
     *
     * @param request a [[kafka.api.OffsetCommitRequest]] object.
     * @return a [[kafka.api.OffsetCommitResponse]] object.
     */
    public OffsetCommitResponse commitOffsets(OffsetCommitRequest request) {
        return OffsetCommitResponse.readFrom(sendRequest(request).buffer());
    }

    /**
     * Fetch offsets for a topic
     *
     * @param request a [[kafka.api.OffsetFetchRequest]] object.
     * @return a [[kafka.api.OffsetFetchResponse]] object.
     */
    public OffsetFetchResponse fetchOffsets(OffsetFetchRequest request) {
        return OffsetFetchResponse.readFrom(sendRequest(request).buffer());
    }

    private void getOrMakeConnection() {
        if (!isClosed && !blockingChannel.isConnected()) {
            connect();
        }
    }

    /**
     * Get the earliest or latest offset of a given topic, partition.
     *
     * @param topicAndPartition Topic and partition of which the offset is needed.
     * @param earliestOrLatest  A value to indicate earliest or latest offset.
     * @param consumerId        Id of the consumer which could be a consumer client, SimpleConsumerShell or a follower broker.
     * @return Requested offset.
     */
    public long earliestOrLatestOffset(final TopicAndPartition topicAndPartition, final long earliestOrLatest, int consumerId) {
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = Maps.newHashMap();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(earliestOrLatest, 1));
        OffsetRequest request = new OffsetRequest(requestInfo,
                /*clientId = */clientId,
                /*replicaId = */consumerId);
        PartitionOffsetsResponse partitionErrorAndOffset = getOffsetsBefore(request).partitionErrorAndOffsets.get(topicAndPartition);
        if (partitionErrorAndOffset.error == ErrorMapping.NoError) {
            return Utils.head(partitionErrorAndOffset.offsets);
        }
        throw ErrorMapping.exceptionFor(partitionErrorAndOffset.error);
    }
}
