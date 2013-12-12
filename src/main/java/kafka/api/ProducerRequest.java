package kafka.api;

import com.google.common.collect.Table;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.network.BoundedByteBufferSend;
import kafka.network.Request;
import kafka.network.RequestChannel;
import kafka.network.Response;
import kafka.utils.Function2;
import kafka.utils.Function3;
import kafka.utils.Tuple2;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Map;

import static kafka.api.ApiUtils.shortStringLength;
import static kafka.api.ApiUtils.writeShortString;

public class ProducerRequest extends RequestOrResponse {
    public short versionId;
    public String clientId;
    public short requiredAcks;
    public int ackTimeoutMs;
    public Map<TopicAndPartition, ByteBufferMessageSet> data;

    public ProducerRequest(short versionId,
                           int correlationId,
                           String clientId,
                           short requiredAcks,
                           int ackTimeoutMs,
                           Map<TopicAndPartition, ByteBufferMessageSet> data) {
        super(RequestKeys.ProduceKey, correlationId);

        this.versionId = versionId;
        this.clientId = clientId;
        this.requiredAcks = requiredAcks;
        this.ackTimeoutMs = ackTimeoutMs;
        this.data = data;

        dataGroupedByTopic = Utils.groupBy(data, new Function2<TopicAndPartition, ByteBufferMessageSet, String>() {
            @Override
            public String apply(TopicAndPartition topicAndPartition, ByteBufferMessageSet messageSet) {
                return topicAndPartition.topic;
            }
        });

        topicPartitionMessageSizeMap = Utils.map(data, new Function2<TopicAndPartition, ByteBufferMessageSet, Tuple2<TopicAndPartition, Integer>>() {
            @Override
            public Tuple2<TopicAndPartition, Integer> apply(TopicAndPartition arg1, ByteBufferMessageSet arg2) {
                return Tuple2.make(arg1, arg2.sizeInBytes());
            }
        });
    }


    /**
     * Partitions the data into a map of maps (one for each topic).
     */
    private Table<String, TopicAndPartition, ByteBufferMessageSet> dataGroupedByTopic;
    public Map<TopicAndPartition, Integer> topicPartitionMessageSizeMap;

    public ProducerRequest(int correlationId,
                           String clientId,
                           Short requiredAcks,
                           int ackTimeoutMs,
                           Map<TopicAndPartition, ByteBufferMessageSet> data) {
        this(ProducerRequestReader.CurrentVersion, correlationId, clientId, requiredAcks, ackTimeoutMs, data);
    }

    public void writeTo(final ByteBuffer buffer) {
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        writeShortString(buffer, clientId);
        buffer.putShort(requiredAcks);
        buffer.putInt(ackTimeoutMs);

        //save the topic structure
        buffer.putInt(dataGroupedByTopic.size()); //the number of topics

        Utils.foreach(dataGroupedByTopic, new Function2<String, Map<TopicAndPartition, ByteBufferMessageSet>, Void>() {
            @Override
            public Void apply(String topic, Map<TopicAndPartition, ByteBufferMessageSet> topicAndPartitionData) {
                writeShortString(buffer, topic); //write the topic
                buffer.putInt(topicAndPartitionData.size()); //the number of partitions

                Utils.foreach(topicAndPartitionData, new Function2<TopicAndPartition, ByteBufferMessageSet, Void>() {
                    @Override
                    public Void apply(TopicAndPartition arg1, ByteBufferMessageSet partitionMessageData) {
                        int partition = arg1.partition;
                        ByteBuffer bytes = partitionMessageData.buffer;
                        buffer.putInt(partition);
                        buffer.putInt(bytes.limit());
                        buffer.put(bytes);
                        bytes.rewind();

                        return null;
                    }
                });
                return null;
            }
        });
    }

    public int sizeInBytes() {
        return 2 + /* versionId */
                4 + /* correlationId */
                shortStringLength(clientId) + /* client id */
                2 + /* requiredAcks */
                4 + /* ackTimeoutMs */
                4 + /* number of topics */
                Utils.foldLeft(dataGroupedByTopic, 0,
                        new Function3<Integer, String, Map<TopicAndPartition, ByteBufferMessageSet>, Integer>() {
                            @Override
                            public Integer apply(Integer folded, String topic, Map<TopicAndPartition, ByteBufferMessageSet> currTopic) {
                                return folded + shortStringLength(topic)
                                        + 4 /* the number of partitions */
                                        + Utils.foldLeft(currTopic, 0, new Function3<Integer, TopicAndPartition, ByteBufferMessageSet, Integer>() {
                                    @Override
                                    public Integer apply(Integer arg1, TopicAndPartition arg2, ByteBufferMessageSet arg3) {
                                        return arg1 +
                                                4 + /* partition id */
                                                4 + /* byte-length of serialized messages */
                                                arg3.sizeInBytes();
                                    }
                                });
                            }
                        });

    }

    public int numPartitions() {
        return data.size();
    }

    @Override
    public String toString() {
        StringBuilder producerRequest = new StringBuilder();
        producerRequest.append("Name: " + this.getClass().getSimpleName());
        producerRequest.append("; Version: " + versionId);
        producerRequest.append("; CorrelationId: " + correlationId);
        producerRequest.append("; ClientId: " + clientId);
        producerRequest.append("; RequiredAcks: " + requiredAcks);
        producerRequest.append("; AckTimeoutMs: " + ackTimeoutMs + " ms");
        producerRequest.append("; TopicAndPartition: " + topicPartitionMessageSizeMap);
        return producerRequest.toString();
    }

    @Override
    public void handleError(final Throwable e, RequestChannel requestChannel, Request request) {
        if (((ProducerRequest) request.requestObj).requiredAcks == 0) {
            requestChannel.closeConnection(request.processor, request);
        } else {
            Map<TopicAndPartition, ProducerResponseStatus> producerResponseStatus = Utils.map(data, new Function2<TopicAndPartition, ByteBufferMessageSet, Tuple2<TopicAndPartition, ProducerResponseStatus>>() {
                @Override
                public Tuple2<TopicAndPartition, ProducerResponseStatus> apply(TopicAndPartition arg1, ByteBufferMessageSet arg2) {
                    return Tuple2.make(arg1, new ProducerResponseStatus(ErrorMapping.codeFor(e.getClass()), -1l));
                }
            });

            ProducerResponse errorResponse = new ProducerResponse(correlationId, producerResponseStatus);
            requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)));
        }
    }

    public void emptyData() {
        data.clear();
    }
}
