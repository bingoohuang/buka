package kafka.api;

import kafka.common.ErrorMapping;
import kafka.network.BoundedByteBufferSend;
import kafka.network.Request;
import kafka.network.RequestChannel;
import kafka.network.Response;
import kafka.utils.Callable1;
import kafka.utils.Function1;
import kafka.utils.Function2;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.List;

import static kafka.api.ApiUtils.shortStringLength;
import static kafka.api.ApiUtils.writeShortString;

public class TopicMetadataRequest extends RequestOrResponse {
    public final short versionId;
    public final String clientId;
    public final List<String> topics;


    public TopicMetadataRequest(short versionId, int correlationId, String clientId, List<String> topics) {
        super(RequestKeys.MetadataKey, correlationId);

        this.versionId = versionId;
        this.clientId = clientId;
        this.topics = topics;

    }

    public TopicMetadataRequest(List<String> topics, int correlationId) {
        this(TopicMetadataRequestReader.CurrentVersion, correlationId, TopicMetadataRequestReader.DefaultClientId, topics);
    }


    @Override
    public int sizeInBytes() {
        return 2 +  /* version id */
                4 + /* correlation id */
                shortStringLength(clientId) + /* client id */
                4  /* number of topics */
                + Utils.foldLeft(topics, 0, new Function2<Integer, String, Integer>() {
            @Override
            public Integer apply(Integer arg1, String topic) {
                return arg1 + shortStringLength(topic);
            }
        });
    }

    @Override
    public void writeTo(final ByteBuffer buffer) {
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        writeShortString(buffer, clientId);
        buffer.putInt(topics.size());
        Utils.foreach(topics, new Callable1<String>() {
            @Override
            public void apply(String topic) {
                writeShortString(buffer, topic);
            }
        });
    }

    @Override
    public String toString() {
        StringBuilder topicMetadataRequest = new StringBuilder();
        topicMetadataRequest.append("Name: " + this.getClass().getSimpleName());
        topicMetadataRequest.append("; Version: " + versionId);
        topicMetadataRequest.append("; CorrelationId: " + correlationId);
        topicMetadataRequest.append("; ClientId: " + clientId);
        topicMetadataRequest.append("; Topics: " + topics);
        return topicMetadataRequest.toString();
    }

    @Override
    public void handleError(final Throwable e, RequestChannel requestChannel, Request request) {
        List<TopicMetadata> topicMetadata = Utils.mapList(topics, new Function1<String, TopicMetadata>() {
            @Override
            public TopicMetadata apply(String topic) {
                return new TopicMetadata(topic, null, ErrorMapping.codeFor(e.getClass()));
            }
        });

        TopicMetadataResponse errorResponse = new TopicMetadataResponse(topicMetadata, correlationId);
        requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)));
    }
}
