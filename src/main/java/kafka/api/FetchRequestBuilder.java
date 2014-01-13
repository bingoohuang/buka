package kafka.api;

import com.google.common.collect.Maps;
import kafka.common.TopicAndPartition;
import kafka.consumer.ConsumerConfigs;
import kafka.utils.NonThreadSafe;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@NonThreadSafe
public class FetchRequestBuilder {
    private AtomicInteger correlationId = new AtomicInteger(0);
    private short versionId = FetchRequestReader.CurrentVersion;
    private String clientId = ConsumerConfigs.DefaultClientId;
    private int replicaId = Requests.OrdinaryConsumerId;
    private int maxWait = FetchRequestReader.DefaultMaxWait;
    private int minBytes = FetchRequestReader.DefaultMinBytes;
    private Map<TopicAndPartition, PartitionFetchInfo> requestMap = Maps.newHashMap();

    public FetchRequestBuilder addFetch(String topic, int partition, long offset, int fetchSize)  {
        requestMap.put(new TopicAndPartition(topic, partition), new PartitionFetchInfo(offset, fetchSize));
        return this;
    }

    public FetchRequestBuilder clientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    /**
     * Only for internal use. Clients shouldn't set replicaId.
     */
    public FetchRequestBuilder replicaId(int replicaId){
        this.replicaId = replicaId;
        return this;
    }

    public FetchRequestBuilder maxWait(int maxWait) {
        this.maxWait = maxWait;
        return this;
    }

    public FetchRequestBuilder minBytes(int minBytes) {
        this.minBytes = minBytes;
        return this;
    }

    public FetchRequest build() {
        FetchRequest fetchRequest = new
                FetchRequest(versionId, correlationId.getAndIncrement(), clientId, replicaId, maxWait, minBytes, requestMap);
        requestMap.clear();
        return fetchRequest;
    }
}
