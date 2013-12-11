package kafka.api;

import com.google.common.collect.Maps;
import kafka.common.KafkaException;
import kafka.utils.Tuple2;

import java.util.Map;

public class RequestKeys {
    public static final short ProduceKey = 0;
    public static final short FetchKey = 1;
    public static final short OffsetsKey = 2;
    public static final short MetadataKey = 3;
    public static final short LeaderAndIsrKey = 4;
    public static final short StopReplicaKey = 5;
    public static final short UpdateMetadataKey = 6;
    public static final short ControlledShutdownKey = 7;
    public static final short OffsetCommitKey = 8;
    public static final short OffsetFetchKey = 9;

    public static Map<Short, Tuple2<String, RequestReader>> keyToNameAndDeserializerMap = Maps.newHashMap();

    static {
        keyToNameAndDeserializerMap.put(ProduceKey, Tuple2.make("Produce", ProducerRequestReader.instance));
        keyToNameAndDeserializerMap.put(FetchKey, Tuple2.make("Fetch", FetchRequestReader.instance));
        keyToNameAndDeserializerMap.put(OffsetsKey, Tuple2.make("Offsets", OffsetRequestReader.instance));
        keyToNameAndDeserializerMap.put(MetadataKey, Tuple2.make("Metadata", TopicMetadataRequestReader.instance));
        keyToNameAndDeserializerMap.put(LeaderAndIsrKey, Tuple2.make("LeaderAndIsr", LeaderAndIsrRequestReader.instance));
        keyToNameAndDeserializerMap.put(StopReplicaKey, Tuple2.make("StopReplica", StopReplicaRequestReader.instance));
        keyToNameAndDeserializerMap.put(UpdateMetadataKey, Tuple2.make("UpdateMetadata", UpdateMetadataRequestReader.instance));
        keyToNameAndDeserializerMap.put(ControlledShutdownKey, Tuple2.make("ControlledShutdown", ControlledShutdownRequestReader.instance));
        keyToNameAndDeserializerMap.put(OffsetCommitKey, Tuple2.make("OffsetCommit", OffsetCommitRequestReader.instance));
        keyToNameAndDeserializerMap.put(OffsetFetchKey, Tuple2.make("OffsetFetch", OffsetFetchRequestReader.instance));
    }

    public static String nameForKey(Short key) {
        Tuple2<String, RequestReader> nameAndSerializer = keyToNameAndDeserializerMap.get(key);
        if (nameAndSerializer == null) throw new KafkaException("Wrong request type %d", key);

        return nameAndSerializer._1;
    }

    public static RequestReader deserializerForKey(Short key) {
        Tuple2<String, RequestReader> nameAndSerializer = keyToNameAndDeserializerMap.get(key);
        if (nameAndSerializer == null) throw new KafkaException("Wrong request type %d", key);

        return nameAndSerializer._2;
    }
}
