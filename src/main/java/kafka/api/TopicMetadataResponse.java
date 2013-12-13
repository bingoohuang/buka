package kafka.api;

import com.google.common.collect.Lists;
import kafka.cluster.Broker;
import kafka.cluster.Brokers;
import kafka.utils.*;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class TopicMetadataResponse extends RequestOrResponse {
    public static TopicMetadataResponse readFrom(final ByteBuffer buffer) {
        int correlationId = buffer.getInt();
        int brokerCount = buffer.getInt();
        List<Broker> brokers = Utils.flatList(0, brokerCount, new Function1<Integer, Broker>() {
            @Override
            public Broker apply(Integer index) {
                return Brokers.readFrom(buffer);
            }
        });

        final Map<Integer, Broker> brokerMap = Utils.map(brokers, new Function1<Broker, Tuple2<Integer, Broker>>() {
            @Override
            public Tuple2<Integer, Broker> apply(Broker broker) {
                return Tuple2.make(broker.id, broker);
            }
        });
        int topicCount = buffer.getInt();
        List<TopicMetadata> topicsMetadata = Utils.flatList(0, topicCount, new Function1<Integer, TopicMetadata>() {
            @Override
            public TopicMetadata apply(Integer arg) {
                return TopicMetadata.readFrom(buffer, brokerMap);
            }
        });

        return new TopicMetadataResponse(topicsMetadata, correlationId);
    }

    public List<TopicMetadata> topicsMetadata;

    public TopicMetadataResponse(List<TopicMetadata> topicsMetadata, int correlationId) {
        super(correlationId);

        this.topicsMetadata = topicsMetadata;
    }

    @Override
    public int sizeInBytes() {
        Collection<Broker> brokers = extractBrokers(topicsMetadata).values();

        return 4 + 4
                + Utils.foldLeft(brokers, 0, new Function2<Integer, Broker, Integer>() {
            @Override
            public Integer apply(Integer arg1, Broker _) {
                return arg1 + _.sizeInBytes();
            }
        })
                + 4
                + Utils.foldLeft(topicsMetadata, 0, new Function2<Integer, TopicMetadata, Integer>() {
            @Override
            public Integer apply(Integer arg1, TopicMetadata _) {
                return arg1 + _.sizeInBytes();
            }
        });
    }

    @Override
    public void writeTo(final ByteBuffer buffer) {
        buffer.putInt(correlationId);
    /* brokers */
        Collection<Broker> brokers = extractBrokers(topicsMetadata).values();
        buffer.putInt(brokers.size());
        Utils.foreach(brokers, new Callable1<Broker>() {
            @Override
            public void apply(Broker _) {
                _.writeTo(buffer);
            }
        });
    /* topic metadata */
        buffer.putInt(topicsMetadata.size());

        Utils.foreach(topicsMetadata, new Callable1<TopicMetadata>() {
            @Override
            public void apply(TopicMetadata _) {
                _.writeTo(buffer);
            }
        });
    }

    private Map<Integer, Broker> extractBrokers(List<TopicMetadata> topicMetadatas) {
        List<PartitionMetadata> parts = Utils.mapLists(topicsMetadata, new Function1<TopicMetadata, Collection<PartitionMetadata>>() {
            @Override
            public Collection<PartitionMetadata> apply(TopicMetadata arg) {
                return arg.partitionsMetadata;
            }
        });

        List<Broker> brokers = Utils.mapLists(parts, new Function1<PartitionMetadata, Collection<Broker>>() {
            @Override
            public Collection<Broker> apply(PartitionMetadata _) {
                List<Broker> lst = Lists.newArrayList();
                lst.addAll(_.replicas);

                if (_.leader != null) lst.add(_.leader);

                return lst;
            }
        });

        return Utils.map(brokers, new Function1<Broker, Tuple2<Integer, Broker>>() {
            @Override
            public Tuple2<Integer, Broker> apply(Broker b) {
                return Tuple2.make(b.id, b);
            }
        });
    }
}
