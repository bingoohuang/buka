package kafka.producer;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import kafka.api.PartitionMetadata;
import kafka.api.TopicMetadata;
import kafka.cluster.Broker;
import kafka.common.UnavailableProducerException;
import kafka.utils.Callable1;
import kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ProducerPool {
    /**
     * Used in ProducerPool to initiate a SyncProducer connection with a broker.
     */
    public static SyncProducer createSyncProducer(ProducerConfig config, Broker broker) {
        Properties props = new Properties();
        props.put("host", broker.host);
        props.put("port", broker.port + "");
        props.putAll(config.props.props);
        return new SyncProducer(new SyncProducerConfig(props));
    }

    public ProducerConfig config;

    public ProducerPool(ProducerConfig config) {
        this.config = config;
    }

    private Map<Integer, SyncProducer> syncProducers = Maps.newHashMap();
    private Object lock = new Object();

    Logger logger = LoggerFactory.getLogger(ProducerPool.class);

    public void updateProducer(Collection<TopicMetadata> topicMetadata) {
        final Set<Broker> newBrokers = Sets.newHashSet();
        Utils.foreach(topicMetadata, new Callable1<TopicMetadata>() {
            @Override
            public void apply(TopicMetadata tmd) {
                Utils.foreach(tmd.partitionsMetadata, new Callable1<PartitionMetadata>() {
                    @Override
                    public void apply(PartitionMetadata pmd) {
                        if(pmd.leader != null)
                            newBrokers.add(pmd.leader);
                    }
                });
            }
        });
        synchronized(lock)  {
            Utils.foreach(newBrokers, new Callable1<Broker>() {
                @Override
                public void apply(Broker b) {
                    if(syncProducers.containsKey(b.id)){
                        syncProducers.get(b.id).close();
                        syncProducers.put(b.id, ProducerPool.createSyncProducer(config, b));
                    } else
                        syncProducers.put(b.id, ProducerPool.createSyncProducer(config, b));
                }
            });
        }
    }

    public SyncProducer getProducer(int brokerId)  {
        synchronized(lock) {
            SyncProducer producer = syncProducers.get(brokerId);
            if (producer != null) return producer;

            throw new UnavailableProducerException("Sync producer for broker id %d does not exist", brokerId);
        }
    }

    /**
     * Closes all the producers in the pool
     */
    public void close()  {
        synchronized(lock) {
            logger.info("Closing all sync producers");
            Iterator<SyncProducer> iter = syncProducers.values().iterator();
            while(iter.hasNext())
                iter.next().close();
        }
    }
}
