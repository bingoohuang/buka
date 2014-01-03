package kafka.client;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import kafka.api.TopicMetadataRequest;
import kafka.api.TopicMetadataRequestReader;
import kafka.api.TopicMetadataResponse;
import kafka.cluster.Broker;
import kafka.common.KafkaException;
import kafka.producer.ProducerConfig;
import kafka.producer.ProducerPool;
import kafka.producer.SyncProducer;
import kafka.utils.Function1;
import kafka.utils.Function2;
import kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Helper functions common to clients (producer, consumer, or admin)
 */
public class ClientUtils {
    static Logger logger = LoggerFactory.getLogger(ClientUtils.class);

    /**
     * Used by the producer to send a metadata request since it has access to the ProducerConfig
     *
     * @param topics         The topics for which the metadata needs to be fetched
     * @param brokers        The brokers in the cluster as configured on the producer through metadata.broker.list
     * @param producerConfig The producer's config
     * @return topic metadata response
     */
    public static TopicMetadataResponse fetchTopicMetadata(Set<String> topics, Collection<Broker> brokers, ProducerConfig producerConfig, int correlationId) {
        Boolean fetchMetaDataSucceeded = false;
        int i = 0;
        TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(TopicMetadataRequestReader.CurrentVersion, correlationId, producerConfig.clientId, Lists.newArrayList(topics));
        TopicMetadataResponse topicMetadataResponse = null;
        Throwable t = null;
        // shuffle the list of brokers before sending metadata requests so that most requests don't get routed to the
        // same broker
        List<Broker> shuffledBrokers = Lists.newArrayList(brokers);
        Collections.shuffle(shuffledBrokers);
        while (i < shuffledBrokers.size() && !fetchMetaDataSucceeded) {
            SyncProducer producer = ProducerPool.createSyncProducer(producerConfig, shuffledBrokers.get(i));
            logger.info("Fetching metadata from broker {} with correlation id {} for {} topic(s) {}", shuffledBrokers.get(i), correlationId, topics.size(), topics);
            try {
                topicMetadataResponse = producer.send(topicMetadataRequest);
                fetchMetaDataSucceeded = true;
            } catch (Throwable e) {
                logger.warn("Fetching topic metadata with correlation id {} for topics [{}] from broker [{}] failed",
                        correlationId, topics, shuffledBrokers.get(i), e);
                t = e;
            } finally {
                i = i + 1;
                producer.close();
            }
        }
        if (!fetchMetaDataSucceeded) {
            throw new KafkaException(t, "fetching topic metadata for topics [%s] from broker [%s] failed", topics, shuffledBrokers);
        } else {
            logger.debug("Successfully fetched metadata for {} topic(s) {}", topics.size(), topics);
        }
        return topicMetadataResponse;
    }

    /**
     * Used by a non-producer client to send a metadata request
     *
     * @param topics   The topics for which the metadata needs to be fetched
     * @param brokers  The brokers in the cluster as configured on the client
     * @param clientId The client's identifier
     * @return topic metadata response
     */
    public static TopicMetadataResponse fetchTopicMetadata(Set<String> topics, Collection<Broker> brokers, String clientId, int timeoutMs,
                                                           int correlationId /*= 0*/) {
        Properties props = new Properties();
        props.put("metadata.broker.list", Joiner.on(',').join(Utils.mapList(brokers, new Function1<Broker, String>() {
            @Override
            public String apply(Broker _) {
                return _.getConnectionString();
            }
        })));
        props.put("client.id", clientId);
        props.put("request.timeout.ms", timeoutMs + "");
        ProducerConfig producerConfig = new ProducerConfig(props);
        return fetchTopicMetadata(topics, brokers, producerConfig, correlationId);
    }

    /**
     * Parse a list of broker urls in the form host1:port1, host2:port2, ...
     */
    public static List<Broker> parseBrokerList(String brokerListStr) {
        List<String> brokersStr = Utils.parseCsvList(brokerListStr);

        return Utils.zipWithIndex(brokersStr, new Function2<String, Integer, Broker>() {
            @Override
            public Broker apply(String brokerStr, Integer brokerId) {
                String[] brokerInfos = brokerStr.split(":");
                String hostName = brokerInfos[0];
                int port = Integer.parseInt(brokerInfos[1]);
                return new Broker(brokerId, hostName, port);
            }
        });
    }

}
