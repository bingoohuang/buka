package kafka.consumer;

import com.google.common.base.Function;
import com.yammer.metrics.core.Meter;
import kafka.common.ClientIdAndTopic;
import kafka.metrics.KafkaMetricsGroup;
import kafka.utils.Pool;
import kafka.utils.ThreadSafe;

import java.util.concurrent.TimeUnit;

/**
 * Stores the topic stats information of each consumer client in a (clientId -> ConsumerTopicStats) map.
 */
public class ConsumerTopicStatsRegistry {
    private static Function<String, ConsumerTopicStats> valueFactory = new Function<String, ConsumerTopicStatsRegistry.ConsumerTopicStats>() {
        @Override
        public ConsumerTopicStatsRegistry.ConsumerTopicStats apply(String k) {
            return new ConsumerTopicStats(k);
        }
    };
    private static Pool<String, ConsumerTopicStats> globalStats = new Pool<String, ConsumerTopicStats>(valueFactory);

    public static ConsumerTopicStats getConsumerTopicStat(String clientId) {
        return globalStats.getAndMaybePut(clientId);
    }

    @ThreadSafe
    static class ConsumerTopicMetrics extends KafkaMetricsGroup {
        ClientIdAndTopic metricId;

        ConsumerTopicMetrics(ClientIdAndTopic metricId) {
            this.metricId = metricId;
            messageRate = newMeter(metricId + "MessagesPerSec", "messages", TimeUnit.SECONDS);
            byteRate = newMeter(metricId + "BytesPerSec", "bytes", TimeUnit.SECONDS);
        }

        public Meter messageRate;
        public Meter byteRate;
    }

    public static class ConsumerTopicStats {
        String clientId;

        /**
         * Tracks metrics for each topic the given consumer client has consumed data from.
         *
         * @param clientId The clientId of the given consumer client.
         */
        ConsumerTopicStats(String clientId) {
            this.clientId = clientId;
            allTopicStats = new ConsumerTopicMetrics(new ClientIdAndTopic(clientId, "AllTopics")); // to differentiate from a topic named AllTopics
        }

        private Function<ClientIdAndTopic, ConsumerTopicMetrics> valueFactory = new Function<ClientIdAndTopic, ConsumerTopicStatsRegistry.ConsumerTopicMetrics>() {
            @Override
            public ConsumerTopicStatsRegistry.ConsumerTopicMetrics apply(ClientIdAndTopic k) {
                return new ConsumerTopicMetrics(k);
            }
        };
        private Pool<ClientIdAndTopic, ConsumerTopicMetrics> stats = new Pool<ClientIdAndTopic, ConsumerTopicMetrics>(valueFactory);
        private ConsumerTopicMetrics allTopicStats;

        public ConsumerTopicMetrics getConsumerAllTopicStats() {
            return allTopicStats;
        }

        public ConsumerTopicMetrics getConsumerTopicStats(String topic) {
            return stats.getAndMaybePut(new ClientIdAndTopic(clientId, topic + "-"));
        }
    }
}
