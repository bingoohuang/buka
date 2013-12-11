package kafka.consumer;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

public abstract class TopicCount {
    public abstract Map<String, Set<String>> getConsumerThreadIdsPerTopic();

    public abstract Map<String, Integer> getTopicCountMap();

    public abstract String pattern();

    protected Map<String, Set<String>> makeConsumerThreadIdsPerTopic(String consumerIdString,
                                                                     Map<String, Integer> topicCountMap) {
        Map<String, Set<String>> consumerThreadIdsPerTopicMap = Maps.newHashMap();
        for (Map.Entry<String, Integer> entry : topicCountMap.entrySet()) {
            String topic = entry.getKey();
            Integer nConsumers = entry.getValue();
            Set<String> consumerSet = Sets.newTreeSet();
            assert (nConsumers >= 1);
            for (int i = 0; i < nConsumers; ++i)
                consumerSet.add(consumerIdString + "-" + i);

            consumerThreadIdsPerTopicMap.put(topic, consumerSet);
        }

        return consumerThreadIdsPerTopicMap;
    }
}
