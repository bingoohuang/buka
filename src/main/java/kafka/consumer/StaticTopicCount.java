package kafka.consumer;

import java.util.Map;
import java.util.Set;

public class StaticTopicCount extends TopicCount {
    public final String consumerIdString;
    public final Map<String, Integer> topicCountMap;

    public StaticTopicCount(String consumerIdString, Map<String, Integer> topicCountMap) {
        this.consumerIdString = consumerIdString;
        this.topicCountMap = topicCountMap;
    }

    @Override
    public Map<String, Set<String>> getConsumerThreadIdsPerTopic() {
        return makeConsumerThreadIdsPerTopic(consumerIdString, topicCountMap);
    }

    @Override
    public Map<String, Integer> getTopicCountMap() {
        return topicCountMap;
    }

    @Override
    public String pattern() {
        return TopicCounts.staticPattern;
    }
}
