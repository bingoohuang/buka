package kafka.consumer;

import com.google.common.collect.Maps;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class WildcardTopicCount extends TopicCount {
    public final ZkClient zkClient;
    public final String consumerIdString;
    public final TopicFilter topicFilter;
    public final int numStreams;

    public WildcardTopicCount(ZkClient zkClient,
                              String consumerIdString,
                              TopicFilter topicFilter,
                              int numStreams) {
        this.zkClient = zkClient;
        this.consumerIdString = consumerIdString;
        this.topicFilter = topicFilter;
        this.numStreams = numStreams;
    }

    @Override
    public Map<String, Set<String>> getConsumerThreadIdsPerTopic() {
        List<String> children = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerTopicsPath);

        Map<String, Integer> topicCountMap = Maps.newHashMap();
        if (children != null) {
            for (String topic : children) {
                if (topicFilter.isTopicAllowed(topic))
                    topicCountMap.put(topic, numStreams);
            }
        }


        return makeConsumerThreadIdsPerTopic(consumerIdString, topicCountMap);
    }

    @Override
    public Map<String, Integer> getTopicCountMap() {
        return null;
    }

    @Override
    public String pattern() {
        return null;
    }
}
