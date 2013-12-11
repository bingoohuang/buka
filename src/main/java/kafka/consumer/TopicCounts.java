package kafka.consumer;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Throwables;
import kafka.common.KafkaException;
import kafka.utils.*;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TopicCounts {
    public static final String whiteListPattern = "white_list";
    public static final String blackListPattern = "black_list";
    public static final String staticPattern = "static";

    static Logger logger = LoggerFactory.getLogger(TopicCounts.class);

    public static TopicCount constructTopicCount(String group, String consumerId, ZkClient zkClient) {
        ZKGroupDirs dirs = new ZKGroupDirs(group);
        String topicCountString = ZkUtils.readData(zkClient, dirs.consumerRegistryDir() + "/" + consumerId)._1;
        String subscriptionPattern = null;
        Map<String, Integer> topMap = null;
        try {
            JSONObject consumerRegistrationMap = Json.parseFull(topicCountString);
            if (consumerRegistrationMap == null)
                throw new KafkaException("error constructing TopicCount : " + topicCountString);

            String pattern = consumerRegistrationMap.getString("pattern");
            if (pattern == null) throw new KafkaException("error constructing TopicCount : " + topicCountString);

            subscriptionPattern = pattern;


            topMap = (Map<String, Integer>) consumerRegistrationMap.get("subscription");
            if (topMap == null) throw new KafkaException("error constructing TopicCount : " + topicCountString);

        } catch (Throwable e) {
            logger.error("error parsing consumer json string {}", topicCountString, e);
            throw Throwables.propagate(e);
        }

        boolean hasWhiteList = whiteListPattern.equals(subscriptionPattern);
        boolean hasBlackList = blackListPattern.equals(subscriptionPattern);

        if (topMap.isEmpty() || !(hasWhiteList || hasBlackList)) {
            return new StaticTopicCount(consumerId, topMap);
        } else {
            Tuple2<String, Integer> head = Utils.head(topMap);
            String regex = head._1;
            int numStreams = head._2;
            TopicFilter filter = hasWhiteList ? new Whitelist(regex) : new Blacklist(regex);
            return new WildcardTopicCount(zkClient, consumerId, filter, numStreams);
        }
    }

    public static TopicCount constructTopicCount(String consumerIdString, Map<String, Integer> topicCount) {
        return new StaticTopicCount(consumerIdString, topicCount);
    }

    public static TopicCount constructTopicCount(String consumerIdString, TopicFilter filter,
                                                 int numStreams, ZkClient zkClient) {
        return new WildcardTopicCount(zkClient, consumerIdString, filter, numStreams);
    }
}
