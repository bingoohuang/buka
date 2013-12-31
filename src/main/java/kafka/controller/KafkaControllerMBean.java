package kafka.controller;

import kafka.common.TopicAndPartition;

import java.util.Set;

public interface KafkaControllerMBean {
    Set<TopicAndPartition> shutdownBroker(int id);
}
