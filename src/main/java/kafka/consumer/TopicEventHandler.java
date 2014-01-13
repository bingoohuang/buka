package kafka.consumer;

import java.util.Collection;

public interface TopicEventHandler<T>{
    void handleTopicEvent(Collection<T> allTopics);
}
