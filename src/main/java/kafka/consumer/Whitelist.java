package kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Whitelist extends TopicFilter {
    public Whitelist(String rawRegex) {
        super(rawRegex);
    }

    Logger logger = LoggerFactory.getLogger(Whitelist.class);

    @Override
    public boolean isTopicAllowed(String topic) {
        boolean allowed = topic.matches(regex);

        logger.debug("{} {}",
                topic, (allowed) ? "allowed" : "filtered");

        return allowed;
    }
}
