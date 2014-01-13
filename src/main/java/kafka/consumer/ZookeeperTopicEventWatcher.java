package kafka.consumer;

import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ZookeeperTopicEventWatcher {
    public ZkClient zkClient;
    public TopicEventHandler<String> eventHandler;

    public ZookeeperTopicEventWatcher(ZkClient zkClient, TopicEventHandler<String> eventHandler) {
        this.zkClient = zkClient;
        this.eventHandler = eventHandler;
        startWatchingTopicEvents();
    }

    Logger logger = LoggerFactory.getLogger(ZookeeperTopicEventWatcher.class);
    public Object lock = new Object();

    private void startWatchingTopicEvents() {
        ZkTopicEventListener topicEventListener = new ZkTopicEventListener();
        ZkUtils.makeSurePersistentPathExists(zkClient, ZkUtils.BrokerTopicsPath);

        zkClient.subscribeStateChanges(new ZkSessionExpireListener(topicEventListener));

        List<String> topics = zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, topicEventListener);

        // call to bootstrap topic list
        topicEventListener.handleChildChange(ZkUtils.BrokerTopicsPath, topics);
    }

    private void stopWatchingTopicEvents() {
        zkClient.unsubscribeAll();
    }

    public void shutdown() {
        synchronized (lock) {
            logger.info("Shutting down topic event watcher.");
            if (zkClient != null) {
                stopWatchingTopicEvents();
            } else {
                logger.warn("Cannot shutdown since the embedded zookeeper client has already closed.");
            }
        }
    }

    class ZkTopicEventListener implements IZkChildListener {

        @Override
        public void handleChildChange(String s, List<String> strings) {
            synchronized (lock) {
                try {
                    if (zkClient != null) {
                        List<String> latestTopics = zkClient.getChildren(ZkUtils.BrokerTopicsPath);
                        logger.debug("all topics: {}", latestTopics);
                        eventHandler.handleTopicEvent(latestTopics);
                    }
                } catch (Throwable e) {
                    logger.error("error in handling child changes", e);
                }
            }
        }
    }

    class ZkSessionExpireListener implements IZkStateListener {
        public ZkTopicEventListener topicEventListener;

        ZkSessionExpireListener(ZkTopicEventListener topicEventListener) {
            this.topicEventListener = topicEventListener;
        }

        @Override
        public void handleStateChanged(Watcher.Event.KeeperState keeperState) throws Exception {

        }

        @Override
        public void handleNewSession() throws Exception {
            synchronized (lock) {
                if (zkClient != null) {
                    logger.info("ZK expired: resubscribing topic event listener to topic registry");
                    zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, topicEventListener);
                }
            }
        }

        @Override
        public void handleSessionEstablishmentError(Throwable throwable) throws Exception {

        }
    }

}
