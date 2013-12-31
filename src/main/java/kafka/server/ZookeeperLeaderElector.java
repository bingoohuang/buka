package kafka.server;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;
import kafka.controller.ControllerContext;
import kafka.controller.KafkaControllers;
import kafka.utils.Callable0;
import kafka.utils.Function2;
import kafka.utils.SystemTime;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static kafka.utils.ZkUtils.*;

/**
 * This class handles zookeeper based leader election based on an ephemeral path. The election module does not handle
 * session expiration, instead it assumes the caller will handle it by probably try to re-elect again. If the existing
 * leader is dead, this class will handle automatic re-election and if it succeeds, it invokes the leader state change
 * callback
 */
public class ZookeeperLeaderElector implements LeaderElector {
    public ControllerContext controllerContext;
    public String electionPath;
    public Callable0 onBecomingLeader;
    public int brokerId;

    public ZookeeperLeaderElector(ControllerContext controllerContext,
                                  String electionPath,
                                  Callable0 onBecomingLeader,
                                  int brokerId) {
        this.controllerContext = controllerContext;
        this.electionPath = electionPath;
        this.onBecomingLeader = onBecomingLeader;
        this.brokerId = brokerId;

        index = electionPath.lastIndexOf("/");
        if (index > 0)
            makeSurePersistentPathExists(controllerContext.zkClient, electionPath.substring(0, index));
    }

    public int leaderId = -1;
    // create the election path in ZK, if one does not exist
    public int index;

    public LeaderChangeListener leaderChangeListener = new LeaderChangeListener();
    Logger logger = LoggerFactory.getLogger(ZookeeperLeaderElector.class);

    @Override
    public void startup() {
        synchronized (controllerContext.controllerLock) {
            controllerContext.zkClient.subscribeDataChanges(electionPath, leaderChangeListener);
            elect();
        }
    }

    @Override
    public boolean amILeader() {
        return leaderId == brokerId;
    }

    @Override
    public boolean elect() {
        String timestamp = SystemTime.instance.milliseconds() + "";
        String electString = JSON.toJSONString(ImmutableMap.of("version", 1, "brokerid", brokerId, "timestamp", timestamp));

        try {
            ZkUtils.createEphemeralPathExpectConflictHandleZKBug(controllerContext.zkClient, electionPath, electString, brokerId,
                    new Function2<String, Object, Boolean>() {
                        @Override
                        public Boolean apply(String controllerString, Object leaderId) {
                            return KafkaControllers.parseControllerId(controllerString) == (int) leaderId;
                        }
                    },
                    controllerContext.zkSessionTimeout);
            logger.info("{} successfully elected as leader", brokerId);
            leaderId = brokerId;
            onBecomingLeader.apply();
        } catch (ZkNodeExistsException e) {
            // If someone else has written the path, then
            String controller = readDataMaybeNull(controllerContext.zkClient, electionPath)._1;
            if (controller != null) {
                leaderId = KafkaControllers.parseControllerId(controller);
            } else {
                logger.warn("A leader has been elected but just resigned, this will result in another round of election");
                leaderId = -1;
            }

            if (leaderId != -1)
                logger.debug("Broker {} was elected as leader instead of broker {}", leaderId, brokerId);
        } catch (Throwable e) {
            logger.error("Error while electing or becoming leader on broker {}", brokerId, e);
            leaderId = -1;
        }

        return amILeader();
    }

    @Override
    public void close() {
        leaderId = -1;
    }

    public void resign() {
        leaderId = -1;
        deletePath(controllerContext.zkClient, electionPath);
    }

    /**
     * We do not have session expiration listen in the ZkElection, but assuming the caller who uses this module will
     * have its own session expiration listener and handler
     */
    class LeaderChangeListener implements IZkDataListener {
        /**
         * Called when the leader information stored in zookeeper has changed. Record the new leader in memory
         *
         * @throws Exception On any error.
         */
        @Override
        public void handleDataChange(String dataPath, Object data) throws Exception {
            synchronized (controllerContext.controllerLock) {
                leaderId = KafkaControllers.parseControllerId(data.toString());
                logger.info("New leader is {}", leaderId);
            }
        }

        /**
         * Called when the leader information stored in zookeeper has been delete. Try to elect as the leader
         *
         * @throws Exception On any error.
         */
        @Override
        public void handleDataDeleted(String dataPath) throws Exception {
            synchronized (controllerContext.controllerLock) {
                logger.debug("{} leader change listener fired for path {} to handle data deleted: trying to elect as a leader",
                        brokerId, dataPath);
                elect();
            }
        }
    }

}
