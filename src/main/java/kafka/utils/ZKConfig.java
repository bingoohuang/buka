package kafka.utils;

public class ZKConfig {
    public final VerifiableProperties props;

    public ZKConfig(VerifiableProperties props) {
        this.props = props;

        zkConnect = props.getString("zookeeper.connect");
        zkSessionTimeoutMs = props.getInt("zookeeper.session.timeout.ms", 6000);
        zkConnectionTimeoutMs = props.getInt("zookeeper.connection.timeout.ms", zkSessionTimeoutMs);
        zkSyncTimeMs = props.getInt("zookeeper.sync.time.ms", 2000);
    }

    /**
     * ZK host string
     */
    public final String zkConnect;

    /**
     * zookeeper session timeout
     */
    public final int zkSessionTimeoutMs;

    /**
     * the max time that the client waits to establish a connection to zookeeper
     */
    public final int zkConnectionTimeoutMs;

    /**
     * how far a ZK follower can be behind a ZK leader
     */
    public final int zkSyncTimeMs;
}
