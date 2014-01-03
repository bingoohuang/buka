package kafka.producer;

import kafka.utils.VerifiableProperties;

import java.util.Properties;

public class SyncProducerConfig extends SyncProducerConfigShared {
    public static String DefaultClientId = "";
    public static short DefaultRequiredAcks = 0;
    public static int DefaultAckTimeoutMs = 10000;

    public SyncProducerConfig(VerifiableProperties props) {
        super(props);
        host = props.getString("host");
        port = props.getInt("port");
    }

    public SyncProducerConfig(Properties originalProps) {
        this(new VerifiableProperties(originalProps));
        // no need to verify the property since SyncProducerConfig is supposed to be used internally
    }

    /**
     * the broker to which the producer sends events
     */
    public String host;

    /**
     * the port on which the broker is running
     */
    public int port;
}
