package kafka.producer;

import kafka.utils.Range;
import kafka.utils.VerifiableProperties;

public abstract class SyncProducerConfigShared {
    public VerifiableProperties props;

    protected SyncProducerConfigShared(VerifiableProperties props) {
        this.props = props;
        init();
    }

    private void init() {
        sendBufferBytes = props.getInt("send.buffer.bytes", 100 * 1024);
        clientId = props.getString("client.id", SyncProducerConfig.DefaultClientId);
        requestRequiredAcks = props.getShort("request.required.acks", SyncProducerConfig.DefaultRequiredAcks);
        requestTimeoutMs = props.getIntInRange("request.timeout.ms", SyncProducerConfig.DefaultAckTimeoutMs,
                Range.make(1, Integer.MAX_VALUE));
    }

    public int sendBufferBytes;

    /* the client application sending the producer requests */
    public String clientId;

    /*
     * The required acks of the producer requests - negative value means ack
     * after the replicas in ISR have caught up to the leader's offset
     * corresponding to this produce request.
     */
    public short requestRequiredAcks;

    /*
     * The ack timeout of the producer requests. Value must be non-negative and non-zero
     */
    public int requestTimeoutMs;
}
