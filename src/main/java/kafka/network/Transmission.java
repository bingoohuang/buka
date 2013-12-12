package kafka.network;

import kafka.common.KafkaException;

/**
 * Represents a stateful transfer of data to or from the network
 */
public abstract class Transmission {
    public abstract boolean complete();

    protected void expectIncomplete() {
        if (complete())
            throw new KafkaException("This operation cannot be completed on a complete request.");
    }

    protected void expectComplete() {
        if (!complete())
            throw new KafkaException("This operation cannot be completed on an incomplete request.");
    }
}
