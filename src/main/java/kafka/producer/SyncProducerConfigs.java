package kafka.producer;

public interface SyncProducerConfigs {
    String DefaultClientId = "";
    short DefaultRequiredAcks = 0;
    int DefaultAckTimeoutMs = 10000;
}
