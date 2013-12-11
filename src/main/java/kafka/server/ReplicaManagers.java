package kafka.server;

public interface ReplicaManagers {
    long UnknownLogEndOffset = -1L;
    String HighWatermarkFilename = "replication-offset-checkpoint";
}
