package kafka.api;

public abstract class Requests {
    public final static int OrdinaryConsumerId = -1;
    public final static int DebuggingConsumerId = -2;

    // Followers use broker id as the replica id, which are non-negative int.
    public static boolean isReplicaIdFromFollower(int replicaId) {
        return (replicaId >= 0);
    }
}
