package kafka.common;

/**
 * Convenience case class since (clientId, brokerInfo) pairs are used to create
 * SyncProducer Request Stats and SimpleConsumer Request and Response Stats.
 */
public class ClientIdAndBroker {
    public String clientId;
    public String brokerInfo;

    public ClientIdAndBroker(String clientId, String brokerInfo) {
        this.clientId = clientId;
        this.brokerInfo = brokerInfo;
    }

    @Override
    public String toString() {
        return clientId + "-" + brokerInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClientIdAndBroker that = (ClientIdAndBroker) o;

        if (brokerInfo != null ? !brokerInfo.equals(that.brokerInfo) : that.brokerInfo != null) return false;
        if (clientId != null ? !clientId.equals(that.clientId) : that.clientId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = clientId != null ? clientId.hashCode() : 0;
        result = 31 * result + (brokerInfo != null ? brokerInfo.hashCode() : 0);
        return result;
    }
}
