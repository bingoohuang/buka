package kafka.common;

/**
 * Convenience case class since (clientId, topic) pairs are used in the creation
 * of many Stats objects.
 */
public class ClientIdAndTopic {
    public String clientId;
    public String topic;

    public ClientIdAndTopic(String clientId, String topic) {
        this.clientId = clientId;
        this.topic = topic;
    }

    @Override
    public String toString() {
        return "ClientIdAndTopic{" +
                "clientId='" + clientId + '\'' +
                ", topic='" + topic + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClientIdAndTopic that = (ClientIdAndTopic) o;

        if (clientId != null ? !clientId.equals(that.clientId) : that.clientId != null) return false;
        if (topic != null ? !topic.equals(that.topic) : that.topic != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = clientId != null ? clientId.hashCode() : 0;
        result = 31 * result + (topic != null ? topic.hashCode() : 0);
        return result;
    }
}
