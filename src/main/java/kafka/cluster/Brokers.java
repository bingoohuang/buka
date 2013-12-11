package kafka.cluster;

import com.alibaba.fastjson.JSONObject;
import kafka.common.BrokerNotAvailableException;
import kafka.common.KafkaException;
import kafka.utils.Json;

import java.nio.ByteBuffer;

import static kafka.api.ApiUtils.readShortString;

public class Brokers {
    public static Broker createBroker(int id, String brokerInfoString) {
        if (brokerInfoString == null)
            throw new BrokerNotAvailableException("Broker id %s does not exist", id);

        try {
            JSONObject brokerInfo = Json.parseFull(brokerInfoString);
            if (brokerInfo == null)
                throw new BrokerNotAvailableException("Broker id %d does not exist", id);

            String host = brokerInfo.getString("host");
            int port = brokerInfo.getIntValue("port");
            return new Broker(id, host, port);
        } catch (Throwable t) {
            throw new KafkaException(t, "Failed to parse the broker info from zookeeper: %s", brokerInfoString);
        }
    }

    public static Broker readFrom(ByteBuffer buffer) {
        int id = buffer.getInt();
        String host = readShortString(buffer);
        int port = buffer.getInt();
        return new Broker(id, host, port);
    }
}
