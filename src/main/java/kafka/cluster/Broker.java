package kafka.cluster;

import java.nio.ByteBuffer;

import static kafka.api.ApiUtils.shortStringLength;
import static kafka.api.ApiUtils.writeShortString;
import static kafka.utils.Utils.hashcode;

/**
 * A Kafka broker
 */
public class Broker {
    public final int id;
    public final String host;
    public final int port;

    public Broker(int id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    @Override
    public String toString() {
        return id + " (" + host + ":" + port + ")";
    }

    public String getConnectionString() {
        return host + ":" + port;
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(id);
        writeShortString(buffer, host);
        buffer.putInt(port);
    }

    public int sizeInBytes() {
        return shortStringLength(host) /* host name */ + 4 /* port */ + 4 /* broker id*/;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Broker) {
            Broker n = (Broker) obj;
            return id == n.id && host == n.host && port == n.port;
        }

        return false;
    }

    @Override
    public int hashCode() {
        return hashcode(id, host, port);
    }
}
