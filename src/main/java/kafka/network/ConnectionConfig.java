package kafka.network;

public class ConnectionConfig {
    public String host;
    public int port;
    public int sendBufferSize = -1;
    public int receiveBufferSize = -1;
    public boolean tcpNoDelay = true;
    public boolean keepAlive = false;
}
