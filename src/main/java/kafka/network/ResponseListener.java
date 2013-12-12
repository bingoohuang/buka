package kafka.network;

public interface ResponseListener {
    void onResponse(int processor);
}
