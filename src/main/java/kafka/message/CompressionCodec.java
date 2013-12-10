package kafka.message;

public interface CompressionCodec {
    int codec();
    String name();
}
