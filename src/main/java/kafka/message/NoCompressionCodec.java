package kafka.message;


public class NoCompressionCodec implements CompressionCodec {
    public static final CompressionCodec instance = new NoCompressionCodec();

    @Override
    public int codec() {
        return 0;
    }

    @Override
    public String name() {
        return "none";
    }
}
