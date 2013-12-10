package kafka.serializer;

/**
 * The default implementation does nothing, just returns the same byte array it takes in.
 */
public class DefaultDecoder implements Decoder<byte[]> {
    @Override
    public byte[] fromBytes(byte[] bytes) {
        return bytes;
    }
}
