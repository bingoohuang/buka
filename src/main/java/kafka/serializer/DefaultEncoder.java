package kafka.serializer;

import kafka.utils.VerifiableProperties;

/**
 * The default implementation is a no-op, it just returns the same array it takes in
 */
public class DefaultEncoder implements Encoder<byte[]> {
    public VerifiableProperties props;

    public DefaultEncoder() {

    }

    public DefaultEncoder(VerifiableProperties props) {
        this.props = props;
    }

    @Override
    public byte[] toBytes(byte[] bytes) {
        return bytes;
    }
}
