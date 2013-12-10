package kafka.serializer;

import kafka.utils.VerifiableProperties;

public class NullEncoder<T> implements Encoder<T> {
    public VerifiableProperties props;

    public NullEncoder() {

    }

    public NullEncoder(VerifiableProperties props) {
        this.props = props;
    }
    @Override
    public byte[] toBytes(T t) {
        return null;
    }
}
