package kafka.serializer;

import kafka.common.KafkaException;
import kafka.utils.VerifiableProperties;

import java.io.UnsupportedEncodingException;

/**
 * The string encoder takes an optional parameter serializer.encoding which controls
 * the character set used in encoding the string into bytes.
 */
public class StringEncoder implements Encoder<String> {
    public VerifiableProperties props;

    public StringEncoder() {
        this(null);
    }

    public StringEncoder(VerifiableProperties props) {
        this.props = props;
        encoding = props == null ? "UTF8" : props.getString("serializer.encoding", "UTF8");

    }

    public final String encoding;

    @Override
    public byte[] toBytes(String s) {
        if (s == null) return null;
        try {
            return s.getBytes(encoding);
        } catch (UnsupportedEncodingException e) {
            throw new KafkaException(e);
        }
    }
}
