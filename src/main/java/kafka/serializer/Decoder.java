package kafka.serializer;

/**
 * A decoder is a method of turning byte arrays into objects.
 * An implementation is required to provide a constructor that
 * takes a VerifiableProperties instance.
 */
public interface Decoder<T> {
    T fromBytes(byte[] bytes);
}
