package kafka.serializer;

/**
 * An encoder is a method of turning objects into byte arrays.
 * An implementation is required to provide a constructor that
 * takes a VerifiableProperties instance.
 */
public interface Encoder<T> {
    byte[] toBytes(T t);
}
