package kafka.utils;

public interface Callable3<K, T, S> {
    void apply(K k, T t, S s);
}
