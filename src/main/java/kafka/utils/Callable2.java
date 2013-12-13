package kafka.utils;

public interface Callable2<K, T> {
    void apply(K k, T t);
}
