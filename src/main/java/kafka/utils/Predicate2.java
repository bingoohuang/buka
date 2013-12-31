package kafka.utils;

public interface Predicate2<K, T> {
    boolean apply(K k, T t);
}
