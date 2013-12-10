package kafka.utils;

public interface Function2<A1,A2,B> {
    B apply(A1 arg1, A2 arg2);
}
