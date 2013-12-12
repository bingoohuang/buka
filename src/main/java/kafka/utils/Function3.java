package kafka.utils;

public interface Function3<A1,A2, A3,B> {
    B apply(A1 arg1, A2 arg2, A3 arg3);
}
