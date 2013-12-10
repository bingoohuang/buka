package kafka.utils;

public class Range<T> {
    public final T _1;
    public final T _2;

    public Range(T minValue, T maxValue) {
        _1 = minValue;
        _2 = maxValue;
    }

    public static <T> Range<T> make(T minValue, T maxValue) {
        return new Range<T>(minValue, maxValue);
    }
}
