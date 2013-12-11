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

    @Override
    public String toString() {
        return "Range{"+ _1 +
                ", " + _2 +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Range range = (Range) o;

        if (_1 != null ? !_1.equals(range._1) : range._1 != null) return false;
        if (_2 != null ? !_2.equals(range._2) : range._2 != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = _1 != null ? _1.hashCode() : 0;
        result = 31 * result + (_2 != null ? _2.hashCode() : 0);
        return result;
    }
}
