package kafka.utils;

public class Tuple2<T1, T2> {
    public final T1 _1;
    public final T2 _2;

    public Tuple2(T1 t1, T2 t2) {
        this._1 = t1;
        this._2 = t2;
    }

    public static <T1, T2> Tuple2<T1,T2> make(T1 t1, T2 t2) {
        return new Tuple2<T1, T2>(t1, t2);
    }

    @Override
    public String toString() {
        return "Tuple{" + _1 +
                ", " + _2 +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple2 tuple2 = (Tuple2) o;

        if (_1 != null ? !_1.equals(tuple2._1) : tuple2._1 != null) return false;
        if (_2 != null ? !_2.equals(tuple2._2) : tuple2._2 != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = _1 != null ? _1.hashCode() : 0;
        result = 31 * result + (_2 != null ? _2.hashCode() : 0);
        return result;
    }
}
