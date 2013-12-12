package kafka.utils;

public class Tuple3<T1, T2, T3> extends Tuple2<T1, T2> {
    public final T3 _3;

    public Tuple3(T1 t1, T2 t2, T3 _3) {
        super(t1, t2);
        this._3 = _3;
    }

    @Override
    public String toString() {
        return "(" + _1 + ", " + _2 + ", " + _3 + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        Tuple3 tuple3 = (Tuple3) o;

        if (_3 != null ? !_3.equals(tuple3._3) : tuple3._3 != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (_3 != null ? _3.hashCode() : 0);
        return result;
    }

    public static <T1, T2, T3> Tuple3<T1, T2, T3> make(T1 t1, T2 t2, T3 t3) {
        return new Tuple3<T1, T2, T3>(t1, t2, t3);
    }
}
