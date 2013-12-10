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
}
