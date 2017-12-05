package org.mapdb.tuple;

import org.mapdb.serializer.Serializers;

import java.util.Comparator;

/**
 * Utility methods for tuples
 */
public final class Tuple {

    //TODO make btreemap.prefixMap() work

    /** returns true if all elements are equal, works with nulls*/
    static boolean eq(Object a, Object b) {
        return a==b || (a!=null && a.equals(b));
    }
    /** compare method which respects 'null' as negative infinity and 'HI' as positive inf */
    static <E> int compare2(Comparator<E> comparator, E a, E b) {
        if(a==b) return 0;
        if(a==null||b==HI) return -1;
        if(b==null||a==HI) return 1;
        return comparator.compare(a,b);
    }


    static final Comparator<Tuple2> TUPLE2_COMPARATOR = new Tuple2Serializer(Serializers.ELSA, Serializers.ELSA);
    static final Comparator<Tuple3> TUPLE3_COMPARATOR = new Tuple3Serializer(Serializers.ELSA, Serializers.ELSA, Serializers.ELSA);
    static final Comparator<Tuple4> TUPLE4_COMPARATOR = new Tuple4Serializer(Serializers.ELSA, Serializers.ELSA, Serializers.ELSA, Serializers.ELSA);
    static final Comparator<Tuple5> TUPLE5_COMPARATOR = new Tuple5Serializer(Serializers.ELSA, Serializers.ELSA, Serializers.ELSA, Serializers.ELSA, Serializers.ELSA);
    static final Comparator<Tuple6> TUPLE6_COMPARATOR = new Tuple6Serializer(Serializers.ELSA, Serializers.ELSA, Serializers.ELSA, Serializers.ELSA, Serializers.ELSA, Serializers.ELSA);


    /** positive infinity object. Is larger than anything else. Used in tuple comparators.
     * Negative infinity is represented by 'null' */
    public static final Object HI = new Comparable(){
        @Override public String toString() {
            return "HI";
        }

        @Override
        public int compareTo(final Object o) {
            return o==HI?0:1; //always greater than anything else
        }
    };


    /** autocast version of `HI`*/
    public static <A> A HI(){
        return (A) HI;
    }

    static <E> E hiIfNull(E e){
        return e==null ? HI() : e;
    }

    public static <A,B> Tuple2<A,B> t2(A a, B b) {
        return new Tuple2<>(a, b);
    }

    public static <A,B,C> Tuple3<A,B,C> t3(A a, B b, C c) {
        return new Tuple3<>(a, b, c);
    }

    public static <A,B,C,D> Tuple4<A,B,C,D> t4(A a, B b, C c, D d) {
        return new Tuple4<>(a, b, c, d);
    }

    public static <A,B,C,D,E> Tuple5<A,B,C,D,E> t5(A a, B b, C c, D d, E e) {
        return new Tuple5<>(a, b, c, d, e);
    }

    public static <A,B,C,D,E,F> Tuple6<A,B,C,D,E,F> t6(A a, B b, C c, D d, E e, F f) {
        return new Tuple6<>(a, b, c, d, e, f);
    }

//
//    /**
//     * Tuple2 Serializer which uses Default Serializer from DB and expect values to implement {@code Comparable} interface.
//     */
//    public static final Tuple2Serializer TUPLE2 = new Tuple2Serializer(null, null, null);

    //
//    /**
//     * Tuple3 Serializer which uses Default Serializer from DB and expect values to implement {@code Comparable} interface.
//     */
//    public static final Tuple3Serializer TUPLE3 = new Tuple3Serializer(null, null, null, null, null);

    //    /**
//     * Tuple4 Serializer which uses Default Serializer from DB and expect values to implement {@code Comparable} interface.
//     */
//    public static final Tuple4Serializer TUPLE4 = new Tuple4Serializer(null, null, null, null, null, null, null);


}
