package org.mapdb.tuple;

import java.io.Serializable;

public final class Tuple2<A,B> implements Comparable<Tuple2<A,B>>, Serializable {

    private static final long serialVersionUID = -8816277286657643283L;
		
		final public A a;
        final public B b;

        public Tuple2(A a, B b) {
            this.a = a;
            this.b = b;
        }


        @Override public int compareTo(Tuple2<A,B> o) {
            return Tuple.TUPLE2_COMPARATOR.compare(this, o);
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final Tuple2 t = (Tuple2) o;

            return Tuple.eq(a,t.a) && Tuple.eq(b,t.b);
        }

        @Override public int hashCode() {
            int result = a != null ? a.hashCode() : 0;
            result =  -1640531527 * result + (b != null ? b.hashCode() : 0);
            return result;
        }

        @Override public String toString() {
            return "Tuple2[" + a +", "+b+"]";
        }
    }