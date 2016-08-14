package org.mapdb.tuple;

import java.io.Serializable;

import static org.mapdb.tuple.Tuple.eq;

final public class Tuple4<A,B,C,D> implements Comparable<Tuple4<A,B,C,D>>, Serializable {

    	private static final long serialVersionUID = 1630397500758650718L;

    final public A a;
        final public B b;
        final public C c;
        final public D d;

        public Tuple4(A a, B b, C c, D d) {
            this.a = a;
            this.b = b;
            this.c = c;
            this.d = d;
        }

        @Override
        public int compareTo(Tuple4<A,B,C,D> o) {
            return Tuple.TUPLE4_COMPARATOR.compare(this, o);
        }


        @Override public String toString() {
            return "Tuple4[" + a +", "+b+", "+c+", "+d+"]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tuple4 t = (Tuple4) o;

            return eq(a,t.a) && eq(b,t.b) && eq(c,t.c) && eq(d,t.d);
        }

        @Override
        public int hashCode() {
            int result = a != null ? a.hashCode() : 0;
            result = 31 * result + (b != null ? b.hashCode() : 0);
            result = 31 * result + (c != null ? c.hashCode() : 0);
            result = 31 * result + (d != null ? d.hashCode() : 0);
            return result;
        }
    }
