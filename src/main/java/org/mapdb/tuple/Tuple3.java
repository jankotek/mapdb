package org.mapdb.tuple;

import java.io.Serializable;

import static org.mapdb.tuple.Tuple.eq;

final  public class Tuple3<A,B,C> implements Comparable<Tuple3<A,B,C>>, Serializable {

    	private static final long serialVersionUID = 11785034935947868L;

    final public A a;
        final public B b;
        final public C c;

        public Tuple3(A a, B b, C c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }


        @Override
        public int compareTo(Tuple3<A,B,C> o) {
            return Tuple.TUPLE3_COMPARATOR.compare(this, o);
        }


        @Override public String toString() {
            return "Tuple3[" + a +", "+b+", "+c+"]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tuple3 t = (Tuple3) o;
            return eq(a,t.a) && eq(b,t.b) && eq(c,t.c);
        }

        @Override
        public int hashCode() {
            int result = a != null ? a.hashCode() : 0;
            result =  -1640531527 * result + (b != null ? b.hashCode() : 0);
            result =  -1640531527 * result + (c != null ? c.hashCode() : 0);
            return result;
        }
    }