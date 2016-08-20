package org.mapdb.tuple;

import java.io.Serializable;

import static org.mapdb.tuple.Tuple.eq;

final  public class Tuple5<A, B, C, D, E> implements Comparable<Tuple5<A,B,C,D,E>>, Serializable {

        private static final long serialVersionUID = 3975016300758650718L;

        final public A a;
        final public B b;
        final public C c;
        final public D d;
        final public E e;

        public Tuple5(A a, B b, C c, D d, E e) {
            this.a = a;
            this.b = b;
            this.c = c;
            this.d = d;
            this.e = e;
        }


        @Override
        public int compareTo(Tuple5<A,B,C,D,E> o) {
            return Tuple.TUPLE5_COMPARATOR.compare(this, o);
        }


        @Override
        public String toString() {
            return "Tuple5[" + a + ", " + b + ", " + c + ", " + d + ", " + e + "]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tuple5 t = (Tuple5) o;

            return eq(a,t.a) && eq(b,t.b) && eq(c,t.c) && eq(d,t.d) && eq(e,t.e);
        }

        @Override
        public int hashCode() {
            int result = a != null ? a.hashCode() : 0;
            result =  -1640531527 * result + (b != null ? b.hashCode() : 0);
            result =  -1640531527 * result + (c != null ? c.hashCode() : 0);
            result =  -1640531527 * result + (d != null ? d.hashCode() : 0);
            result =  -1640531527 * result + (e != null ? e.hashCode() : 0);
            return result;
        }
    }
