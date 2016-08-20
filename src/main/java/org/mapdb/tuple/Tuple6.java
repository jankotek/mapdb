package org.mapdb.tuple;

import static org.mapdb.tuple.Tuple.eq;

final public class Tuple6<A, B, C, D, E, F> implements Comparable<Tuple6<A, B, C, D, E, F>> {

        private static final long serialVersionUID = 7500397586163050718L;

        final public A a;
        final public B b;
        final public C c;
        final public D d;
        final public E e;
        final public F f;

        public Tuple6(A a, B b, C c, D d, E e, F f) {
            this.a = a;
            this.b = b;
            this.c = c;
            this.d = d;
            this.e = e;
            this.f = f;
        }

        @Override
        public int compareTo(Tuple6<A, B, C, D, E, F> o) {
            return Tuple.TUPLE6_COMPARATOR.compare(this, o);
        }


        @Override
        public String toString() {
            return "Tuple6[" + a + ", " + b + ", " + c + ", " + d + ", " + e + ", " + f + "]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tuple6 t = (Tuple6) o;

            return eq(a,t.a) && eq(b,t.b) && eq(c,t.c) && eq(d,t.d) && eq(e,t.e) && eq(f,t.f);
        }

        @Override
        public int hashCode() {
            int result = a != null ? a.hashCode() : 0;
            result =  -1640531527 * result + (b != null ? b.hashCode() : 0);
            result =  -1640531527 * result + (c != null ? c.hashCode() : 0);
            result =  -1640531527 * result + (d != null ? d.hashCode() : 0);
            result =  -1640531527 * result + (e != null ? e.hashCode() : 0);
            result =  -1640531527 * result + (f != null ? f.hashCode() : 0);
            return result;
        }
    }
