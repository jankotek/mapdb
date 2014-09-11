/*
 *  Copyright (c) 2012 Jan Kotek
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.mapdb;

import java.io.DataInput;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Functional stuff. Tuples, function, callback methods etc..
 *
 * @author Jan Kotek
 */
public final class Fun {


	public static final Comparator COMPARATOR_NULLABLE = new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
            if(o1 == null)
                return o2 == null?0:-1;
            if(o2 == null) return 1;

            if(o1 == HI)
                return o2 == HI?0:1;
            if(o2 == HI) return -1;

            return o1.compareTo(o2);
        }
    };

    public static final Comparator COMPARATOR = new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
            return o1.compareTo(o2);
        }
    };

    public static final Comparator<Comparable> REVERSE_COMPARATOR = new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
            return -COMPARATOR.compare(o1,o2);
        }
    };


    /** empty iterator (note: Collections.EMPTY_ITERATOR is Java 7 specific and should not be used)*/
    public static final Iterator EMPTY_ITERATOR = new ArrayList(0).iterator();


    public static final Comparator<Tuple2> TUPLE2_COMPARATOR = new Tuple2Comparator(null,null);
    public static final Comparator<Tuple3> TUPLE3_COMPARATOR = new Tuple3Comparator(null,null,null);
    public static final Comparator<Tuple4> TUPLE4_COMPARATOR = new Tuple4Comparator(null,null,null,null);
    public static final Comparator<Tuple5> TUPLE5_COMPARATOR = new Tuple5Comparator(null,null,null,null,null);
    public static final Comparator<Tuple6> TUPLE6_COMPARATOR = new Tuple6Comparator(null,null,null,null,null,null);

    private Fun(){}

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

    public static <A,B> Tuple2<A,B> t2(A a, B b) {
        return new Tuple2<A, B>(a,b);
    }

    public static <A,B,C> Tuple3<A,B,C> t3(A a, B b, C c) {
        return new Tuple3<A, B, C>(a, b, c);
    }

    public static <A,B,C,D> Tuple4<A,B,C,D> t4(A a, B b, C c, D d) {
        return new Tuple4<A, B, C, D>(a,b,c,d);
    }

    public static <A,B,C,D,E> Tuple5<A,B,C,D,E> t5(A a, B b, C c, D d, E e) {
        return new Tuple5<A,B,C,D,E>(a,b,c,d,e);
    }

    public static <A,B,C,D,E,F> Tuple6<A,B,C,D,E,F> t6(A a, B b, C c, D d, E e, F f) {
        return new Tuple6<A,B,C,D,E,F>(a, b, c, d, e, f);
    }

    /** returns true if all elements are equal, works with nulls*/
    static public boolean eq(Object a, Object b) {
        return a==b || (a!=null && a.equals(b));
    }

    /** Convert object to string, even if it is primitive array */
    static String toString(Object keys) {
        if(keys instanceof long[])
            return Arrays.toString((long[]) keys);
        else if(keys instanceof int[])
            return Arrays.toString((int[]) keys);
        else if(keys instanceof byte[])
            return Arrays.toString((byte[]) keys);
        else if(keys instanceof char[])
            return Arrays.toString((char[]) keys);
        else if(keys instanceof float[])
            return Arrays.toString((float[]) keys);
        else if(keys instanceof double[])
            return Arrays.toString((double[]) keys);
        else  if(keys instanceof boolean[])
            return Arrays.toString((boolean[]) keys);
        else  if(keys instanceof Object[])
            return Arrays.toString((Object[]) keys);
        else
            return keys.toString();
    }

    public interface Tuple {
		
        void copyIntoArray(Object[] array, int offset);

        int compare(Comparator[] comparators, Object[] values, int offset);

        Object get(int i);
    }


    static public final class Tuple2<A,B> implements Comparable<Tuple2<A,B>>, Serializable, Tuple {

    	private static final long serialVersionUID = -8816277286657643283L;
		
		final public A a;
        final public B b;

        public Tuple2(A a, B b) {
            this.a = a;
            this.b = b;
        }

        /** constructor used for deserialization*/
        protected Tuple2(SerializerBase serializer, DataInput in, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            this.a = (A) serializer.deserialize(in, objectStack);
            this.b =  (B) serializer.deserialize(in, objectStack);
        }


        @Override public int compareTo(Tuple2<A,B> o) {
            return TUPLE2_COMPARATOR.compare(this, o);
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final Tuple2 t = (Tuple2) o;

            return eq(a,t.a) && eq(b,t.b);
        }

        @Override public int hashCode() {
            int result = a != null ? a.hashCode() : 0;
            result = 31 * result + (b != null ? b.hashCode() : 0);
            return result;
        }

        @Override public String toString() {
            return "Tuple2[" + a +", "+b+"]";
        }

        @Override
        public void copyIntoArray(Object[] array, int offset) {
            array[offset++] = a;
            array[offset]=b;
    }

        @Override
        public int compare(Comparator[] comparators, Object[] values, int offset) {
            int i = comparators[0].compare(a,values[offset++]);
            if(i!=0) return i;
            return comparators[1].compare(b,values[offset]);
         }

        @Override
        public Object get(int i) {
            switch(i){
                case 0: return a;
                default: return b;
            }
        }
    }

    final static public class Tuple3<A,B,C> implements Comparable<Tuple3<A,B,C>>, Serializable, Tuple {

    	private static final long serialVersionUID = 11785034935947868L;
    	
		final public A a;
        final public B b;
        final public C c;

        public Tuple3(A a, B b, C c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }

        /** constructor used for deserialization, `extra` is added so the functions do not colide*/
        protected Tuple3(SerializerBase serializer, DataInput in, SerializerBase.FastArrayList<Object> objectStack, int extra) throws IOException {
            objectStack.add(this);
            this.a = (A) serializer.deserialize(in, objectStack);
            this.b =  (B) serializer.deserialize(in, objectStack);
            this.c =  (C) serializer.deserialize(in, objectStack);
        }


        @Override
        public int compareTo(Tuple3<A,B,C> o) {
            return TUPLE3_COMPARATOR.compare(this, o);
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
            result = 31 * result + (b != null ? b.hashCode() : 0);
            result = 31 * result + (c != null ? c.hashCode() : 0);
            return result;
        }

        @Override
        public void copyIntoArray(Object[] array, int offset) {
            array[offset++]=a;
            array[offset++]=b;
            array[offset]=c;
    }

        @Override
        public int compare(Comparator[] comparators, Object[] values, int offset) {
            int i = comparators[0].compare(a,values[offset++]);
            if(i!=0) return i;
            i = comparators[1].compare(b,values[offset++]);
            if(i!=0) return i;
            return comparators[2].compare(c,values[offset]);
        }

        @Override
        public Object get(int i) {
            switch(i){
                case 0: return a;
                case 1: return b;
                default: return c;
            }
        }

    }

    final static public class Tuple4<A,B,C,D> implements Comparable<Tuple4<A,B,C,D>>, Serializable, Tuple {

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

        /** constructor used for deserialization*/
        protected Tuple4(SerializerBase serializer, DataInput in, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            this.a = (A) serializer.deserialize(in, objectStack);
            this.b =  (B) serializer.deserialize(in, objectStack);
            this.c =  (C) serializer.deserialize(in, objectStack);
            this.d =  (D) serializer.deserialize(in, objectStack);
        }

        @Override
        public int compareTo(Tuple4<A,B,C,D> o) {
            return TUPLE4_COMPARATOR.compare(this, o);
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

        @Override
        public void copyIntoArray(Object[] array, int offset) {
            array[offset++] = a;
            array[offset++]=b;
            array[offset++]=c;
            array[offset]=d;
    }


        @Override
        public int compare(Comparator[] comparators, Object[] values, int offset) {
            int i = comparators[0].compare(a,values[offset++]);
            if(i!=0) return i;
            i = comparators[1].compare(b,values[offset++]);
            if(i!=0) return i;
            i =  comparators[2].compare(c,values[offset++]);
            if(i!=0) return i;
            return comparators[3].compare(d,values[offset]);
        }

        @Override
        public Object get(int i) {
            switch(i){
                case 0: return a;
                case 1: return b;
                case 2: return c;
                default: return d;
            }
        }
    }


    final static public class Tuple5<A, B, C, D, E> implements Comparable<Tuple5<A,B,C,D,E>>, Serializable, Tuple {

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

        /**
         * constructor used for deserialization
         */
        protected Tuple5(SerializerBase serializer, DataInput in, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            this.a = (A) serializer.deserialize(in, objectStack);
            this.b = (B) serializer.deserialize(in, objectStack);
            this.c = (C) serializer.deserialize(in, objectStack);
            this.d = (D) serializer.deserialize(in, objectStack);
            this.e = (E) serializer.deserialize(in, objectStack);
        }

        @Override
        public int compareTo(Tuple5<A,B,C,D,E> o) {
            return TUPLE5_COMPARATOR.compare(this, o);
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
            result = 31 * result + (b != null ? b.hashCode() : 0);
            result = 31 * result + (c != null ? c.hashCode() : 0);
            result = 31 * result + (d != null ? d.hashCode() : 0);
            result = 31 * result + (e != null ? e.hashCode() : 0);
            return result;
        }

        @Override
        public int compare(Comparator[] comparators, Object[] values, int offset) {
            int i = comparators[0].compare(a, values[offset++]);
            if(i!=0) return i;
            i = comparators[1].compare(b, values[offset++]);
            if(i!=0) return i;
            i =  comparators[2].compare(c,values[offset++]);
            if(i!=0) return i;
            i =  comparators[3].compare(d,values[offset++]);
            if(i!=0) return i;
            return comparators[4].compare(e,values[offset]);
    }

        @Override
        public void copyIntoArray(Object[] array, int offset) {
            array[offset++] = a;
            array[offset++]=b;
            array[offset++]=c;
            array[offset++]=d;
            array[offset]=e;
        }

        @Override
        public Object get(int i) {
            switch(i){
                case 0: return a;
                case 1: return b;
                case 2: return c;
                case 3: return d;
                default: return e;
            }
        }
    }


    final static public class Tuple6<A, B, C, D, E, F> implements Comparable<Tuple6<A, B, C, D, E, F>>, Serializable, Tuple {

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

        /**
         * constructor used for deserialization
         */
        protected Tuple6(SerializerBase serializer, DataInput in, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            this.a = (A) serializer.deserialize(in, objectStack);
            this.b = (B) serializer.deserialize(in, objectStack);
            this.c = (C) serializer.deserialize(in, objectStack);
            this.d = (D) serializer.deserialize(in, objectStack);
            this.e = (E) serializer.deserialize(in, objectStack);
            this.f = (F) serializer.deserialize(in, objectStack);
        }

        @Override
        public int compareTo(Tuple6<A, B, C, D, E, F> o) {
            return TUPLE6_COMPARATOR.compare(this, o);
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
            result = 31 * result + (b != null ? b.hashCode() : 0);
            result = 31 * result + (c != null ? c.hashCode() : 0);
            result = 31 * result + (d != null ? d.hashCode() : 0);
            result = 31 * result + (e != null ? e.hashCode() : 0);
            result = 31 * result + (f != null ? f.hashCode() : 0);
            return result;
        }

        @Override
        public void copyIntoArray(Object[] array, int offset) {
            array[offset++] = a;
            array[offset++]=b;
            array[offset++]=c;
            array[offset++]=d;
            array[offset++]=e;
            array[offset]=f;
    }

        @Override
        public int compare(Comparator[] comparators, Object[] values, int offset) {
            int i = comparators[0].compare(a,values[offset++]);
            if(i!=0) return i;
            i = comparators[1].compare(b,values[offset++]);
            if(i!=0) return i;
            i =  comparators[2].compare(c,values[offset++]);
            if(i!=0) return i;
            i =  comparators[3].compare(d,values[offset++]);
            if(i!=0) return i;
            i =  comparators[4].compare(e,values[offset++]);
            if(i!=0) return i;
            return comparators[5].compare(f,values[offset]);
        }

        @Override
        public Object get(int i) {
            switch(i){
                case 0: return a;
                case 1: return b;
                case 2: return c;
                case 3: return d;
                case 4: return e;
                default: return f;
            }
        }
    }

    public static final class Tuple2Comparator<A,B> implements Comparator<Tuple2<A,B>>,Serializable {

        private static final long serialVersionUID = 1156568632023474010L;

        protected final Comparator<A> a;
        protected final Comparator<B> b;

        public Tuple2Comparator(Comparator<A> a, Comparator<B> b) {
            this.a = a==null? COMPARATOR_NULLABLE   :a;
            this.b = b==null? COMPARATOR_NULLABLE   :b;
        }


        /** constructor used for deserialization*/
        protected Tuple2Comparator(SerializerBase serializer, DataInput in, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            this.a = (Comparator) serializer.deserialize(in, objectStack);
            this.b =  (Comparator) serializer.deserialize(in, objectStack);
        }

        @Override
        public int compare(final Tuple2<A,B> o1, final Tuple2<A,B> o2) {
            int i = a.compare(o1.a,o2.a);
            if(i!=0) return i;
            i = b.compare(o1.b,o2.b);
            return i;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tuple2Comparator that = (Tuple2Comparator) o;

            return a.equals(that.a) && b.equals(that.b);
        }

        @Override
        public int hashCode() {
            int result = a.hashCode();
            result = 31 * result + b.hashCode();
            return result;
        }
    }

    public static final class Tuple3Comparator<A,B,C> implements Comparator<Tuple3<A,B,C>>,Serializable  {

        private static final long serialVersionUID = 6908945189367914695L;

        protected final Comparator<A> a;
        protected final Comparator<B> b;
        protected final Comparator<C> c;

        public Tuple3Comparator(Comparator<A> a, Comparator<B> b, Comparator<C> c) {
            this.a = a==null? COMPARATOR_NULLABLE   :a;
            this.b = b==null? COMPARATOR_NULLABLE   :b;
            this.c = c==null? COMPARATOR_NULLABLE   :c;
        }

        /** constructor used for deserialization, `extra` is added just to make function not to collide*/
        protected Tuple3Comparator(SerializerBase serializer, DataInput in, SerializerBase.FastArrayList<Object> objectStack, int extra) throws IOException {
            objectStack.add(this);
            this.a = (Comparator) serializer.deserialize(in, objectStack);
            this.b =  (Comparator) serializer.deserialize(in, objectStack);
            this.c =  (Comparator) serializer.deserialize(in, objectStack);
        }

        @Override
        public int compare(final Tuple3<A,B,C> o1, final Tuple3<A,B,C> o2) {
            int i = a.compare(o1.a,o2.a);
            if(i!=0) return i;
            i = b.compare(o1.b,o2.b);
            if(i!=0) return i;
            return c.compare(o1.c,o2.c);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tuple3Comparator that = (Tuple3Comparator) o;
            return a.equals(that.a) && b.equals(that.b) && c.equals(that.c);
        }

        @Override
        public int hashCode() {
            int result = a.hashCode();
            result = 31 * result + b.hashCode();
            result = 31 * result + c.hashCode();
            return result;
        }
    }

    public static final class Tuple4Comparator<A,B,C,D> implements Comparator<Tuple4<A,B,C,D>>,Serializable  {

        private static final long serialVersionUID = 4994247318830102213L;

        protected final Comparator<A> a;
        protected final Comparator<B> b;
        protected final Comparator<C> c;
        protected final Comparator<D> d;

        public Tuple4Comparator(Comparator<A> a, Comparator<B> b, Comparator<C> c, Comparator<D> d) {
            this.a = a==null? COMPARATOR_NULLABLE   :a;
            this.b = b==null? COMPARATOR_NULLABLE   :b;
            this.c = c==null? COMPARATOR_NULLABLE   :c;
            this.d = d==null? COMPARATOR_NULLABLE   :d;
        }

        /** constructor used for deserialization*/
        protected Tuple4Comparator(SerializerBase serializer, DataInput in, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            this.a = (Comparator) serializer.deserialize(in, objectStack);
            this.b =  (Comparator) serializer.deserialize(in, objectStack);
            this.c =  (Comparator) serializer.deserialize(in, objectStack);
            this.d =  (Comparator) serializer.deserialize(in, objectStack);
        }

        @Override
        public int compare(final Tuple4<A,B,C,D> o1, final Tuple4<A,B,C,D> o2) {
            int i = a.compare(o1.a,o2.a);
            if(i!=0) return i;
            i = b.compare(o1.b,o2.b);
            if(i!=0) return i;
            i = c.compare(o1.c,o2.c);
            if(i!=0) return i;
            return d.compare(o1.d,o2.d);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tuple4Comparator that = (Tuple4Comparator) o;
            return a.equals(that.a) && b.equals(that.b) && c.equals(that.c) && d.equals(that.d);
        }

        @Override
        public int hashCode() {
            int result = a.hashCode();
            result = 31 * result + b.hashCode();
            result = 31 * result + c.hashCode();
            result = 31 * result + d.hashCode();
            return result;
        }
    }


    public static final class Tuple5Comparator<A,B,C,D,E> implements Comparator<Tuple5<A,B,C,D,E>>, Serializable {

        private static final long serialVersionUID = -6571610438255691118L;

        protected final Comparator<A> a;
        protected final Comparator<B> b;
        protected final Comparator<C> c;
        protected final Comparator<D> d;
        protected final Comparator<E> e;


        public Tuple5Comparator(Comparator<A> a, Comparator<B> b, Comparator<C> c, Comparator<D> d, Comparator<E> e) {
            this.a = a == null ? COMPARATOR_NULLABLE   : a;
            this.b = b == null ? COMPARATOR_NULLABLE   : b;
            this.c = c == null ? COMPARATOR_NULLABLE   : c;
            this.d = d == null ? COMPARATOR_NULLABLE   : d;
            this.e = e == null ? COMPARATOR_NULLABLE   : e;
        }

        /**
         * constructor used for deserialization
         */
        protected Tuple5Comparator(SerializerBase serializer, DataInput in, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            this.a = (Comparator) serializer.deserialize(in, objectStack);
            this.b = (Comparator) serializer.deserialize(in, objectStack);
            this.c = (Comparator) serializer.deserialize(in, objectStack);
            this.d = (Comparator) serializer.deserialize(in, objectStack);
            this.e = (Comparator) serializer.deserialize(in, objectStack);
        }

        @Override
        public int compare(final Tuple5<A,B,C,D,E> o1, final Tuple5<A,B,C,D,E> o2) {
            int i = a.compare(o1.a, o2.a);
            if (i != 0) return i;
            i = b.compare(o1.b, o2.b);
            if (i != 0) return i;
            i = c.compare(o1.c, o2.c);
            if (i != 0) return i;
            i = d.compare(o1.d, o2.d);
            if (i != 0) return i;
            return e.compare(o1.e, o2.e);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tuple5Comparator that = (Tuple5Comparator) o;
            return a.equals(that.a) && b.equals(that.b) && c.equals(that.c) && d.equals(that.d) && e.equals(that.e);
        }

        @Override
        public int hashCode() {
            int result = a.hashCode();
            result = 31 * result + b.hashCode();
            result = 31 * result + c.hashCode();
            result = 31 * result + d.hashCode();
            result = 31 * result + e.hashCode();
            return result;
        }
    }

    public static final class Tuple6Comparator<A,B,C,D,E,F> implements Comparator<Tuple6<A,B,C,D,E,F>>, Serializable {

        private static final long serialVersionUID = 4254578670751190479L;

        protected final Comparator<A> a;
        protected final Comparator<B> b;
        protected final Comparator<C> c;
        protected final Comparator<D> d;
        protected final Comparator<E> e;
        protected final Comparator<F> f;


        public Tuple6Comparator(Comparator<A> a, Comparator<B> b, Comparator<C> c, Comparator<D> d, Comparator<E> e, Comparator<F> f) {
            this.a = a == null ? COMPARATOR_NULLABLE   : a;
            this.b = b == null ? COMPARATOR_NULLABLE   : b;
            this.c = c == null ? COMPARATOR_NULLABLE   : c;
            this.d = d == null ? COMPARATOR_NULLABLE   : d;
            this.e = e == null ? COMPARATOR_NULLABLE   : e;
            this.f = f == null ? COMPARATOR_NULLABLE   : f;
        }

        /**
         * constructor used for deserialization
         */
        protected Tuple6Comparator(SerializerBase serializer, DataInput in, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            this.a = (Comparator) serializer.deserialize(in, objectStack);
            this.b = (Comparator) serializer.deserialize(in, objectStack);
            this.c = (Comparator) serializer.deserialize(in, objectStack);
            this.d = (Comparator) serializer.deserialize(in, objectStack);
            this.e = (Comparator) serializer.deserialize(in, objectStack);
            this.f = (Comparator) serializer.deserialize(in, objectStack);
        }

        @Override
        public int compare(final Tuple6<A,B,C,D,E,F> o1, final Tuple6<A,B,C,D,E,F> o2) {
            int i = a.compare(o1.a, o2.a);
            if (i != 0) return i;
            i = b.compare(o1.b, o2.b);
            if (i != 0) return i;
            i = c.compare(o1.c, o2.c);
            if (i != 0) return i;
            i = d.compare(o1.d, o2.d);
            if (i != 0) return i;
            i = e.compare(o1.e, o2.e);
            if (i != 0) return i;
            return f.compare(o1.f, o2.f);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tuple6Comparator that = (Tuple6Comparator) o;
            return a.equals(that.a) && b.equals(that.b) && c.equals(that.c) && d.equals(that.d) && e.equals(that.e) && f.equals(that.f);
        }

        @Override
        public int hashCode() {
            int result = a.hashCode();
            result = 31 * result + b.hashCode();
            result = 31 * result + c.hashCode();
            result = 31 * result + d.hashCode();
            result = 31 * result + e.hashCode();
            result = 31 * result + f.hashCode();
            return result;
        }
    }


    /**
     * Used to run background threads.
     * Unlike {@link java.util.concurrent.ThreadFactory} it does not give access to threads,
     * so tasks can run inside {@link java.util.concurrent.Executor}.
     *
     * There are some expectations from submitted tasks:
     *
     *  * Background tasks is started within reasonable delay. You can not block if thread pool is full.
     *      That could cause memory leak since queues are not flushed etc..
     *
     *  * Runnable code might pause and call {@link Thread#sleep(long)}.
     *
     *  * Threads must not be interrupted or terminated. Using daemon thread is forbidden.
     *      Runnable will exit itself, once db is closed.
     *
     */
    public interface ThreadFactory{

        /** Basic thread factory which starts new thread for each runnable */
        public static final ThreadFactory BASIC = new ThreadFactory() {
            @Override
            public void newThread(String threadName, Runnable runnable) {
                new Thread(runnable,threadName).start();
            }
        };

        /** execute new runnable. Optionally you can name thread using `threadName` argument */
        void newThread(String threadName, Runnable runnable);
    }

    /** function which takes no argument and returns one value*/
    public interface Function0<R>{
        R run();
    }

    /** function which takes one argument and returns one value*/
    public interface Function1<R,A>{
        R run(A a);
    }

    /** function which takes two argument and returns one value*/
    public interface Function2<R,A,B>{
        R run(A a, B b);
    }


    public static <K,V> Fun.Function1<K,Fun.Tuple2<K,V>> extractKey(){
        return new Fun.Function1<K, Fun.Tuple2<K, V>>() {
            @Override
            public K run(Fun.Tuple2<K, V> t) {
                return t.a;
            }
        };
    }

    public static <K,V> Fun.Function1<V,Fun.Tuple2<K,V>> extractValue(){
        return new Fun.Function1<V, Fun.Tuple2<K, V>>() {
            @Override
            public V run(Fun.Tuple2<K, V> t) {
                return t.b;
            }
        };
    }


    /** returns function which always returns the value itself without transformation */
    public static <K> Function1<K,K> extractNoTransform() {
        return new Function1<K, K>() {
            @Override
            public K run(K k) {
                return k;
            }
        };
    }


    public static final Comparator<byte[]> BYTE_ARRAY_COMPARATOR = new Comparator<byte[]>() {
        @Override
        public int compare(byte[] o1, byte[] o2) {
            if(o1==o2) return 0;
            final int len = Math.min(o1.length,o2.length);
            for(int i=0;i<len;i++){
                if(o1[i]==o2[i])
                    continue;
                if(o1[i]>o2[i])
                    return 1;
                return -1;
            }
            return compareInt(o1.length, o2.length);
        }
    };


    public static final Comparator<char[]> CHAR_ARRAY_COMPARATOR = new Comparator<char[]>() {
        @Override
        public int compare(char[] o1, char[] o2) {
            if(o1==o2) return 0;
            final int len = Math.min(o1.length,o2.length);
            for(int i=0;i<len;i++){
                if(o1[i]==o2[i])
                    continue;
                if(o1[i]>o2[i])
                    return 1;
                return -1;
            }
            return compareInt(o1.length, o2.length);
        }
    };

    public static final Comparator<int[]> INT_ARRAY_COMPARATOR = new Comparator<int[]>() {
        @Override
        public int compare(int[] o1, int[] o2) {
            if(o1==o2) return 0;
            final int len = Math.min(o1.length,o2.length);
            for(int i=0;i<len;i++){
                if(o1[i]==o2[i])
                    continue;
                if(o1[i]>o2[i])
                    return 1;
                return -1;
            }
            return compareInt(o1.length, o2.length);
        }
    };

    public static final Comparator<long[]> LONG_ARRAY_COMPARATOR = new Comparator<long[]>() {
        @Override
        public int compare(long[] o1, long[] o2) {
            if(o1==o2) return 0;
            final int len = Math.min(o1.length,o2.length);
            for(int i=0;i<len;i++){
                if(o1[i]==o2[i])
                    continue;
                if(o1[i]>o2[i])
                    return 1;
                return -1;
            }
            return compareInt(o1.length, o2.length);
        }
    };

    public static final Comparator<double[]> DOUBLE_ARRAY_COMPARATOR = new Comparator<double[]>() {
        @Override
        public int compare(double[] o1, double[] o2) {
            if(o1==o2) return 0;
            final int len = Math.min(o1.length,o2.length);
            for(int i=0;i<len;i++){
                if(o1[i]==o2[i])
                    continue;
                if(o1[i]>o2[i])
                    return 1;
                return -1;
            }
            return compareInt(o1.length, o2.length);
        }
    };


    /** Compares two arrays which contains comparable elements */
    public static final Comparator<Object[]> COMPARABLE_ARRAY_COMPARATOR = new Comparator<Object[]>() {
        @Override
        public int compare(Object[] o1, Object[] o2) {
            if(o1==o2) return 0;
            final int len = Math.min(o1.length,o2.length);
            for(int i=0;i<len;i++){
                int r = Fun.COMPARATOR.compare(o1[i],o2[i]);
                if(r!=0)
                    return r;
            }
            return compareInt(o1.length, o2.length);
        }
    };

    /** compares two arrays using given comparators*/
    public static final class ArrayComparator implements Comparator<Object[]>{
        protected final Comparator[] comparators;

        public ArrayComparator(Comparator<?>[] comparators2) {
            this.comparators = comparators2.clone();
            for(int i=0;i<this.comparators.length;i++){
                if(this.comparators[i]==null)
                    this.comparators[i] = Fun.COMPARATOR;
            }
        }

        /** constructor used for deserialization*/
        protected ArrayComparator(SerializerBase serializer, DataInput in, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            this.comparators = (Comparator[]) serializer.deserialize(in, objectStack);
        }


        @Override
        public int compare(Object[] o1, Object[] o2) {
            if(o1==o2) return 0;
            int len = Math.min(o1.length,o2.length);
            for(int i=0;i<len;i++){
                int r = comparators[i].compare(o1[i],o2[i]);
                if(r!=0)
                    return r;
            }
            return compareInt(o1.length, o2.length);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ArrayComparator that = (ArrayComparator) o;
            return Arrays.equals(comparators, that.comparators);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(comparators);
        }
    }


    public static int compareInt(int x, int y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    public static int compareLong(long x, long y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    /**
     * Find all Primary Keys associated with Secondary Key.
     * This is useful companion to {@link Bind#mapInverse(org.mapdb.Bind.MapWithModificationListener, java.util.Set)}
     * and {@link Bind#secondaryKey(org.mapdb.Bind.MapWithModificationListener, java.util.Set, org.mapdb.Fun.Function2)}
     * It can by also used to find values from 'MultiMap'.
     *
     * @param secondaryKeys Secondary Set or 'MultiMap' to find values in
     * @param secondaryKey key to look from
     * @return all keys where primary value equals to `secondaryKey`
     *
     */
    //TODO there is an idea this could return NavigableSet instead of Iterable
    public static <K2,K1> Iterable<K1> filter(final NavigableSet<Fun.Tuple2<K2, K1>> secondaryKeys, final K2 secondaryKey) {
        return filter(secondaryKeys, secondaryKey, true, secondaryKey, true);
    }
    public static <K2,K1> Iterable<K1> filter(final NavigableSet<Fun.Tuple2<K2, K1>> secondaryKeys,
                                              final K2 lo, final boolean loInc, final K2 hi, final boolean hiInc) {
        return new Iterable<K1>(){
            @Override
            public Iterator<K1> iterator() {
                //use range query to get all values
                final Iterator<Fun.Tuple2<K2,K1>> iter =
                        ((NavigableSet)secondaryKeys) //cast is workaround for generics
                                .subSet(
                                        Fun.t2(lo,null), loInc,//NULL represents lower bound, everything is larger than null
                                        Fun.t2(hi,hiInc?Fun.HI:null),hiInc // HI is upper bound everything is smaller then HI
                                ).iterator();

                return new Iterator<K1>() {
                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public K1 next() {
                        return iter.next().b;
                    }

                    @Override
                    public void remove() {
                        iter.remove();
                    }
                };
            }
        };

    }


    public static <A,B,C> Iterable<C> filter(final NavigableSet<Fun.Tuple3<A, B, C>> secondaryKeys,
                                             final A a, final B b) {
        return new Iterable<C>(){
            @Override
            public Iterator<C> iterator() {
                //use range query to get all values
                final Iterator<Fun.Tuple3> iter =
                        ((NavigableSet)secondaryKeys) //cast is workaround for generics
                                .subSet(
                                        Fun.t3(a, b, null), //NULL represents lower bound, everything is larger than null
                                        Fun.t3(a,b==null?Fun.HI():b,Fun.HI()) // HI is upper bound everything is smaller then HI
                                ).iterator();

                return new Iterator<C>() {
                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public C next() {
                        return (C) iter.next().c;
                    }

                    @Override
                    public void remove() {
                        iter.remove();
                    }
                };
            }
        };

    }

    public static <A,B,C,D> Iterable<D> filter(final NavigableSet<Fun.Tuple4<A, B, C, D>> secondaryKeys, final A a, final B b, final C c) {
        return new Iterable<D>(){
            @Override
            public Iterator<D> iterator() {
                //use range query to get all values
                final Iterator<Fun.Tuple4> iter =
                        ((NavigableSet)secondaryKeys) //cast is workaround for generics
                                .subSet(
                                        Fun.t4(a,b,c, null), //NULL represents lower bound, everything is larger than null
                                        Fun.t4(a,b==null?Fun.HI():b,c==null?Fun.HI():c,Fun.HI()) // HI is upper bound everything is smaller then HI
                                ).iterator();

                return new Iterator<D>() {
                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public D next() {
                        return (D) iter.next().d;
                    }

                    @Override
                    public void remove() {
                        iter.remove();
                    }
                };
            }
        };

    }


    /** decides if some action should be executed on an record*/
    public interface RecordCondition<A>{
        boolean run(final long recid, final A value, final Serializer<A> serializer);
    }

    /**  record condition which always returns true*/
    public static final RecordCondition RECORD_ALWAYS_TRUE = new RecordCondition() {
        @Override
        public boolean run(long recid, Object value, Serializer serializer) {
            return true;
        }
    };



}
