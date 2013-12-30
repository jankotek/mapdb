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
@SuppressWarnings({ "unchecked", "rawtypes" })
public final class Fun {


	public static final Comparator COMPARATOR = new Comparator<Comparable>() {
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

    private Fun(){}

    /** positive infinity object. Is larger than anything else. Used in tuple comparators.
     * Negative infinity is represented by 'null' */
    public static final Comparable HI = new Comparable(){
        @Override public String toString() {
            return "HI";
        }

        @Override
        public int compareTo(final Object o) {
            return o==HI?0:1; //always greater than anything else
        }
    };

    /** autocast version of `HI`*/
    public static final <A> A HI(){
        return (A) HI;
    }

    public static <A,B> Tuple2<A,B> t2(A a, B b) {
        return new Tuple2<A, B>(a,b);
    }

    public static <A,B,C> Tuple3<A,B,C> t3(A a, B b, C c) {
        return new Tuple3<A, B, C>((A)a, (B)b, (C)c);
    }

    public static <A,B,C,D> Tuple4<A,B,C,D> t4(A a, B b, C c, D d) {
        return new Tuple4<A, B, C, D>(a,b,c,d);
    }



    static public final class Tuple2<A,B> implements Comparable, Serializable {
		
    	private static final long serialVersionUID = -8816277286657643283L;
		
		final public A a;
        final public B b;

        public Tuple2(A a, B b) {
            this.a = a;
            this.b = b;
        }

        /** constructor used for deserilization*/
        protected Tuple2(SerializerBase serializer, DataInput in, SerializerBase.FastArrayList objectStack) throws IOException {
            objectStack.add(this);
            this.a = (A) serializer.deserialize(in, objectStack);
            this.b =  (B) serializer.deserialize(in, objectStack);
        }


        @Override public int compareTo(Object o) {
            return TUPLE2_COMPARATOR.compare(this, (Tuple2) o);
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final Tuple2 tuple2 = (Tuple2) o;

            if (a != null ? !a.equals(tuple2.a) : tuple2.a != null) return false;
            if (b != null ? !b.equals(tuple2.b) : tuple2.b != null) return false;

            return true;
        }

        @Override public int hashCode() {
            int result = a != null ? a.hashCode() : 0;
            result = 31 * result + (b != null ? b.hashCode() : 0);
            return result;
        }

        @Override public String toString() {
            return "Tuple2[" + a +", "+b+"]";
        }
    }

    final static public class Tuple3<A,B,C> implements Comparable, Serializable{

    	private static final long serialVersionUID = 11785034935947868L;
    	
		final public A a;
        final public B b;
        final public C c;

        public Tuple3(A a, B b, C c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }

        /** constructor used for deserilization, `extra` is added so the functions do not colide*/
        protected Tuple3(SerializerBase serializer, DataInput in, SerializerBase.FastArrayList objectStack, int extra) throws IOException {
            objectStack.add(this);
            this.a = (A) serializer.deserialize(in, objectStack);
            this.b =  (B) serializer.deserialize(in, objectStack);
            this.c =  (C) serializer.deserialize(in, objectStack);
        }


        @Override public int compareTo(Object o) {
            return TUPLE3_COMPARATOR.compare(this, (Tuple3) o);
        }


        @Override public String toString() {
            return "Tuple3[" + a +", "+b+", "+c+"]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tuple3 tuple3 = (Tuple3) o;

            if (a != null ? !a.equals(tuple3.a) : tuple3.a != null) return false;
            if (b != null ? !b.equals(tuple3.b) : tuple3.b != null) return false;
            if (c != null ? !c.equals(tuple3.c) : tuple3.c != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = a != null ? a.hashCode() : 0;
            result = 31 * result + (b != null ? b.hashCode() : 0);
            result = 31 * result + (c != null ? c.hashCode() : 0);
            return result;
        }
    }

    final static public class Tuple4<A,B,C,D> implements Comparable, Serializable{

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

        /** constructor used for deserilization*/
        protected Tuple4(SerializerBase serializer, DataInput in, SerializerBase.FastArrayList objectStack) throws IOException {
            objectStack.add(this);
            this.a = (A) serializer.deserialize(in, objectStack);
            this.b =  (B) serializer.deserialize(in, objectStack);
            this.c =  (C) serializer.deserialize(in, objectStack);
            this.d =  (D) serializer.deserialize(in, objectStack);
        }

        @Override public int compareTo(Object o) {
            return TUPLE4_COMPARATOR.compare(this, (Tuple4) o);
        }


        @Override public String toString() {
            return "Tuple4[" + a +", "+b+", "+c+", "+d+"]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tuple4 tuple4 = (Tuple4) o;

            if (a != null ? !a.equals(tuple4.a) : tuple4.a != null) return false;
            if (b != null ? !b.equals(tuple4.b) : tuple4.b != null) return false;
            if (c != null ? !c.equals(tuple4.c) : tuple4.c != null) return false;
            if (d != null ? !d.equals(tuple4.d) : tuple4.d != null) return false;

            return true;
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


    public static final class Tuple2Comparator<A,B> implements Comparator<Tuple2<A,B>>,Serializable {

        protected final Comparator a;
        protected final Comparator b;

        public Tuple2Comparator(Comparator<A> a, Comparator<B> b) {
            this.a = a==null? COMPARATOR :a;
            this.b = b==null? COMPARATOR :b;
        }


        /** constructor used for deserilization*/
        protected Tuple2Comparator(SerializerBase serializer, DataInput in, SerializerBase.FastArrayList objectStack) throws IOException {
            objectStack.add(this);
            this.a = (Comparator) serializer.deserialize(in, objectStack);
            this.b =  (Comparator) serializer.deserialize(in, objectStack);
        }

        @Override
        public int compare(final Tuple2 o1, final Tuple2 o2) {
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

        protected final Comparator a;
        protected final Comparator b;
        protected final Comparator c;

        public Tuple3Comparator(Comparator<A> a, Comparator<B> b, Comparator<C> c) {
            this.a = a==null? COMPARATOR :a;
            this.b = b==null? COMPARATOR :b;
            this.c = c==null? COMPARATOR :c;
        }

        /** constructor used for deserilization, `extra` is added just to make function not to collide*/
        protected Tuple3Comparator(SerializerBase serializer, DataInput in, SerializerBase.FastArrayList objectStack, int extra) throws IOException {
            objectStack.add(this);
            this.a = (Comparator) serializer.deserialize(in, objectStack);
            this.b =  (Comparator) serializer.deserialize(in, objectStack);
            this.c =  (Comparator) serializer.deserialize(in, objectStack);
        }

        @Override
        public int compare(final Tuple3 o1, final Tuple3 o2) {
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

        protected final Comparator a;
        protected final Comparator b;
        protected final Comparator c;
        protected final Comparator d;

        public Tuple4Comparator(Comparator<A> a, Comparator<B> b, Comparator<C> c, Comparator<C> d) {
            this.a = a==null? COMPARATOR :a;
            this.b = b==null? COMPARATOR :b;
            this.c = c==null? COMPARATOR :c;
            this.d = d==null? COMPARATOR :d;
        }

        /** constructor used for deserilization*/
        protected Tuple4Comparator(SerializerBase serializer, DataInput in, SerializerBase.FastArrayList objectStack) throws IOException {
            objectStack.add(this);
            this.a = (Comparator) serializer.deserialize(in, objectStack);
            this.b =  (Comparator) serializer.deserialize(in, objectStack);
            this.c =  (Comparator) serializer.deserialize(in, objectStack);
            this.d =  (Comparator) serializer.deserialize(in, objectStack);
        }

        @Override
        public int compare(final Tuple4 o1, final Tuple4 o2) {
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

    public interface Function1<R,A>{
        R run(A a);
    }

    public interface Function2<R,A,B>{
        R run(A a, B b);
    }

    public interface Runnable2<A,B>{
        void run(A a, B b);
    }

    public interface Runnable3<A,B,C>{
        void run(A a, B b, C c);
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
            return intCompare(o1.length, o2.length);
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
            return intCompare(o1.length, o2.length);
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
            return intCompare(o1.length, o2.length);
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
            return intCompare(o1.length, o2.length);
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
            return intCompare(o1.length, o2.length);
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
            return intCompare(o1.length, o2.length);
        }
    };

    /** compares two arrays using given comparators*/
    public static final class ArrayComparator implements Comparator<Object[]>{
        protected final Comparator[] comparators;

        public ArrayComparator(Comparator[] comparators2) {
            this.comparators = comparators2.clone();
            for(int i=0;i<this.comparators.length;i++){
                if(this.comparators[i]==null)
                    this.comparators[i] = Fun.COMPARATOR;
            }
        }

        /** constructor used for deserilization*/
        protected ArrayComparator(SerializerBase serializer, DataInput in, SerializerBase.FastArrayList objectStack) throws IOException {
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
            return intCompare(o1.length, o2.length);
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

    private static int intCompare(int x, int y) {
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
     * @param <K2> Secondary Key type
     * @param <K1> Primary Key type
     * @return all keys where primary value equals to `secondaryKey`
     */
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


}
