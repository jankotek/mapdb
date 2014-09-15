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


    private Fun(){}

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

    static public final class Pair<A,B> implements Comparable<Pair<A,B>>, Serializable {

    	private static final long serialVersionUID = -8816277286657643283L;
		
		final public A a;
        final public B b;

        public Pair(A a, B b) {
            this.a = a;
            this.b = b;
        }

        /** constructor used for deserialization*/
        protected Pair(SerializerBase serializer, DataInput in, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            this.a = (A) serializer.deserialize(in, objectStack);
            this.b =  (B) serializer.deserialize(in, objectStack);
        }


        @Override public int compareTo(Pair<A,B> o) {
               int i = ((Comparable)a).compareTo(o.a);
                if(i!=0)
                    return i;
                i = ((Comparable)b).compareTo(o.b);
                return i;

        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final Pair t = (Pair) o;

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

        public void copyIntoArray(Object[] array, int offset) {
            array[offset++] = a;
            array[offset]=b;
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


    public static <K,V> Fun.Function1<K,Pair<K,V>> extractKey(){
        return new Fun.Function1<K, Pair<K, V>>() {
            @Override
            public K run(Pair<K, V> t) {
                return t.a;
            }
        };
    }

    public static <K,V> Fun.Function1<V,Pair<K,V>> extractValue(){
        return new Fun.Function1<V, Pair<K, V>>() {
            @Override
            public V run(Pair<K, V> t) {
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
     * TODO document tuples
     *
     * Find all Primary Keys associated with Secondary Key.
     * This is useful companion to {@link Bind#mapInverse(org.mapdb.Bind.MapWithModificationListener, java.util.Set)}
     * and {@link Bind#secondaryKey(org.mapdb.Bind.MapWithModificationListener, java.util.Set, org.mapdb.Fun.Function2)}
     * It can by also used to find values from 'MultiMap'.
     *
     * @param set Set or 'MultiMap' to find values in
     * @param keys key to look from
     * @return all keys where primary value equals to `secondaryKey`
     */
    public static  Iterable<Object[]> filter(final NavigableSet<Object[]> set,  final Object... keys) {
        return new Iterable<Object[]>() {
            @Override
            public Iterator<Object[]> iterator() {
                final Iterator<Object[]> iter = set.tailSet(keys).iterator();

                if(!iter.hasNext())
                    return Fun.EMPTY_ITERATOR;

                return new Iterator<Object[]>() {

                    Object[] next = moveToNext();

                    Object[] moveToNext() {
                        if(!iter.hasNext())
                            return null;
                        Object[] next = iter.next();
                        if(next==null)
                            return null;
                        //check all elements are equal
                        //TODO this does not work if byte[] etc is used in array. Document or fail!
                        //TODO add special check for Fun.ARRAY comparator and use its sub-comparators
                        for(int i=0;i<keys.length;i++){
                            if(!keys[i].equals(next[i]))
                                return null;
                        }
                        return next;
                    }

                    @Override
                    public boolean hasNext() {
                        return next!=null;
                    }

                    @Override
                    public Object[] next() {
                        Object[] ret = next;
                        if(ret == null)
                            throw new NoSuchElementException();
                        next = moveToNext();
                        return ret;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
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
