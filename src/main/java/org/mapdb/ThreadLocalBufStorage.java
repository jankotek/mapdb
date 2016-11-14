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

import java.util.AbstractQueue;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Queue;

/**
 * Thread local pool for node deserialization primitives
 *
 * @author yury.vasyutinskiy
 * @since 14/11/2016
 */
public class ThreadLocalBufStorage {
    private static int NUMBER_OF_INSTANCES = 10;

    private static ThreadLocal<Queue<long[][]>> buf16 = new ThreadLocal<Queue<long[][]>>() {
        @Override
        protected Queue<long[][]> initialValue() {
            Queue<long[][]> result = new CircularArrayQueue<>(NUMBER_OF_INSTANCES);
            for (int i = 0; i < NUMBER_OF_INSTANCES; i++) {
                result.add(new long[16][]);
            }
            return result;
        }
    };
    private static ThreadLocal<Queue<long[][]>> buf8 = new ThreadLocal<Queue<long[][]>>() {
        @Override
        protected Queue<long[][]> initialValue() {
            Queue<long[][]> result = new CircularArrayQueue<>(NUMBER_OF_INSTANCES);
            for (int i = 0; i < NUMBER_OF_INSTANCES; i++) {
                result.add(new long[16][8]);
            }
            return result;
        }
    };

    static long[][] borrowLong16Buf() {
        long[][] buf = buf16.get().poll();
        buf16.get().offer(buf);
        return buf;
    }

    static long[][] borrowLong8Bufs() {
        long[][] buf = buf8.get().poll();
        buf8.get().offer(buf);
        return buf;
    }

    private static class CircularArrayQueue<E> extends AbstractQueue<E> {
        private E[] q;
        private final int n; // size
        private int f = 0;
        private int r = 0;
        private int size = 0;


        CircularArrayQueue(int capacity){
            n = capacity;
            //noinspection unchecked
            q = (E[])new Object[n];
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean isEmpty() {
            return size == 0;
        }

        @Override
        public Object[] toArray() {
            return Arrays.copyOf(q, n);
        }

        boolean isFull() {
            return size == n;
        }

        @Override
        public boolean offer(E e) {
            if(size == n){
                return false;
            }else{
                q[r] = e;
                r = (r + 1) % n;
                size++;
                return true;
            }
        }

        @Override
        public E poll() {
            E item;
            if(size == 0){
                return null;
            }else{
                item = q[f];
                q[f] = null;
                f = (f + 1) % n;
                size--;
            }
            return item;
        }

        @Override
        public E peek() {
            E item;
            if(size == 0){
                return null;
            }else{
                item = q[f];
                f = (f + 1) % n;
            }
            return item;
        }

        @Override
        public Iterator<E> iterator() {
            return new Iterator<E>() {
                int pos = f;

                @Override
                public boolean hasNext() {
                    return pos <= r;
                }

                @Override
                public E next() {
                    E item = q[f];
                    pos = (pos + 1) % n;
                    return item;
                }
            };
        }
    }
}
