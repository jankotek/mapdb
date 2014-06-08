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
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Various queue algorithms
 */
public final class Queues {

    private Queues(){}



    public static abstract class SimpleQueue<E> implements BlockingQueue<E> {

        protected final boolean useLocks;
        protected final ReentrantLock[] locks;


        protected static final int TICK = 10*1000;

        protected final Engine engine;
        protected final Serializer<E> serializer;

        protected final Atomic.Long head;


        protected static class NodeSerializer<E> implements Serializer<Node<E>> {
            private final Serializer<E> serializer;

            public NodeSerializer(Serializer<E> serializer) {
                this.serializer = serializer;
            }

            @Override
            public void serialize(DataOutput out, Node<E> value) throws IOException {
                if(value==Node.EMPTY) return;
                DataOutput2.packLong(out,value.next);
                serializer.serialize(out, value.value);
            }

            @Override
            public Node<E> deserialize(DataInput in, int available) throws IOException {
                if(available==0)return (Node<E>) Node.EMPTY;
                return new Node<E>(DataInput2.unpackLong(in), serializer.deserialize(in,-1));
            }

            @Override
            public int fixedSize() {
                return -1;
            }

        }

        protected final Serializer<Node<E>> nodeSerializer;


        public SimpleQueue(Engine engine, Serializer<E> serializer, long headRecidRef, boolean useLocks) {
            this.engine = engine;
            this.serializer = serializer;
            head = new Atomic.Long(engine,headRecidRef);
            nodeSerializer = new NodeSerializer(serializer);
            this.useLocks = useLocks;
            if(useLocks){
                locks = new ReentrantLock[CC.CONCURRENCY];
                for(int i=0;i<locks.length;i++)
                    locks[i] = new ReentrantLock(CC.FAIR_LOCKS);
            }else{
                locks = null;
            }


        }


        /**
         * Closes underlying storage and releases all resources.
         * Used mostly with temporary collections where engine is not accessible.
         */
        public void close(){
            engine.close();
        }


        @Override
        public E peek() {
            final long head2 = head.get();
            if(useLocks)locks[Store.lockPos(head2)].lock();
            try{
                Node n = engine.get(head2,nodeSerializer);
                if(n==Node.EMPTY)
                    return null; //empty queue
                return (E) n.value;
            }finally{
                if(useLocks)locks[Store.lockPos(head2)].unlock();
            }
        }


        @Override
        public E poll() {
            for(;;){
                final long head2 = head.get();
                if(useLocks)locks[Store.lockPos(head2)].lock();
                try{
                    Node n = engine.get(head2,nodeSerializer);
                    if(n==Node.EMPTY)
                        return null; //empty queue

                    //update head
                    if(head.compareAndSet(head2,n.next)){
                        //updated fine, so we can take a value
                        if(useLocks){
                            engine.delete(head2,nodeSerializer);
                        }else{
                            engine.update(head2, (Node<E>) Node.EMPTY,nodeSerializer);
                        }
                        return (E) n.value;
                    }

                }finally{
                    if(useLocks)locks[Store.lockPos(head2)].unlock();
                }
            }
        }


        protected static final class Node<E>{

            protected static final Node<?> EMPTY = new Node(0L, null);

            final protected long next;
            final protected E value;

            public Node(long next, E value) {
                this.next = next;
                this.value = value;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Node node = (Node) o;

                if (next != node.next) return false;
                if (value != null ? !value.equals(node.value) : node.value != null) return false;

                return true;
            }

            @Override
            public int hashCode() {
                int result = (int) (next ^ (next >>> 32));
                result = 31 * result + (value != null ? value.hashCode() : 0);
                return result;
            }
        }

        @Override
        public void clear() {
            while(!isEmpty())
                poll();
        }


        @Override
        public E remove() {
            E ret = poll();
            if(ret == null) throw new NoSuchElementException();
            return ret;
        }


        @Override
        public E element() {
            E ret = peek();
            if(ret == null) throw new NoSuchElementException();
            return ret;
        }


        @Override
        public boolean offer(E e) {
            try {
                return add(e);
            }catch (IllegalStateException ee){
                return false;
            }
        }


        @Override
        public void put(E e) throws InterruptedException {
            while(!offer(e)){
                Thread.sleep(0,TICK);
            }
        }

        @Override
        public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
            if(offer(e)) return true;
            long target = System.currentTimeMillis() + unit.toMillis(timeout);
            while(target>=System.currentTimeMillis()){
                if(offer(e))
                    return true;
                Thread.sleep(0,TICK);
            }

            return false;
        }

        @Override
        public E take() throws InterruptedException {
            E e = poll();
            while(e==null){
                Thread.sleep(0,TICK);
                e = poll();
            }
            return e;
        }

        @Override
        public E poll(long timeout, TimeUnit unit) throws InterruptedException {
            E e = poll();
            if(e!=null) return e;
            long target = System.currentTimeMillis() + unit.toMillis(timeout);
            while(target>=System.currentTimeMillis()){
                Thread.sleep(0,TICK);
                e = poll();
                if(e!=null)
                    return e;
            }
            return null;
        }

        @Override
        public int drainTo(Collection<? super E> c) {
            return drainTo(c,Integer.MAX_VALUE);
        }

        @Override
        public int drainTo(Collection<? super E> c, int maxElements) {
            int counter=0;
            while(counter<maxElements){
                E e = poll();
                if(e==null)
                    return counter;
                c.add(e);
                counter++;
            }
            return counter;
        }

        @Override
        public int remainingCapacity() {
            return Integer.MAX_VALUE;
        }

        @Override
        public boolean isEmpty() {
            return peek()==null;
        }


        @Override
        public int size() {
            throw new UnsupportedOperationException();
        }


        @Override
        public boolean contains(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<E> iterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object[] toArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            throw new UnsupportedOperationException();
        }


        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(Collection<? extends E> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }
    }



    /**
     * Last in first out lock-free queue
     *
     * @param <E>
     */
    public static class Stack<E> extends SimpleQueue<E> {



        public Stack(Engine engine,  Serializer<E> serializer, long headerRecidRef, boolean useLocks) {
            super(engine, serializer, headerRecidRef, useLocks);
        }

        @Override
        public boolean add(E e) {
            long head2 = head.get();
            Node<E> n = new Node<E>(head2, e);
            long recid = engine.put(n, nodeSerializer);
            while(!head.compareAndSet(head2, recid)){
                //failed to update head, so read new value and start over
                head2 = head.get();
                n = new Node<E>(head2, e);
                engine.update(recid, n, nodeSerializer);
            }
            return true;
        }
    }


    /**
     * First in first out lock-free queue
     *
     * @param <E>
     */
    public static class Queue<E> extends SimpleQueue<E> {

        protected final Atomic.Long tail;

        public Queue(Engine engine, Serializer<E> serializer, long headerRecid,
                     long nextTailRecid, boolean useLocks) {
            super(engine, serializer,headerRecid,useLocks);
            tail = new Atomic.Long(engine,nextTailRecid);
        }

        @Override
        public boolean add(E e) {
            long nextTail = engine.put((Node<E>) Node.EMPTY,nodeSerializer);
            long tail2 = tail.get();
            while(!tail.compareAndSet(tail2,nextTail)){
                tail2 = tail.get();
            }
            //now we have tail2 just for us
            Node<E> n = new Node(nextTail,e);
            engine.update(tail2,n,nodeSerializer);
            return true;
        }



    }




    public static class CircularQueue<E> extends SimpleQueue<E> {

        protected final Atomic.Long headInsert;
        //TODO is there a way to implement this without global locks?
        protected final Lock lock = new ReentrantLock(CC.FAIR_LOCKS);
        protected final long size;

        public CircularQueue(Engine engine, Serializer<E> serializer, long headRecid, long headInsertRecid, long size) {
            super(engine, serializer, headRecid,false);
            headInsert = new Atomic.Long(engine, headInsertRecid);
            this.size = size;
        }

        @Override
        public boolean add(Object o) {
            lock.lock();
            try{
                long nRecid = headInsert.get();
                Node<E> n = engine.get(nRecid, nodeSerializer);
                n = new Node<E>(n.next, (E) o);
                engine.update(nRecid, n, nodeSerializer);
                headInsert.set(n.next);
                //move 'poll' head if it points to currently replaced item
                head.compareAndSet(nRecid, n.next);
                return true;
            }finally {
                lock.unlock();
            }
        }

        @Override
        public void clear() {
            // praise locking
            lock.lock();
            try {
                for (int i = 0; i < size; i++) {
                    poll();
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public E poll() {
            lock.lock();
            try{
                long nRecid = head.get();
                Node<E> n = engine.get(nRecid, nodeSerializer);
                engine.update(nRecid, new Node<E>(n.next, null), nodeSerializer);
                head.set(n.next);
                return n.value;
            }finally {
                lock.unlock();
            }
        }

        @Override
        public E peek() {
            lock.lock();
            try{
                long nRecid = head.get();
                Node<E> n = engine.get(nRecid, nodeSerializer);
                return n.value;
            }finally {
                lock.unlock();
            }
        }

    }


}
