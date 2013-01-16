package org.mapdb;

import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Least In First Out ordered Queue.
 * Is thread safe, concurrently scalable and uses CAS.
 * <p/>
 * It has very limited functionality, only put/pop operations are supported.
 */
public abstract class Queue2<E> implements Queue<E> {


    protected final Engine engine;
    protected final Serializer<E> serializer;

    protected final Atomic.Long head;


    protected final Serializer<Node<E>> nodeSerializer = new Serializer<Node<E>>() {
        @Override
        public void serialize(DataOutput out, Node<E> value) throws IOException {
            if(value==null) return;
            Utils.packLong(out,value.next);
            serializer.serialize(out, value.value);
        }

        @Override
        public Node<E> deserialize(DataInput in, int available) throws IOException {
            if(available==0)return null;
            return new Node<E>(Utils.unpackLong(in), serializer.deserialize(in,-1));
        }
    };


    public Queue2(Engine engine, Serializer<E> serializer) {
        this.engine = engine;
        this.serializer = serializer;
        long recid = engine.put(0L, Serializer.LONG_SERIALIZER);
        head = new Atomic.Long(engine,recid);
    }


    /**
     * Closes underlying storage and releases all resources.
     * Used mostly with temporary collections where engine is not accessible.
     */
    public void close(){
        engine.close();
    }



    protected static final class Node<E>{
        final protected long next;
        final protected E value;

        public Node(long next, E value) {
            this.next = next;
            this.value = value;
        }
    }

    @Override
    public void clear() {
        while(!isEmpty())
            remove();
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
        return add(e);
    }



    @Override
    public boolean isEmpty() {
        return head.get()==0;
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


    public static class Lifo<E> extends Queue2<E>{

        protected final boolean useLocks;
        protected final Locks.RecidLocks locks;

        public Lifo(Engine engine, Serializer<E> serializer, boolean useLocks) {
            super(engine, serializer);
            this.useLocks = useLocks;
            locks = useLocks? new Locks.LongHashMapRecidLocks() : null;
        }

        @Override
        public E peek() {
            while(true){
                long head2 = head.get();
                if(0 == head2) return null;
                Node<E> n = engine.get(head2, nodeSerializer);
                long head3 = head.get();
                if(0 == head2) return null;
                if(head2 == head3) return (E) n.value;
            }
        }

        @Override
        public E poll() {
            long head2 = 0;
            Node<E> n;
            do{
                if(useLocks && head2!=0)locks.unlock(head2);
                head2 =head.get();
                if(head2 == 0) return null;

                if(useLocks && head2!=0)locks.lock(head2);
                n = engine.get(head2, nodeSerializer);
            }while(n==null || !head.compareAndSet(head2, n.next));
            if(useLocks && head2!=0){
                engine.delete(head2);
                locks.unlock(head2);
            }else{
                engine.update(head2, null, nodeSerializer);
            }
            return (E) n.value;
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

    public static class Fifo<E> extends Queue2<E>{

        protected final Atomic.Long tail;

        public Fifo(Engine engine, Serializer<E> serializer) {
            super(engine, serializer);
            long nextTail = engine.put(null, nodeSerializer);
            long recid = engine.put(nextTail, Serializer.LONG_SERIALIZER);
            tail = new Atomic.Long(engine,recid);
        }


        @Override
        public boolean isEmpty() {
            return head.get() == 0;
        }

        public boolean add(E item){
            final long nextTail = engine.put(null, nodeSerializer);
            Node<E> n = new Node<E>(nextTail, item);
            long tail2 = tail.get();
            while(!engine.compareAndSwap(tail2, null, n, nodeSerializer)){
                tail2 = tail.get();
            }
            head.compareAndSet(0,tail2);
            tail.set(nextTail);
            return true;
        }

        public E poll(){
            while(true){
                long head2 = head.get();
                if(head2 == 0)return null;
                Node<E> n = engine.get(head2,nodeSerializer);
                if(n==null) continue;
                if(!engine.compareAndSwap(head2,n, null, nodeSerializer))
                    continue;
                if(!head.compareAndSet(head2,n.next)) throw new InternalError();
                return n.value;
            }
        }

        @Override
        public E peek() {
            long head2 = head.get();
            if(head2==0) return null;
            Node<E> n = engine.get(head2,nodeSerializer);
            while(n == null){
                n = engine.get(head2,nodeSerializer);
            }

            return n.value;
        }

    }


}
