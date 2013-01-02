package org.mapdb;

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
public class LinkedQueueLifo<E> implements Queue<E> {

    protected static final Long ZERO = Long.valueOf(0L);

    protected final Engine engine;
    protected final Serializer<E> serializer;
    protected final boolean useLocks;
    protected final Locks.RecidLocks locks;
    protected final long headRecid;


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

    public LinkedQueueLifo(Engine engine, Serializer<E> serializer, boolean useLocks) {
        this.engine = engine;
        this.serializer = serializer;
        this.useLocks = useLocks;
        this.locks = useLocks? new Locks.LongHashMapRecidLocks() : null;
        this.headRecid = engine.put(ZERO, Serializer.LONG_SERIALIZER);
    }

    public LinkedQueueLifo(Engine engine, Serializer<E> serializer, boolean useLocks, long headRecid) {
        this.engine = engine;
        this.serializer = serializer;
        this.useLocks = useLocks;
        this.locks = useLocks? new Locks.SegmentedRecidLocks(16) : null;
        this.headRecid = headRecid;
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
    public E poll() {
        long head = 0;
        Node<E> n;
        do{
            if(useLocks && head!=0)locks.unlock(head);
            head =engine.get(headRecid, Serializer.LONG_SERIALIZER);
            if(ZERO.equals(head)) return null;

            if(useLocks && head!=0)locks.lock(head);
            n = engine.get(head, nodeSerializer);
        }while(n==null || !engine.compareAndSwap(headRecid, head, n.next, Serializer.LONG_SERIALIZER));
        if(useLocks && head!=0){
            engine.delete(head);
            locks.unlock(head);
        }else{
            engine.update(head, null, nodeSerializer);
        }
        return (E) n.value;
    }

    @Override
    public E element() {
        E ret = peek();
        if(ret == null) throw new NoSuchElementException();
        return ret;

    }

    @Override
    public E peek() {
        while(true){
            Long head = engine.get(headRecid, Serializer.LONG_SERIALIZER);
            if(ZERO.equals(head)) return null;
            Node<E> n = engine.get(head, nodeSerializer);
            Long head2 = engine.get(headRecid, Serializer.LONG_SERIALIZER);
            if(ZERO.equals(head2)) return null;
            if(head.equals(head2)) return (E) n.value;
        }
    }

    @Override
    public boolean offer(E e) {
        return add(e);
    }

    @Override
    public boolean add(E e) {
        Long head = engine.get(headRecid, Serializer.LONG_SERIALIZER);
        Node<E> n = new Node<E>(head, e);
        long recid = engine.put(n, nodeSerializer);
        while(!engine.compareAndSwap(headRecid, head, recid, Serializer.LONG_SERIALIZER)){
            //failed to update head, so read new value and start over
            head = engine.get(headRecid, Serializer.LONG_SERIALIZER);
            n = new Node<E>(head, e);
            engine.update(recid, n, nodeSerializer);
        }
        return true;
    }


    @Override
    public boolean isEmpty() {
        Long head = engine.get(headRecid, Serializer.LONG_SERIALIZER);
        return ZERO.equals(head);
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
