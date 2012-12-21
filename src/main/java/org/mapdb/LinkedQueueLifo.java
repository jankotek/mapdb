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
    protected final Serializer serializer;
    protected final boolean useLocks;
    protected final Locks.RecidLocks locks;
    protected final long headRecid;


    protected final Serializer<Node> nodeSerializer = new Serializer<Node>() {
        @Override
        public void serialize(DataOutput out, Node value) throws IOException {
            if(value==null) return;
            Utils.packLong(out,value.next);
            serializer.serialize(out, value.value);
        }

        @Override
        public Node deserialize(DataInput in, int available) throws IOException {
            if(available==0)return null;
            return new Node(Utils.unpackLong(in), serializer.deserialize(in,-1));
        }
    };

    public LinkedQueueLifo(Engine engine, Serializer serializer, boolean useLocks) {
        this.engine = engine;
        this.serializer = serializer;
        this.useLocks = useLocks;
        this.locks = useLocks? new Locks.SegmentedRecidLocks(16) : null;
        this.headRecid = engine.recordPut(ZERO, Serializer.LONG_SERIALIZER);
    }

    public LinkedQueueLifo(Engine engine, Serializer serializer, boolean useLocks, long headRecid) {
        this.engine = engine;
        this.serializer = serializer;
        this.useLocks = useLocks;
        this.locks = useLocks? new Locks.SegmentedRecidLocks(16) : null;
        this.headRecid = headRecid;
    }



    protected static final class Node{
        final protected long next;
        final protected Object value;

        public Node(long next, Object value) {
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
        Node n;
        do{
            if(useLocks && head!=0)locks.unlock(head);
            head =engine.recordGet(headRecid, Serializer.LONG_SERIALIZER);
            if(ZERO.equals(head)) return null;

            if(useLocks && head!=0)locks.lock(head);
            n = engine.recordGet(head, nodeSerializer);
        }while(n==null || !engine.recordCompareAndSwap(headRecid, head, n.next , Serializer.LONG_SERIALIZER));
        if(useLocks && head!=0){
            engine.recordDelete(head);
            locks.unlock(head);
        }else{
            engine.recordUpdate(head, null, nodeSerializer);
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
            Long head = engine.recordGet(headRecid, Serializer.LONG_SERIALIZER);
            if(ZERO.equals(head)) return null;
            Node n = engine.recordGet(head, nodeSerializer);
            Long head2 = engine.recordGet(headRecid, Serializer.LONG_SERIALIZER);
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
        Long head = engine.recordGet(headRecid, Serializer.LONG_SERIALIZER);
        Node n = new Node(head, e);
        long recid = engine.recordPut(n, nodeSerializer);
        while(!engine.recordCompareAndSwap(headRecid, head, recid,Serializer.LONG_SERIALIZER)){
            //failed to update head, so read new value and start over
            head = engine.recordGet(headRecid, Serializer.LONG_SERIALIZER);
            n = new Node(head, e);
            engine.recordUpdate(recid, n, nodeSerializer);
        }
        return true;
    }


    @Override
    public boolean isEmpty() {
        Long head = engine.recordGet(headRecid, Serializer.LONG_SERIALIZER);
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
