package org.mapdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Various queue algorithms
 */
public final class Queues {

    private Queues(){}


    public static abstract class SimpleQueue<E> implements java.util.Queue<E>{

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
                Utils.packLong(out,value.next);
                serializer.serialize(out, value.value);
            }

            @Override
            public Node<E> deserialize(DataInput in, int available) throws IOException {
                if(available==0)return Node.EMPTY;
                return new Node<E>(Utils.unpackLong(in), serializer.deserialize(in,-1));
            }
        }

        protected final Serializer<Node<E>> nodeSerializer;


        public SimpleQueue(Engine engine, Serializer<E> serializer, long headRecid) {
            this.engine = engine;
            this.serializer = serializer;
            if(headRecid == 0) headRecid = engine.put(0L, Serializer.LONG_SERIALIZER);
            head = new Atomic.Long(engine,headRecid);
            nodeSerializer = new NodeSerializer<E>(serializer);
        }


        /**
         * Closes underlying storage and releases all resources.
         * Used mostly with temporary collections where engine is not accessible.
         */
        public void close(){
            engine.close();
        }



        protected static final class Node<E>{

            protected static final Node EMPTY = new Node(0L, null);

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
    }

    /**
     * Last in first out lock-free queue
     *
     * @param <E>
     */
    public static class Stack<E> extends SimpleQueue<E> {

        protected final boolean useLocks;
        protected final ReentrantLock[] locks;


        public Stack(Engine engine,  Serializer<E> serializer, long headerRecid, boolean useLocks) {
            super(engine, serializer, headerRecid);
            this.useLocks = useLocks;
            locks = useLocks? Utils.newLocks(32) : null;
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
                if(useLocks && head2!=0)Utils.lock(locks,head2);
                head2 =head.get();
                if(head2 == 0) return null;

                if(useLocks && head2!=0)Utils.lock(locks,head2);
                n = engine.get(head2, nodeSerializer);
            }while(n==null || !head.compareAndSet(head2, n.next));
            if(useLocks && head2!=0){
                engine.delete(head2,Serializer.LONG_SERIALIZER);
                Utils.unlock(locks,head2);
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

    protected static final class StackRoot{
        final long headerRecid;
        final boolean useLocks;
        final Serializer serializer;

        public StackRoot(long headerRecid, boolean useLocks, Serializer serializer) {
            this.headerRecid = headerRecid;
            this.useLocks = useLocks;
            this.serializer = serializer;
        }
    }

    protected static final class StackRootSerializer implements Serializer<StackRoot>{

        final Serializer<Serializer> serialierSerializer;

        public StackRootSerializer(Serializer<Serializer> serialierSerializer) {
            this.serialierSerializer = serialierSerializer;
        }

        @Override
        public void serialize(DataOutput out, StackRoot value) throws IOException {
            out.write(SerializationHeader.MAPDB_STACK);
            Utils.packLong(out, value.headerRecid);
            out.writeBoolean(value.useLocks);
            serialierSerializer.serialize(out,value.serializer);
        }

        @Override
        public StackRoot deserialize(DataInput in, int available) throws IOException {
            if(in.readUnsignedByte()!=SerializationHeader.MAPDB_STACK) throw new InternalError();
            return new StackRoot(
                    Utils.unpackLong(in),
                    in.readBoolean(),
                    serialierSerializer.deserialize(in,-1)
            );
        }
    }

    static <E> long createStack(Engine engine, Serializer<Serializer> serializerSerializer, Serializer<E> serializer, boolean useLocks){
        long headerRecid = engine.put(0L, Serializer.LONG_SERIALIZER);
        StackRoot root = new StackRoot(headerRecid, useLocks, serializer);
        StackRootSerializer rootSerializer = new StackRootSerializer(serializerSerializer);
        return engine.put(root, rootSerializer);
    }

    static <E> Stack<E> getStack(Engine engine, Serializer<Serializer> serializerSerializer, long rootRecid){
        StackRoot root = engine.get(rootRecid, new StackRootSerializer(serializerSerializer));
        return new Stack<E>(engine, root.serializer, root.headerRecid, root.useLocks);
    }

    /**
     * First in first out lock-free queue
     *
     * @param <E>
     */
    public static class Queue<E> extends SimpleQueue<E> {

        protected final Atomic.Long tail;
        protected final Atomic.Long size;

        public Queue(Engine engine, Serializer<E> serializer, long headerRecid, long nextTailRecid, long sizeRecid) {
            super(engine, serializer,headerRecid);
            tail = new Atomic.Long(engine,nextTailRecid);
            size = new Atomic.Long(engine,sizeRecid);
        }

        @Override
        public boolean add(E item){
            final long nextTail = engine.put((Node<E>)Node.EMPTY, nodeSerializer);
            Node<E> n = new Node<E>(nextTail, item);
            long tail2 = tail.get();
            while(!engine.compareAndSwap(tail2, (Node<E>)Node.EMPTY, n, nodeSerializer)){
                tail2 = tail.get();
            }
            head.compareAndSet(0,tail2);
            tail.set(nextTail);
            size.incrementAndGet();
            return true;
        }

        @Override
        public E poll(){
            while(true){
                long head2 = head.get();
                if(head2 == 0)return null;
                Node<E> n = engine.get(head2,nodeSerializer);
                if(n==null){
                    //TODO we need to know when queue is empty and we can break the cycle
                    // I am not really sure under what concurrent situation is n==null, so there is 'size' hack
                    // but 'size' hack is probably not thread-safe
                    if(size.get()==0)return null ;
                    continue;
                }
                if(!engine.compareAndSwap(head2,n, (Node<E>)Node.EMPTY, nodeSerializer))
                    continue;
                if(!head.compareAndSet(head2,n.next)) throw new InternalError();
                size.decrementAndGet();
                return n.value;
            }
        }

        @Override
        public E peek() {
            long head2 = head.get();
            if(head2==0) return null;
            Node<E> n = engine.get(head2,nodeSerializer);
            while(n == null){
                if(size.get()==0) return null;
                n = engine.get(head2,nodeSerializer);
            }

            return n.value;
        }
    }


    protected static final class QueueRoot{
        final long headerRecid;
        final long nextTailRecid;
        final Serializer serializer;
        final long sizeRecid;

        public QueueRoot(long headerRecid, long nextTailRecid, long sizeRecid, Serializer serializer) {
            this.headerRecid = headerRecid;
            this.nextTailRecid = nextTailRecid;
            this.serializer = serializer;
            this.sizeRecid = sizeRecid;
        }
    }

    protected static final class QueueRootSerializer implements Serializer<QueueRoot>{

        final Serializer<Serializer> serialierSerializer;

        public QueueRootSerializer(Serializer<Serializer> serialierSerializer) {
            this.serialierSerializer = serialierSerializer;
        }

        @Override
        public void serialize(DataOutput out, QueueRoot value) throws IOException {
            out.write(SerializationHeader.MAPDB_QUEUE);
            Utils.packLong(out, value.headerRecid);
            Utils.packLong(out, value.nextTailRecid);
            Utils.packLong(out, value.sizeRecid);
            serialierSerializer.serialize(out,value.serializer);
        }

        @Override
        public QueueRoot deserialize(DataInput in, int available) throws IOException {
            if(in.readUnsignedByte()!=SerializationHeader.MAPDB_QUEUE) throw new InternalError();
            return new QueueRoot(
                    Utils.unpackLong(in),
                    Utils.unpackLong(in),
                    Utils.unpackLong(in),
                    serialierSerializer.deserialize(in,-1)
                    );
        }
    }

    static <E> long createQueue(Engine engine, Serializer<Serializer> serializerSerializer, Serializer<E> serializer){
        long headerRecid = engine.put(0L, Serializer.LONG_SERIALIZER);
        long nextTail = engine.put(SimpleQueue.Node.EMPTY, new SimpleQueue.NodeSerializer(null));
        long nextTailRecid = engine.put(nextTail, Serializer.LONG_SERIALIZER);
        long sizeRecid = engine.put(0L, Serializer.LONG_SERIALIZER);
        QueueRoot root = new QueueRoot(headerRecid, nextTailRecid, sizeRecid, serializer);
        QueueRootSerializer rootSerializer = new QueueRootSerializer(serializerSerializer);
        return engine.put(root, rootSerializer);
    }


    static <E> Queue<E> getQueue(Engine engine, Serializer<Serializer> serializerSerializer, long rootRecid){
        QueueRoot root = engine.get(rootRecid, new QueueRootSerializer(serializerSerializer));
        return new Queue<E>(engine, root.serializer, root.headerRecid, root.nextTailRecid,root.sizeRecid);
    }

    public static class CircularQueue<E> extends SimpleQueue<E> {

        protected final Atomic.Long headInsert;
        //TODO is there a way to implement this without global locks?
        protected final Lock lock = new ReentrantLock();
        protected final long size;

        public CircularQueue(Engine engine, Serializer serializer, long headRecid, long headInsertRecid, long size) {
            super(engine, serializer, headRecid);
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

    protected static final class CircularQueueRoot{
        final long headerRecid;
        final long headerInsertRecid;
        final Serializer serializer;
        final long sizeRecid;

        public CircularQueueRoot(long headerRecid, long headerInsertRecid, long sizeRecid, Serializer serializer) {
            this.headerRecid = headerRecid;
            this.headerInsertRecid = headerInsertRecid;
            this.serializer = serializer;
            this.sizeRecid = sizeRecid;
        }
    }

    protected static final class CircularQueueRootSerializer implements Serializer<CircularQueueRoot>{

        final Serializer<Serializer> serialierSerializer;

        public CircularQueueRootSerializer(Serializer<Serializer> serialierSerializer) {
            this.serialierSerializer = serialierSerializer;
        }

        @Override
        public void serialize(DataOutput out, CircularQueueRoot value) throws IOException {
            out.write(SerializationHeader.MAPDB_CIRCULAR_QUEUE);
            Utils.packLong(out, value.headerRecid);
            Utils.packLong(out, value.headerInsertRecid);
            Utils.packLong(out, value.sizeRecid);
            serialierSerializer.serialize(out,value.serializer);
        }

        @Override
        public CircularQueueRoot deserialize(DataInput in, int available) throws IOException {
            if(in.readUnsignedByte()!=SerializationHeader.MAPDB_CIRCULAR_QUEUE) throw new InternalError();
            return new CircularQueueRoot(
                    Utils.unpackLong(in),
                    Utils.unpackLong(in),
                    Utils.unpackLong(in),
                    serialierSerializer.deserialize(in,-1)
            );
        }
    }

    static <E> long createCircularQueue(Engine engine, Serializer<Serializer> serializerSerializer, Serializer<E> serializer, long size){
        if(size<2) throw new IllegalArgumentException();
        //insert N Nodes empty nodes into a circle
        long prevRecid = 0;
        long firstRecid = 0;
        Serializer<SimpleQueue.Node> nodeSer = new SimpleQueue.NodeSerializer(serializerSerializer);
        for(long i=0;i<size;i++){
            SimpleQueue.Node n = new SimpleQueue.Node(prevRecid, null);
            prevRecid = engine.put(n, nodeSer);
            if(firstRecid==0) firstRecid = prevRecid;
        }
        //update first node to point to last recid
        engine.update(firstRecid, new SimpleQueue.Node(prevRecid, null), nodeSer );

        long headerRecid = engine.put(prevRecid, Serializer.LONG_SERIALIZER);
        long headerInsertRecid = engine.put(prevRecid, Serializer.LONG_SERIALIZER);

        CircularQueueRoot root = new CircularQueueRoot(headerRecid, headerInsertRecid, size, serializer);
        CircularQueueRootSerializer rootSerializer = new CircularQueueRootSerializer(serializerSerializer);
        return engine.put(root, rootSerializer);
    }


    static <E> CircularQueue<E> getCircularQueue(Engine engine, Serializer<Serializer> serializerSerializer, long rootRecid){
        CircularQueueRoot root = engine.get(rootRecid, new CircularQueueRootSerializer(serializerSerializer));
        return new CircularQueue<E>(engine, root.serializer, root.headerRecid, root.headerInsertRecid,root.sizeRecid);
    }

}
