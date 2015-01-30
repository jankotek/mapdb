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

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Thread safe concurrent HashMap
 * <p>
 * This map uses full 32bit hash from beginning, There is no initial load factor and rehash.
 * Technically it is not hash table, but hash tree with nodes expanding when they become full.
 * <p>
 * This map is suitable for number of records  1e9 and over.
 * Larger number of records will increase hash collisions and performance
 * will degrade linearly with number of records (separate chaining).
 * <p>
 * Concurrent scalability is achieved by splitting HashMap into 16 segments, each with separate lock.
 * Very similar to {@link java.util.concurrent.ConcurrentHashMap}
 *
 * @author Jan Kotek
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class HTreeMap<K,V>   extends AbstractMap<K,V> implements ConcurrentMap<K, V>, Bind.MapWithModificationListener<K,V>, Closeable {

    protected static final Logger LOG = Logger.getLogger(HTreeMap.class.getName());

    protected static final int BUCKET_OVERFLOW = 4;

    protected static final int DIV8 = 3;
    protected static final int MOD8 = 0x7;

    /** is this a Map or Set?  if false, entries do not have values, only keys are allowed*/
    protected final boolean hasValues;

    /**
     * Salt added to hash before rehashing, so it is harder to trigger hash collision attack.
     */
    protected final int hashSalt;

    protected final Atomic.Long counter;

    protected final Serializer<K> keySerializer;
    protected final Serializer<V> valueSerializer;

    protected final Engine engine;

    protected final boolean expireFlag;
    protected final long expireTimeStart;
    protected final long expire;
    protected final boolean expireAccessFlag;
    protected final long expireAccess;
    protected final long expireMaxSize;
    protected final long expireStoreSize;
    protected final boolean expireMaxSizeFlag;

    protected final long[] expireHeads;
    protected final long[] expireTails;

    protected final Fun.Function1<V,K> valueCreator;

    protected boolean shutdown = false;
    protected final CountDownLatch expirationThreadNum;


    /** node which holds key-value pair */
    protected static final class LinkedNode<K,V>{

        public final long next;
        public final long expireLinkNodeRecid;

        public final K key;
        public final V value;

        public LinkedNode(final long next, long expireLinkNodeRecid, final K key, final V value ){
            if(CC.PARANOID && next>>>48!=0)
                throw new AssertionError("next recid too big");
            this.key = key;
            this.expireLinkNodeRecid = expireLinkNodeRecid;
            this.value = value;
            this.next = next;
        }
    }



    protected final Serializer<LinkedNode<K,V>> LN_SERIALIZER = new Serializer<LinkedNode<K,V>>() {

        /** used to check that every 64000 th element has consistent has befor and after (de)serialization*/
        int serCounter = 0;

        @Override
        public void serialize(DataOutput out, LinkedNode<K,V> value) throws IOException {
            if(((serCounter++ )& 0xFFFF)==0){
                assertHashConsistent(value.key);
            }

            DataIO.packLong(out, value.next);
            if(expireFlag)
                DataIO.packLong(out, value.expireLinkNodeRecid);
            keySerializer.serialize(out,value.key);
            if(hasValues)
                valueSerializer.serialize(out,value.value);
        }

        @Override
        public LinkedNode<K,V> deserialize(DataInput in, int available) throws IOException {
            if(CC.PARANOID && ! (available!=0))
                throw new AssertionError();
            return new LinkedNode<K, V>(
                    DataIO.unpackLong(in),
                    expireFlag? DataIO.unpackLong(in):0L,
                    keySerializer.deserialize(in,-1),
                    hasValues? valueSerializer.deserialize(in,-1) : (V) Boolean.TRUE
            );
        }

        @Override
        public boolean isTrusted() {
            return keySerializer.isTrusted() && valueSerializer.isTrusted();
        }
    };

    private final  void assertHashConsistent(K key) throws IOException {
        int hash = keySerializer.hashCode(key);
        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        keySerializer.serialize(out,key);
        DataIO.DataInputByteArray in = new DataIO.DataInputByteArray(out.buf, 0);

        K key2 = keySerializer.deserialize(in,-1);
        if(hash!=keySerializer.hashCode(key2)){
            throw new IllegalArgumentException("Key does not have consistent hash before and after deserialization. Class: "+key.getClass());
        }
        if(!keySerializer.equals(key,key2)){
            throw new IllegalArgumentException("Key does not have consistent equals before and after deserialization. Class: "+key.getClass());
        }
        if(out.pos!=in.pos){
            throw new IllegalArgumentException("Key has inconsistent serialization length. Class: "+key.getClass());
        }
    }


    protected static final Serializer<Object> DIR_SERIALIZER = new Serializer<Object>() {
        @Override
        public void serialize(DataOutput out, Object value) throws IOException {
            DataIO.DataOutputByteArray out2 = (DataIO.DataOutputByteArray) out;
            if(value instanceof long[]) {
                serializeLong(out2, value);
                return;
            }

            int[] c = (int[]) value;

            if(CC.PARANOID){
                int len = 4 +
                        Integer.bitCount(c[0])+
                        Integer.bitCount(c[1])+
                        Integer.bitCount(c[2])+
                        Integer.bitCount(c[3]);

                if(len!=c.length)
                    throw new AssertionError("bitmap!=len");
            }

            //write bitmaps
            out2.writeInt(c[0]);
            out2.writeInt(c[1]);
            out2.writeInt(c[2]);
            out2.writeInt(c[3]);

            if(c.length==4)
                return;

            out2.packLong((((long)c[4])<<1)|1L);
            for(int i=5;i<c.length;i++){
                out2.packLong(c[i]);
            }
        }

        private void serializeLong(DataIO.DataOutputByteArray out, Object value) throws IOException {
            long[] c= (long[]) value;

            if(CC.PARANOID){
                int len = 2 +
                        Long.bitCount(c[0])+
                        Long.bitCount(c[1]);

                if(len!=c.length)
                    throw new AssertionError("bitmap!=len");
            }

            out.writeLong(c[0]);
            out.writeLong(c[1]);
            if(c.length==2)
                return;

            out.packLong(c[2]<<1);
            for(int i=3;i<c.length;i++){
                out.packLong(c[i]);
            }
        }


        @Override
        public Object deserialize(DataInput in, int available) throws IOException {
            DataIO.DataInputInternal in2 = (DataIO.DataInputInternal) in;

            //length of dir is 128 longs, each long has 6 bytes (not 8)
            //to save memory zero values are skipped,
            //there is bitmap at first 16 bytes, each non-zero long has bit set
            //to determine offset one must traverse bitmap and count number of bits set
            int bitmap1 = in.readInt();
            int bitmap2 = in.readInt();
            int bitmap3 = in.readInt();
            int bitmap4 = in.readInt();
            int len = Integer.bitCount(bitmap1) + Integer.bitCount(bitmap2) + Integer.bitCount(bitmap3) + Integer.bitCount(bitmap4);

            if (len == 0) {
                return new int[4];
            }

            long firstVal = in2.unpackLong();

            if ((firstVal & 1) == 0) {
                //return longs
                long[] ret = new long[2 + len];
                ret[0] = ((long) bitmap1 << 32) | (bitmap2 & 0xFFFFFFFF);
                ret[1] = ((long) bitmap3 << 32) | (bitmap4 & 0xFFFFFFFF);
                ret[2] = firstVal >>> 1;
                len += 2;
                in2.unpackLongArray(ret, 3, len);
                return ret;
            } else {
                //return int[]
                int[] ret = new int[4 + len];
                ret[0] = bitmap1;
                ret[1] = bitmap2;
                ret[2] = bitmap3;
                ret[3] = bitmap4;
                ret[4] = (int) (firstVal >>> 1);
                len += 4;
                in2.unpackIntArray(ret,5,len);
                return ret;
            }
        }

        @Override
        public boolean isTrusted() {
            return true;
        }
    };

    /** list of segments, this is immutable*/
    protected final long[] segmentRecids;

    protected final ReentrantReadWriteLock[] segmentLocks;



    /**
     * Opens HTreeMap
     */
    public HTreeMap(Engine engine, long counterRecid, int hashSalt, long[] segmentRecids,
                    Serializer<K> keySerializer, Serializer<V> valueSerializer,
                    long expireTimeStart, long expire, long expireAccess, long expireMaxSize, long expireStoreSize,
                    long[] expireHeads, long[] expireTails, Fun.Function1<V, K> valueCreator, Fun.ThreadFactory threadFactory) {
        if(counterRecid<0) throw new IllegalArgumentException();
        if(engine==null) throw new NullPointerException();
        if(segmentRecids==null) throw new NullPointerException();
        if(keySerializer==null) throw new NullPointerException();

//        SerializerBase.assertSerializable(keySerializer); //TODO serializer serialization
        this.hasValues = valueSerializer!=null;
        if(hasValues) {
//            SerializerBase.assertSerializable(valueSerializer);
        }

        segmentLocks=new ReentrantReadWriteLock[16];
        for(int i=0;i< 16;i++)  {
            segmentLocks[i]=new ReentrantReadWriteLock(CC.FAIR_LOCKS);
        }


        this.engine = engine;
        this.hashSalt = hashSalt;
        this.segmentRecids = Arrays.copyOf(segmentRecids,16);
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;

        if(expire==0 && expireAccess!=0){
            expire = expireAccess;
        }
        if(expireMaxSize!=0 && counterRecid==0){
            throw new IllegalArgumentException("expireMaxSize must have counter enabled");
        }


        this.expireFlag = expire !=0L || expireAccess!=0L || expireMaxSize!=0 || expireStoreSize!=0;
        this.expire = expire;
        this.expireTimeStart = expireTimeStart;
        this.expireAccessFlag = expireAccess !=0L || expireMaxSize!=0 || expireStoreSize!=0;
        this.expireAccess = expireAccess;
        this.expireHeads = expireHeads==null? null : Arrays.copyOf(expireHeads,16);
        this.expireTails = expireTails==null? null : Arrays.copyOf(expireTails,16);
        this.expireMaxSizeFlag = expireMaxSize!=0;
        this.expireMaxSize = expireMaxSize;
        this.expireStoreSize = expireStoreSize;
        this.valueCreator = valueCreator;

        if(counterRecid!=0){
            this.counter = new Atomic.Long(engine,counterRecid);
            Bind.size(this,counter);
        }else{
            this.counter = null;
        }

        if(expireFlag){
            expirationThreadNum = new CountDownLatch(1);
            threadFactory.newThread("HTreeMap expirator", new ExpireRunnable(this));
        }else{
            expirationThreadNum = null;
        }

    }



    protected static long[] preallocateSegments(Engine engine){
        //prealocate segmentRecids, so we dont have to lock on those latter
        long[] ret = new long[16];
        for(int i=0;i<16;i++)
            ret[i] = engine.put(new int[4], DIR_SERIALIZER);
        return ret;
    }



    @Override
    public boolean containsKey(final Object o){
        return getPeek(o)!=null;
    }

    @Override
    public int size() {
        return (int) Math.min(sizeLong(), Integer.MAX_VALUE);
    }


    @Override
    public long sizeLong() {
        if(counter!=null)
            return counter.get();


        long counter = 0;

        //search tree, until we find first non null
        for(int i=0;i<16;i++){
            Lock lock = segmentLocks[i].readLock();
            lock.lock();
            try{
                final long dirRecid = segmentRecids[i];
                counter+=recursiveDirCount(dirRecid);
            }finally {
                lock.unlock();
            }
        }


        return counter;
    }

    private long recursiveDirCount(final long dirRecid) {
        Object dir = engine.get(dirRecid, DIR_SERIALIZER);
        long counter = 0;
        int dirLen = dirLen(dir);
        for(int pos=dirStart(dir);pos<dirLen;pos++){
            long recid = dirGet(dir,pos);
            if((recid&1)==0){
                //reference to another subdir
                recid = recid>>>1;
                counter += recursiveDirCount(recid);
            }else{
                //reference to linked list, count it
                recid = recid>>>1;
                while(recid!=0){
                    LinkedNode n = engine.get(recid, LN_SERIALIZER);
                    if(n!=null){
                        counter++;
                        recid =  n.next;
                    }else{
                        recid = 0;
                    }
                }
            }
        }
        return counter;
    }

    @Override
    public boolean isEmpty() {
        //search tree, until we find first non null
        for(int i=0;i<16;i++){
            Lock lock = segmentLocks[i].readLock();
            lock.lock();
            try{
                long dirRecid = segmentRecids[i];
                Object dir = engine.get(dirRecid, DIR_SERIALIZER);
                if(!dirIsEmpty(dir)){
                    return false;
                }
            }finally {
                lock.unlock();
            }
        }

        return true;
    }



    @Override
    public V get(final Object o){
        if(o==null) return null;
        final int h = hash(o);
        final int segment = h >>>28;

        final Lock lock = expireAccessFlag ? segmentLocks[segment].writeLock() : segmentLocks[segment].readLock();
        lock.lock();
        LinkedNode<K,V> ln;
        try{
            ln = getInner(o, h, segment);

            if(ln!=null && expireAccessFlag)
                expireLinkBump(segment,ln.expireLinkNodeRecid,true);
        }finally {
            lock.unlock();
        }
        if(valueCreator==null){
            if(ln==null)
                return null;
            return ln.value;
        }

        //value creator is set, so create and put new value
        V value = valueCreator.run((K) o);
        //there is race condition, vc could be called twice. But map will be updated only once
        V prevVal = putIfAbsent((K) o,value);
        if(prevVal!=null)
            return prevVal;
        return value;
    }


    /**
     * Return given value, without updating cache statistics if `expireAccess()` is true
     * It also does not use `valueCreator` if value is not found (always returns null if not found)
     *
     * @param key key to lookup
     * @return value associated with key or null
     */
    public V getPeek(final Object key){
        if(key==null) return null;
        final int h = hash(key);
        final int segment = h >>>28;

        final Lock lock = segmentLocks[segment].readLock();
        lock.lock();

        try{
            LinkedNode<K,V> ln = getInner(key, h, segment);
            if(ln==null) return null;
            return ln.value;
        }finally {
            lock.unlock();
        }

    }

    protected LinkedNode<K,V> getInner(Object o, int h, int segment) {
        long recid = segmentRecids[segment];
        for(int level=3;level>=0;level--){
            Object dir = engine.get(recid, DIR_SERIALIZER);
            if(dir == null)
                return null;
            final int slot = (h>>>(level*7 )) & 0x7F;
            if(CC.PARANOID && ! (slot<128))
                throw new AssertionError();
            recid = dirGetSlot(dir, slot);
            if(recid == 0)
                return null;

            if((recid&1)!=0){ //last bite indicates if referenced record is LinkedNode
                recid = recid>>>1;
                while(true){
                    LinkedNode<K,V> ln = engine.get(recid, LN_SERIALIZER);
                    if(ln == null) return null;
                    if(keySerializer.equals(ln.key, (K) o)){
                        if(CC.PARANOID && ! (hash(ln.key)==h))
                            throw new AssertionError();
                        return ln;
                    }
                    if(ln.next==0) return null;
                    recid = ln.next;
                }
            }

            recid = recid>>>1;
        }

        return null;
    }

    protected static boolean dirIsEmpty(Object dir) {
        if(dir == null)
            return true;
        if(dir instanceof long[])
            return false;
        return ((int[])dir).length==4;
    }

    protected static int dirLen(Object dir) {
        return dir instanceof int[]?
                ((int[])dir).length:
                ((long[])dir).length;
    }

    protected static int dirStart(Object dir) {
        return dir instanceof int[]?4:2;
    }


    protected static long dirGet(Object dir, int pos) {
        return dir instanceof int[]?
                ((int[])dir)[pos]:
                ((long[])dir)[pos];
    }

    protected long dirGetSlot(Object dir, int slot) {
        if(dir instanceof int[]){
            int[] cc = (int[]) dir;
            int pos = dirOffsetFromSlot(cc,slot);
            if(pos<0)
                return 0;
            return cc[pos];
        }else{
            long[] cc = (long[]) dir;
            int pos = dirOffsetFromSlot(cc,slot);
            if(pos<0)
                return 0;
            return cc[pos];
        }
    }


    protected static int dirOffsetFromSlot(Object dir, int slot) {
        if(dir instanceof int[])
            return dirOffsetFromSlot((int[])dir,slot);
        else
            return dirOffsetFromSlot((long[])dir,slot);
    }


    /** converts hash slot into actual offset in dir array, using bitmap */
    protected static final int dirOffsetFromSlot(int[] dir, int slot) {
        if(CC.PARANOID && slot>127)
            throw new AssertionError();
        int val = slot>>>5;
        slot &=31;
        int isSet = ((dir[val] >>> (slot)) & 1); //check if bit at given slot is set
        isSet <<=1; //multiply by two, so it is usable in multiplication

        int offset=0;

        int dirPos=0;
        while(dirPos!=val){
            offset+=Integer.bitCount(dir[dirPos++]);
        }

        slot = (1<<(slot))-1; //turn slot into mask for N right bits

        offset += 4+Integer.bitCount(dir[dirPos] & slot);

        //turn into negative value if bit is not set, do not use conditions
        return -offset + isSet*offset;
    }

    /** converts hash slot into actual offset in dir array, using bitmap */
    protected static final int dirOffsetFromSlot(long[] dir, int slot) {
        if(CC.PARANOID && slot>127)
            throw new AssertionError();

        int offset = 0;
        long v = dir[0];

        if(slot>63){
            offset+=Long.bitCount(v);
            v = dir[1];
        }

        slot &= 63;
        long mask = ((1L)<<(slot&63))-1;
        offset += 2+Long.bitCount(v & mask);

        int v2 = (int) ((v>>>(slot))&1);
        v2<<=1;

        //turn into negative value if bit is not set, do not use conditions
        return -offset + v2*offset;
    }


    protected static final Object dirPut(Object dir, int slot, long newRecid){
        if(dir instanceof int[]) {
            int[] dir_ = (int[]) dir;
            int offset = dirOffsetFromSlot(dir_, slot);
            //does new recid fit into integer?
            if (newRecid <= Integer.MAX_VALUE) {
                //make copy and expand it if necessary
                if (offset < 0) {
                    offset = -offset;
                    dir_ = Arrays.copyOf(dir_, dir_.length + 1);
                    //make space for new value
                    System.arraycopy(dir_, offset, dir_, offset + 1, dir_.length - 1 - offset);
                    //and update bitmap
                    //TODO assert slot bit was not set
                    int bytePos = slot / 32;
                    int bitPos = slot % 32;
                    dir_[bytePos] = (dir_[bytePos] | (1 << bitPos));
                } else {
                    //TODO assert slot bit was set
                    dir_ = dir_.clone();
                }
                //and insert value itself
                dir_[offset] = (int) newRecid;
                return dir_;
            } else {
                //new recid does not fit into long, so upgrade to long[] and continue
                long[] dir2 = new long[dir_.length-2];
                //bitmaps
                dir2[0] = ((long)dir_[0]<<32) | dir_[1] & 0xFFFFFFFFL;
                dir2[1] = ((long)dir_[2]<<32) | dir_[3] & 0xFFFFFFFFL;
                for(int i=4;i<dir_.length;i++) {
                    dir2[i-2] = dir_[i];
                }
                dir = dir2;
            }
        }

        //do long stuff
        long[] dir_ = (long[]) dir;
        int offset = dirOffsetFromSlot(dir_, slot);
        //make copy and expand it if necessary
        if (offset < 0) {
            offset = -offset;
            dir_ = Arrays.copyOf(dir_, dir_.length + 1);
            //make space for new value
            System.arraycopy(dir_, offset, dir_, offset + 1, dir_.length - 1 - offset);
            //and update bitmap
            //TODO assert slot bit was not set
            int bytePos = slot / 64;
            int bitPos = slot % 64;
            dir_[bytePos] = (dir_[bytePos] | (1L << bitPos));
        } else {
            //TODO assert slot bit was set
            dir_ = dir_.clone();
        }
        //and insert value itself
        dir_[offset] = newRecid;
        return dir_;
    }

    protected static final Object dirRemove(Object dir, final int slot){
        int offset = dirOffsetFromSlot(dir, slot);
        if(CC.PARANOID && offset<=0){
            throw new AssertionError();
        }

        if(dir instanceof int[]) {
            int[] dir_ = (int[]) dir;
            //shrink and copy data
            int[] dir2 = new int[dir_.length - 1];
            System.arraycopy(dir_, 0, dir2, 0, offset);
            System.arraycopy(dir_, offset + 1, dir2, offset, dir2.length - offset);

            //unset bitmap bit
            //TODO assert slot bit was set
            int bytePos = slot / 32;
            int bitPos = slot % 32;
            dir2[bytePos] =  (dir2[bytePos] & ~(1 << bitPos));
            return dir2;
        }else{
            long[] dir_ = (long[]) dir;
            //shrink and copy data
            long[] dir2 = new long[dir_.length - 1];
            System.arraycopy(dir_, 0, dir2, 0, offset);
            System.arraycopy(dir_, offset + 1, dir2, offset, dir2.length - offset);

            //unset bitmap bit
            //TODO assert slot bit was set
            int bytePos = slot / 64;
            int bitPos = slot % 64;
            dir2[bytePos] =  (dir2[bytePos] & ~(1L << bitPos));
            return dir2;
        }
    }

    @Override
    public V put(final K key, final V value){
        if (key == null)
            throw new IllegalArgumentException("null key");

        if (value == null)
            throw new IllegalArgumentException("null value");

        final int h = hash(key);
        final int segment = h >>>28;
        segmentLocks[segment].writeLock().lock();
        try{
            return putInner(key, value, h, segment);
        }finally {
            segmentLocks[segment].writeLock().unlock();
        }
    }

    private V putInner(K key, V value, int h, int segment) {
        long dirRecid = segmentRecids[segment];

        int level = 3;
        while(true){
            Object dir = engine.get(dirRecid, DIR_SERIALIZER);
            final int slot =  (h>>>(7*level )) & 0x7F;

            if(CC.PARANOID && ! (slot<=127))
                throw new AssertionError();

            if(dir == null ){
                //create new dir
                dir = new int[4];
            }

            final int dirOffset = dirOffsetFromSlot(dir,slot);
            int counter = 0;
            long recid = dirOffset<0 ? 0 : dirGet(dir, dirOffset);

            if(recid!=0){
                if((recid&1) == 0){
                    dirRecid = recid>>>1;
                    level--;
                    continue;
                }
                recid = recid>>>1;

                //traverse linked list, try to replace previous value
                LinkedNode<K,V> ln = engine.get(recid, LN_SERIALIZER);

                while(ln!=null){
                    if(keySerializer.equals(ln.key,key)){
                        //found, replace value at this node
                        V oldVal = ln.value;
                        ln = new LinkedNode<K, V>(ln.next, ln.expireLinkNodeRecid, ln.key, value);
                        if(CC.PARANOID && ln.next==recid)
                            throw new AssertionError("cyclic reference in linked list");

                        engine.update(recid, ln, LN_SERIALIZER);
                        if(expireFlag)
                            expireLinkBump(segment,ln.expireLinkNodeRecid,false);
                        notify(key,  oldVal, value);
                        return oldVal;
                    }
                    recid = ln.next;
                    ln = ((recid==0)?
                            null :
                            engine.get(recid, LN_SERIALIZER));
                    if(CC.PARANOID && ln!=null && ln.next==recid)
                        throw new AssertionError("cyclic reference in linked list");

                    counter++;
                    if(CC.PARANOID && counter>1024*1024)
                        throw new AssertionError("linked list too large");
                }
                //key was not found at linked list, so just append it to beginning
            }


            //check if linked list has overflow and needs to be expanded to new dir level
            if(counter>=BUCKET_OVERFLOW && level>=1){
                Object nextDir = new int[4];

                {
                    final long expireNodeRecid = expireFlag? engine.preallocate():0L;
                    final LinkedNode<K,V> node = new LinkedNode<K, V>(0, expireNodeRecid, key, value);
                    final long newRecid = engine.put(node, LN_SERIALIZER);
                    if(CC.PARANOID && newRecid==node.next)
                        throw new AssertionError("cyclic reference in linked list");
                    //add newly inserted record
                    final int pos =(h >>>(7*(level-1) )) & 0x7F;
                    nextDir = dirPut(nextDir,pos,( newRecid<<1) | 1);
                    if(expireFlag)
                        expireLinkAdd(segment,expireNodeRecid,newRecid,h);
                }


                //redistribute linked bucket into new dir
                long nodeRecid = dirOffset<0?0: dirGet(dir, dirOffset)>>>1;
                while(nodeRecid!=0){
                    LinkedNode<K,V> n = engine.get(nodeRecid, LN_SERIALIZER);
                    final long nextRecid = n.next;
                    final int pos = (hash(n.key) >>>(7*(level -1) )) & 0x7F;
                    final long recid2 = dirGetSlot(nextDir,pos);
                    n = new LinkedNode<K, V>(recid2>>>1, n.expireLinkNodeRecid, n.key, n.value);
                    nextDir = dirPut(nextDir,pos,(nodeRecid<<1) | 1);
                    engine.update(nodeRecid, n, LN_SERIALIZER);
                    if(CC.PARANOID && nodeRecid==n.next)
                        throw new AssertionError("cyclic reference in linked list");
                    nodeRecid = nextRecid;
                }

                //insert nextDir and update parent dir
                long nextDirRecid = engine.put(nextDir, DIR_SERIALIZER);
                int parentPos = (h>>>(7*level )) & 0x7F;
                dir = dirPut(dir, parentPos, (nextDirRecid<<1) | 0);
                engine.update(dirRecid, dir, DIR_SERIALIZER);
                notify(key, null, value);
                return null;
            }else{
                // record does not exist in linked list, so create new one
                recid = dirOffset<0? 0: dirGet(dir, dirOffset)>>>1;
                final long expireNodeRecid = expireFlag? engine.put(ExpireLinkNode.EMPTY, ExpireLinkNode.SERIALIZER):0L;

                final long newRecid = engine.put(
                        new LinkedNode<K, V>(recid, expireNodeRecid, key, value),
                        LN_SERIALIZER);
                if(CC.PARANOID && newRecid==recid)
                    throw new AssertionError("cyclic reference in linked list");
                dir = dirPut(dir,slot,(newRecid<<1) | 1);
                engine.update(dirRecid, dir, DIR_SERIALIZER);
                if(expireFlag) expireLinkAdd(segment,expireNodeRecid, newRecid,h);
                notify(key, null, value);
                return null;
            }
        }
    }


    @Override
    public V remove(Object key){

        final int h = hash(key);
        final int segment = h >>>28;
        segmentLocks[segment].writeLock().lock();
        try{
            return removeInternal(key, segment, h, true);
        }finally {
            segmentLocks[segment].writeLock().unlock();
        }
    }


    protected V removeInternal(Object key, int segment, int h, boolean removeExpire){
        final  long[] dirRecids = new long[4];
        int level = 3;
        dirRecids[level] = segmentRecids[segment];

        if(CC.PARANOID && ! (segment==h>>>28))
            throw new AssertionError();

        while(true){
            Object dir = engine.get(dirRecids[level], DIR_SERIALIZER);
            final int slot =  (h>>>(7*level )) & 0x7F;
            if(CC.PARANOID && ! (slot<=127))
                throw new AssertionError();

            if(dir == null ){
                //create new dir
                dir = new int[4];
            }

            long recid = dirGetSlot(dir, slot);

            if(recid!=0){
                if((recid&1) == 0){
                    level--;
                    dirRecids[level] = recid>>>1;
                    continue;
                }
                recid = recid>>>1;

                //traverse linked list, try to remove node
                LinkedNode<K,V> ln = engine.get(recid, LN_SERIALIZER);
                LinkedNode<K,V> prevLn = null;
                long prevRecid = 0;
                while(ln!=null){
                    if(keySerializer.equals(ln.key, (K) key)){
                        //remove from linkedList
                        if(prevLn == null ){
                            //referenced directly from dir
                            if(ln.next==0){
                                recursiveDirDelete(h, level, dirRecids, dir, slot);


                            }else{
                                dir = dirPut(dir,slot,(ln.next<<1)|1);
                                engine.update(dirRecids[level], dir, DIR_SERIALIZER);
                            }

                        }else{
                            //referenced from LinkedNode
                            prevLn = new LinkedNode<K, V>(ln.next, prevLn.expireLinkNodeRecid,prevLn.key, prevLn.value);
                            engine.update(prevRecid, prevLn, LN_SERIALIZER);
                            if(CC.PARANOID && prevRecid==prevLn.next)
                                throw new AssertionError("cyclic reference in linked list");
                        }
                        //found, remove this node
                        if(CC.PARANOID && ! (hash(ln.key)==h))
                            throw new AssertionError();
                        engine.delete(recid, LN_SERIALIZER);
                        if(removeExpire && expireFlag) expireLinkRemove(segment, ln.expireLinkNodeRecid);
                        notify((K) key, ln.value, null);
                        return ln.value;
                    }
                    prevRecid = recid;
                    prevLn = ln;
                    recid = ln.next;
                    ln = recid==0? null : engine.get(recid, LN_SERIALIZER);
//                        counter++;
                }
                //key was not found at linked list, so it does not exist
                return null;
            }
            //recid is 0, so entry does not exist
            return null;

        }
    }


    private void recursiveDirDelete(int h, int level, long[] dirRecids, Object dir, int slot) {
        //was only item in linked list, so try to collapse the dir
        dir = dirRemove(dir, slot);

        if(dirIsEmpty(dir)){
            //delete from parent dir
            if(level==3){
                //parent is segment, recid of this dir can not be modified,  so just update to null
                engine.update(dirRecids[level], new int[4], DIR_SERIALIZER);
            }else{
                engine.delete(dirRecids[level], DIR_SERIALIZER);

                final Object parentDir = engine.get(dirRecids[level + 1], DIR_SERIALIZER);
                final int parentPos = (h >>> (7 * (level + 1))) & 0x7F;
                recursiveDirDelete(h,level+1,dirRecids, parentDir, parentPos);
                //parentDir[parentPos>>>DIV8][parentPos&MOD8] = 0;
                //engine.update(dirRecids[level + 1],parentDir,DIR_SERIALIZER);

            }
        }else{
            engine.update(dirRecids[level], dir, DIR_SERIALIZER);
        }
    }

    @Override
    public void clear() {
        for(int i = 0; i<16;i++) try{
            segmentLocks[i].writeLock().lock();

            final long dirRecid = segmentRecids[i];
            recursiveDirClear(dirRecid);

            //set dir to null, as segment recid is immutable
            engine.update(dirRecid, new int[4], DIR_SERIALIZER);

            if(expireFlag)
                while(expireLinkRemoveLast(i)!=null){} //TODO speedup remove all

        }finally {
            segmentLocks[i].writeLock().unlock();
        }
    }

    private void recursiveDirClear(final long dirRecid) {
        final Object dir = engine.get(dirRecid, DIR_SERIALIZER);
        if(dir == null)
            return;
        int dirlen = dirLen(dir);
        for(int offset=dirStart(dir);offset<dirlen;offset++){
            long recid = dirGet(dir,offset);
            if((recid&1)==0){
                //another dir
                recid = recid>>>1;
                //recursively remove dir
                recursiveDirClear(recid);
                engine.delete(recid, DIR_SERIALIZER);
            }else{
                //linked list to delete
                recid = recid>>>1;
                while(recid!=0){
                    LinkedNode n = engine.get(recid, LN_SERIALIZER);
                    if(CC.PARANOID && n.next==recid)
                        throw new AssertionError("cyclic reference in linked list");
                    engine.delete(recid,LN_SERIALIZER);
                    notify((K)n.key, (V)n.value , null);
                    recid = n.next;
                }
            }
        }
    }


    @Override
    public boolean containsValue(Object value) {
        for (V v : values()) {
            if (valueSerializer.equals(v, (V) value)) return true;
        }
        return false;
    }



    protected class KeySet extends AbstractSet<K> {

        @Override
        public int size() {
            return HTreeMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return HTreeMap.this.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return HTreeMap.this.containsKey(o);
        }

        @Override
        public Iterator<K> iterator() {
            return new KeyIterator();
        }

        @Override
        public boolean add(K k) {
            if(HTreeMap.this.hasValues)
                throw new UnsupportedOperationException();
            else
                return HTreeMap.this.put(k, (V) Boolean.TRUE) == null;
        }

        @Override
        public boolean remove(Object o) {
//            if(o instanceof Entry){
//                Entry e = (Entry) o;
//                return HTreeMap.this.remove(((Entry) o).getKey(),((Entry) o).getValue());
//            }
            return HTreeMap.this.remove(o)!=null;

        }


        @Override
        public void clear() {
            HTreeMap.this.clear();
        }

        public HTreeMap<K,V> parent(){
            return HTreeMap.this;
        }

        @Override
        public int hashCode() {
            int result = 0;
            for (K k : this) {
                result += keySerializer.hashCode(k);
            }
            return result;

        }
    }



    private final Set<K> _keySet = new KeySet();

    @Override
    public Set<K> keySet() {
        return _keySet;
    }

    private final Collection<V> _values = new AbstractCollection<V>(){

        @Override
        public int size() {
            return HTreeMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return HTreeMap.this.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return HTreeMap.this.containsValue(o);
        }



        @Override
        public Iterator<V> iterator() {
            return new ValueIterator();
        }

    };

    @Override
    public Collection<V> values() {
        return _values;
    }

    private final Set<Entry<K,V>> _entrySet = new AbstractSet<Entry<K,V>>(){

        @Override
        public int size() {
            return HTreeMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return HTreeMap.this.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            if(o instanceof  Entry){
                Entry e = (Entry) o;
                Object val = HTreeMap.this.get(e.getKey());
                return val!=null && valueSerializer.equals((V)val,(V)e.getValue());
            }else
                return false;
        }

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return new EntryIterator();
        }


        @Override
        public boolean add(Entry<K, V> kvEntry) {
            K key = kvEntry.getKey();
            V value = kvEntry.getValue();
            if(key==null || value == null) throw new NullPointerException();
            HTreeMap.this.put(key, value);
            return true;
        }

        @Override
        public boolean remove(Object o) {
            if(o instanceof Entry){
                Entry e = (Entry) o;
                Object key = e.getKey();
                if(key == null) return false;
                return HTreeMap.this.remove(key, e.getValue());
            }
            return false;
        }


        @Override
        public void clear() {
            HTreeMap.this.clear();
        }
    };

    @Override
    public Set<Entry<K, V>> entrySet() {
        return _entrySet;
    }


    protected int hash(final Object key) {
        //TODO investigate hash distribution and performance impact
        int h = keySerializer.hashCode((K) key) ^ hashSalt;
        //spread low bits,
        //need so many mixes so each bit becomes part of segment
        //segment is upper 4 bits
        h ^= (h<<4);
        h ^= (h<<4);
        h ^= (h<<4);
        h ^= (h<<4);
        h ^= (h<<4);
        h ^= (h<<4);
        h ^= (h<<4);
        return h;
    }


    abstract class HashIterator{

        protected LinkedNode[] currentLinkedList;
        protected int currentLinkedListPos = 0;

        private K lastReturnedKey = null;

        private int lastSegment = 0;

        HashIterator(){
            currentLinkedList = findNextLinkedNode(0);
        }

        public void remove() {
            final K keyToRemove = lastReturnedKey;
            if (lastReturnedKey == null)
                throw new IllegalStateException();

            lastReturnedKey = null;
            HTreeMap.this.remove(keyToRemove);
        }

        public boolean hasNext(){
            return currentLinkedList!=null && currentLinkedListPos<currentLinkedList.length;
        }


        protected void  moveToNext(){
            lastReturnedKey = (K) currentLinkedList[currentLinkedListPos].key;
            currentLinkedListPos+=1;
            if(currentLinkedListPos==currentLinkedList.length){
                final int lastHash = hash(lastReturnedKey);
                currentLinkedList = advance(lastHash);
                currentLinkedListPos = 0;
            }
        }

        private LinkedNode[] advance(int lastHash){

            int segment = lastHash>>>28;

            //two phases, first find old item and increase hash
            Lock lock = segmentLocks[segment].readLock();
            lock.lock();
            try{
                long dirRecid = segmentRecids[segment];
                int level = 3;
                //dive into tree, finding last hash position
                while(true){
                    Object dir = engine.get(dirRecid, DIR_SERIALIZER);
                    //check if we need to expand deeper
                    long recid = dirGetSlot(dir,(lastHash >>> (7 * level)) & 0x7F);
                    if(recid==0 || (recid&1)==1) {
                        //increase hash by 1
                        if(level!=0){
                            lastHash = ((lastHash>>>(7 * level)) + 1) << (7*level); //should use mask and XOR
                        }else
                            lastHash +=1;
                        if(lastHash==0){
                            return null;
                        }
                        break;
                    }

                    //reference is dir, move to next level
                    dirRecid = recid>>1;
                    level--;
                }

            }finally {
                lock.unlock();
            }
            return findNextLinkedNode(lastHash);
        }

        private LinkedNode[] findNextLinkedNode(int hash) {
            //second phase, start search from increased hash to find next items
            for(int segment = Math.max(hash>>>28, lastSegment); segment<16;segment++){
                final Lock lock = expireAccessFlag ? segmentLocks[segment].writeLock() :segmentLocks[segment].readLock() ;
                lock.lock();
                try{
                    lastSegment = Math.max(segment,lastSegment);
                    long dirRecid = segmentRecids[segment];
                    LinkedNode ret[] = findNextLinkedNodeRecur(dirRecid, hash, 3);
                    if(CC.PARANOID && ret!=null) for(LinkedNode ln:ret){
                        if(( hash(ln.key)>>>28!=segment))
                            throw new AssertionError();
                    }
                    //System.out.println(Arrays.asList(ret));
                    if(ret !=null){
                        if(expireAccessFlag){
                            for(LinkedNode ln:ret) expireLinkBump(segment,ln.expireLinkNodeRecid,true);
                        }
                        return ret;
                    }
                    hash = 0;
                }finally {
                    lock.unlock();
                }
            }

            return null;
        }

        private LinkedNode[] findNextLinkedNodeRecur(long dirRecid, int newHash, int level){
            final Object dir = engine.get(dirRecid, DIR_SERIALIZER);
            if(dir == null)
                return null;
            int offset = Math.abs(
                    dirOffsetFromSlot(dir,
                            (newHash >>> (level * 7)) & 0x7F));

            boolean first = true;
            int dirlen = dirLen(dir);
            while(offset<dirlen){
                long recid = offset<0?0:dirGet(dir,offset);
                if(recid!=0){
                    if((recid&1) == 1){
                        recid = recid>>1;
                        //found linked list, load it into array and return
                        LinkedNode[] array = new LinkedNode[1];
                        int arrayPos = 0;
                        while(recid!=0){
                            LinkedNode ln = engine.get(recid, LN_SERIALIZER);
                            if(ln==null){
                                recid = 0;
                                continue;
                            }
                            //increase array size if needed
                            if(arrayPos == array.length)
                                array = Arrays.copyOf(array, array.length+1);
                            array[arrayPos++] = ln;
                            recid = ln.next;
                        }
                        return array;
                    }else{
                        //found another dir, continue dive
                        recid = recid>>1;
                        LinkedNode[] ret = findNextLinkedNodeRecur(recid, first ? newHash : 0, level - 1);
                        if(ret != null) return ret;
                    }
                }

                first = false;
                offset+=1;
            }
            return null;
        }
    }

    class KeyIterator extends HashIterator implements  Iterator<K>{

        @Override
        public K next() {
            if(currentLinkedList == null)
                throw new NoSuchElementException();
            K key = (K) currentLinkedList[currentLinkedListPos].key;
            moveToNext();
            return key;
        }
    }

    class ValueIterator extends HashIterator implements  Iterator<V>{

        @Override
        public V next() {
            if(currentLinkedList == null)
                throw new NoSuchElementException();
            V value = (V) currentLinkedList[currentLinkedListPos].value;
            moveToNext();
            return value;
        }
    }

    class EntryIterator extends HashIterator implements  Iterator<Entry<K,V>>{

        @Override
        public Entry<K, V> next() {
            if(currentLinkedList == null)
                throw new NoSuchElementException();
            K key = (K) currentLinkedList[currentLinkedListPos].key;
            moveToNext();
            return new Entry2(key);
        }
    }

    class Entry2 implements Entry<K,V>{

        private final K key;

        Entry2(K key) {
            this.key = key;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return HTreeMap.this.get(key);
        }

        @Override
        public V setValue(V value) {
            return HTreeMap.this.put(key,value);
        }

        @Override
        public boolean equals(Object o) {
            return (o instanceof Entry) && keySerializer.equals(key, (K) ((Entry) o).getKey());
        }

        @Override
        public int hashCode() {
            final V value = HTreeMap.this.get(key);
            return (key == null ? 0 : keySerializer.hashCode(key)) ^
                    (value == null ? 0 : value.hashCode());
        }
    }


    @Override
    public V putIfAbsent(K key, V value) {
        if(key==null||value==null) throw new NullPointerException();

        final int h = HTreeMap.this.hash(key);
        final int segment = h >>>28;
        try{
            segmentLocks[segment].writeLock().lock();

            LinkedNode<K,V> ln = HTreeMap.this.getInner(key,h,segment);
            if (ln==null)
                return put(key, value);
            else
                return ln.value;

        }finally {
            segmentLocks[segment].writeLock().unlock();
        }
    }

    @Override
    public boolean remove(Object key, Object value) {
        if(key==null||value==null) throw new NullPointerException();
        final int h = HTreeMap.this.hash(key);
        final int segment = h >>>28;
        try{
            segmentLocks[segment].writeLock().lock();

            LinkedNode otherVal = getInner(key, h, segment);
            if (otherVal!=null && valueSerializer.equals((V)otherVal.value,(V)value)) {
                removeInternal(key, segment, h, true);
                return true;
            }else
                return false;

        }finally {
            segmentLocks[segment].writeLock().unlock();
        }
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        if(key==null||oldValue==null||newValue==null) throw new NullPointerException();
        final int h = HTreeMap.this.hash(key);
        final int segment = h >>>28;
        try{
            segmentLocks[segment].writeLock().lock();

            LinkedNode<K,V> ln = getInner(key, h,segment);
            if (ln!=null && valueSerializer.equals(ln.value, oldValue)) {
                putInner(key, newValue,h,segment);
                return true;
            } else
                return false;

        }finally {
            segmentLocks[segment].writeLock().unlock();
        }
    }

    @Override
    public V replace(K key, V value) {
        if(key==null||value==null) throw new NullPointerException();
        final int h = HTreeMap.this.hash(key);
        final int segment =  h >>>28;
        try{
            segmentLocks[segment].writeLock().lock();

            if (getInner(key,h,segment)!=null)
                return putInner(key, value,h,segment);
            else
                return null;
        }finally {
            segmentLocks[segment].writeLock().unlock();
        }
    }



    protected static final class ExpireLinkNode{

        public final static ExpireLinkNode EMPTY = new ExpireLinkNode(0,0,0,0,0);

        public static final Serializer<ExpireLinkNode> SERIALIZER = new Serializer<ExpireLinkNode>() {
            @Override
            public void serialize(DataOutput out, ExpireLinkNode value) throws IOException {
                if(value == EMPTY) return;
                DataIO.packLong(out, value.prev);
                DataIO.packLong(out, value.next);
                DataIO.packLong(out, value.keyRecid);
                DataIO.packLong(out, value.time);
                out.writeInt(value.hash);
            }

            @Override
            public ExpireLinkNode deserialize(DataInput in, int available) throws IOException {
                if(available==0) return EMPTY;
                return new ExpireLinkNode(
                        DataIO.unpackLong(in), DataIO.unpackLong(in), DataIO.unpackLong(in), DataIO.unpackLong(in),
                        in.readInt()
                );
            }

            @Override
            public boolean isTrusted() {
                return true;
            }
        };

        public final long prev;
        public final long next;
        public final long keyRecid;
        public final long time;
        public final int hash;

        public ExpireLinkNode(long prev, long next, long keyRecid, long time, int hash) {
            this.prev = prev;
            this.next = next;
            this.keyRecid = keyRecid;
            this.time = time;
            this.hash = hash;
        }

        public ExpireLinkNode copyNext(long next2) {
            return new ExpireLinkNode(prev,next2, keyRecid,time,hash);
        }

        public ExpireLinkNode copyPrev(long prev2) {
            return new ExpireLinkNode(prev2,next, keyRecid,time,hash);
        }

        public ExpireLinkNode copyTime(long time2) {
            return new ExpireLinkNode(prev,next,keyRecid,time2,hash);
        }

    }



    protected void expireLinkAdd(int segment, long expireNodeRecid, long keyRecid, int hash){
        if(CC.PARANOID && ! (segmentLocks[segment].writeLock().isHeldByCurrentThread()))
            throw new AssertionError();
        if(CC.PARANOID && ! (expireNodeRecid>0))
            throw new AssertionError();
        if(CC.PARANOID && ! (keyRecid>0))
            throw new AssertionError();

        long time = expire==0 ? 0: expire+System.currentTimeMillis()-expireTimeStart;
        long head = engine.get(expireHeads[segment],Serializer.LONG);
        if(head == 0){
            //insert new
            ExpireLinkNode n = new ExpireLinkNode(0,0,keyRecid,time,hash);
            engine.update(expireNodeRecid, n, ExpireLinkNode.SERIALIZER);
            engine.update(expireHeads[segment],expireNodeRecid,Serializer.LONG);
            engine.update(expireTails[segment],expireNodeRecid,Serializer.LONG);
        }else{
            //insert new head
            ExpireLinkNode n = new ExpireLinkNode(head,0,keyRecid,time,hash);
            engine.update(expireNodeRecid, n, ExpireLinkNode.SERIALIZER);

            //update old head to have new head as next
            ExpireLinkNode oldHead = engine.get(head,ExpireLinkNode.SERIALIZER);
            oldHead=oldHead.copyNext(expireNodeRecid);
            engine.update(head,oldHead,ExpireLinkNode.SERIALIZER);

            //and update head
            engine.update(expireHeads[segment],expireNodeRecid,Serializer.LONG);
        }
    }

    protected void expireLinkBump(int segment, long nodeRecid, boolean access){
        if(CC.PARANOID && ! (segmentLocks[segment].writeLock().isHeldByCurrentThread()))
            throw new AssertionError();

        ExpireLinkNode n = engine.get(nodeRecid,ExpireLinkNode.SERIALIZER);
        long newTime =
                access?
                        (expireAccess==0?0 : expireAccess+System.currentTimeMillis()-expireTimeStart):
                        (expire==0?0 : expire+System.currentTimeMillis()-expireTimeStart);

        //TODO optimize bellow, but what if there is only size limit?
        //if(n.time>newTime) return; // older time greater than new one, do not update

        if(n.next==0){
            //already head, so just update time
            n = n.copyTime(newTime);
            engine.update(nodeRecid,n,ExpireLinkNode.SERIALIZER);
        }else{
            //update prev so it points to next
            if(n.prev!=0){
                //not a tail
                ExpireLinkNode prev = engine.get(n.prev,ExpireLinkNode.SERIALIZER);
                prev=prev.copyNext(n.next);
                engine.update(n.prev, prev, ExpireLinkNode.SERIALIZER);
            }else{
                //yes tail, so just update it to point to next
                engine.update(expireTails[segment],n.next,Serializer.LONG);
            }

            //update next so it points to prev
            ExpireLinkNode next = engine.get(n.next, ExpireLinkNode.SERIALIZER);
            next=next.copyPrev(n.prev);
            engine.update(n.next,next,ExpireLinkNode.SERIALIZER);

            //TODO optimize if oldHead==next

            //now insert node as new head
            long oldHeadRecid = engine.get(expireHeads[segment],Serializer.LONG);
            ExpireLinkNode oldHead = engine.get(oldHeadRecid, ExpireLinkNode.SERIALIZER);
            oldHead = oldHead.copyNext(nodeRecid);
            engine.update(oldHeadRecid,oldHead,ExpireLinkNode.SERIALIZER);
            engine.update(expireHeads[segment],nodeRecid,Serializer.LONG);

            n = new ExpireLinkNode(oldHeadRecid,0, n.keyRecid, newTime, n.hash);
            engine.update(nodeRecid,n,ExpireLinkNode.SERIALIZER);
        }
    }

    protected ExpireLinkNode expireLinkRemoveLast(int segment){
        if(CC.PARANOID && ! (segmentLocks[segment].writeLock().isHeldByCurrentThread()))
            throw new AssertionError();

        long tail = engine.get(expireTails[segment],Serializer.LONG);
        if(tail==0) return null;

        ExpireLinkNode n = engine.get(tail,ExpireLinkNode.SERIALIZER);
        if(n.next==0){
            //update tail and head
            engine.update(expireHeads[segment],0L,Serializer.LONG);
            engine.update(expireTails[segment],0L,Serializer.LONG);
        }else{
            //point tail to next record
            engine.update(expireTails[segment],n.next,Serializer.LONG);
            //update next record to have zero prev
            ExpireLinkNode next = engine.get(n.next,ExpireLinkNode.SERIALIZER);
            next=next.copyPrev(0L);
            engine.update(n.next, next, ExpireLinkNode.SERIALIZER);
        }

        engine.delete(tail,ExpireLinkNode.SERIALIZER);
        return n;
    }


    protected ExpireLinkNode expireLinkRemove(int segment, long nodeRecid){
        if(CC.PARANOID && ! (segmentLocks[segment].writeLock().isHeldByCurrentThread()))
            throw new AssertionError();

        ExpireLinkNode n = engine.get(nodeRecid,ExpireLinkNode.SERIALIZER);
        engine.delete(nodeRecid,ExpireLinkNode.SERIALIZER);
        if(n.next == 0 && n.prev==0){
            engine.update(expireHeads[segment],0L,Serializer.LONG);
            engine.update(expireTails[segment],0L,Serializer.LONG);
        }else if (n.next == 0) {
            ExpireLinkNode prev = engine.get(n.prev,ExpireLinkNode.SERIALIZER);
            prev=prev.copyNext(0);
            engine.update(n.prev,prev,ExpireLinkNode.SERIALIZER);
            engine.update(expireHeads[segment],n.prev,Serializer.LONG);
        }else if (n.prev == 0) {
            ExpireLinkNode next = engine.get(n.next,ExpireLinkNode.SERIALIZER);
            next=next.copyPrev(0);
            engine.update(n.next,next,ExpireLinkNode.SERIALIZER);
            engine.update(expireTails[segment],n.next,Serializer.LONG);
        }else{
            ExpireLinkNode next = engine.get(n.next,ExpireLinkNode.SERIALIZER);
            next=next.copyPrev(n.prev);
            engine.update(n.next,next,ExpireLinkNode.SERIALIZER);

            ExpireLinkNode prev = engine.get(n.prev,ExpireLinkNode.SERIALIZER);
            prev=prev.copyNext(n.next);
            engine.update(n.prev,prev,ExpireLinkNode.SERIALIZER);
        }

        return n;
    }

    /**
     * Returns maximal (newest) expiration timestamp
     */
    public long getMaxExpireTime(){
        if(!expireFlag) return 0;
        long ret = 0;
        for(int segment = 0;segment<16;segment++){
            segmentLocks[segment].readLock().lock();
            try{
                long head = engine.get(expireHeads[segment],Serializer.LONG);
                if(head == 0) continue;
                ExpireLinkNode ln = engine.get(head, ExpireLinkNode.SERIALIZER);
                if(ln==null || ln.time==0) continue;
                ret = Math.max(ret, ln.time+expireTimeStart);
            }finally{
                segmentLocks[segment].readLock().unlock();
            }
        }
        return ret;
    }

    /**
     * Returns minimal (oldest) expiration timestamp
     */
    public long getMinExpireTime(){
        if(!expireFlag) return 0;
        long ret = Long.MAX_VALUE;
        for(int segment = 0;segment<16;segment++){
            segmentLocks[segment].readLock().lock();
            try{
                long tail = engine.get(expireTails[segment],Serializer.LONG);
                if(tail == 0) continue;
                ExpireLinkNode ln = engine.get(tail, ExpireLinkNode.SERIALIZER);
                if(ln==null || ln.time==0) continue;
                ret = Math.min(ret, ln.time+expireTimeStart);
            }finally{
                segmentLocks[segment].readLock().unlock();
            }
        }
        if(ret == Long.MAX_VALUE) ret =0;
        return ret;
    }

    protected static class ExpireRunnable implements  Runnable{

        //use weak referece to prevent memory leak
        final WeakReference<HTreeMap> mapRef;

        public ExpireRunnable(HTreeMap map) {
            this.mapRef = new WeakReference<HTreeMap>(map);
        }

        @Override
        public void run() {
            if(CC.LOG_HTREEMAP && LOG.isLoggable(Level.FINE)){
                LOG.log(Level.FINE, "HTreeMap expirator thread started");
            }
            boolean pause = false;
            try {
                while(true) {

                    if (pause) {
                        Thread.sleep(1000);
                    }


                    HTreeMap map = mapRef.get();
                    if (map == null || map.engine.isClosed() || map.shutdown)
                        return;

                    //TODO what if store gets closed while working on this?
                    map.expirePurge();

                    if (map.engine.isClosed() || map.shutdown)
                        return;

                    pause = ((!map.expireMaxSizeFlag || map.size() < map.expireMaxSize)
                            && (map.expireStoreSize == 0L ||
                            Store.forEngine(map.engine).getCurrSize() - Store.forEngine(map.engine).getFreeSize() < map.expireStoreSize));

                }

            }catch(Throwable e){
                LOG.log(Level.SEVERE, "HTreeMap expirator thread failed", e);
            }finally {
                HTreeMap m = mapRef.get();
                if (m != null)
                    m.expirationThreadNum.countDown();
                mapRef.clear();
                if(CC.LOG_HTREEMAP && LOG.isLoggable(Level.FINE)){
                    LOG.log(Level.FINE, "HTreeMap expirator finished");
                }
            }
        }

    }


    protected void expirePurge(){
        if(!expireFlag) return;

        long removePerSegment = 0;
        if(expireMaxSizeFlag){
            long size = counter.get();
            if(size>expireMaxSize){
                removePerSegment=1+(size-expireMaxSize)/16;
                if(LOG.isLoggable(Level.FINE)){
                    LOG.log(Level.FINE, "HTreeMap expirator expireMaxSize, will remove {0,number,integer} entries per segment",
                            removePerSegment);
                }
            }
        }


        if(expireStoreSize!=0 && removePerSegment==0){
            Store store = Store.forEngine(engine);
            long storeSize = store.getCurrSize()-store.getFreeSize();
            if(expireStoreSize<storeSize){
                removePerSegment=640;
                if(LOG.isLoggable(Level.FINE)){
                    LOG.log(Level.FINE, "HTreeMap store store size ({0,number,integer}) over limit," +
                                    "will remove {1,number,integer} entries per segment",
                            new Object[]{storeSize, removePerSegment});
                }
            }
        }

        long counter = 0;
        for(int seg=0;seg<16;seg++){
            if(shutdown)
                return;
            counter+=expirePurgeSegment(seg, removePerSegment);
        }
        if(LOG.isLoggable(Level.FINE)){
            LOG.log(Level.FINE, "HTreeMap expirator removed {0,number,integer}", counter);
        }

    }

    protected long expirePurgeSegment(int seg, long removePerSegment) {
        segmentLocks[seg].writeLock().lock();
        try{
//            expireCheckSegment(seg);
            long recid = engine.get(expireTails[seg],Serializer.LONG);
            long counter=0;
            ExpireLinkNode last =null,n=null;
            while(recid!=0){
                n = engine.get(recid, ExpireLinkNode.SERIALIZER);
                if(CC.PARANOID && ! (n!=ExpireLinkNode.EMPTY))
                    throw new AssertionError();
                if(CC.PARANOID && ! ( n.hash>>>28 == seg))
                    throw new AssertionError();

                final boolean remove = ++counter < removePerSegment ||
                        ((expire!=0 || expireAccess!=0) &&  n.time+expireTimeStart<System.currentTimeMillis());

                if(remove){
                    engine.delete(recid, ExpireLinkNode.SERIALIZER);
                    LinkedNode<K,V> ln = engine.get(n.keyRecid,LN_SERIALIZER);
                    removeInternal(ln.key,seg, n.hash, false);
                }else{
                    break;
                }
                last=n;
                recid=n.next;
            }
            // patch linked list
            if(last ==null ){
                //no items removed
            }else if(recid == 0){
                //all items were taken, so zero items
                engine.update(expireTails[seg],0L, Serializer.LONG);
                engine.update(expireHeads[seg],0L, Serializer.LONG);
            }else{
                //update tail to point to next item
                engine.update(expireTails[seg],recid, Serializer.LONG);
                //and update next item to point to tail
                n = engine.get(recid, ExpireLinkNode.SERIALIZER);
                n = n.copyPrev(0);
                engine.update(recid,n,ExpireLinkNode.SERIALIZER);
            }
            return counter;
//            expireCheckSegment(seg);
        }finally{
            segmentLocks[seg].writeLock().unlock();
        }
    }


    protected void expireCheckSegment(int segment){
        long current = engine.get(expireTails[segment],Serializer.LONG);
        if(current==0){
            if(engine.get(expireHeads[segment],Serializer.LONG)!=0)
                throw new AssertionError("head not 0");
            return;
        }

        long prev = 0;
        while(current!=0){
            ExpireLinkNode curr = engine.get(current,ExpireLinkNode.SERIALIZER);
            if(CC.PARANOID && ! (curr.prev==prev))
                throw new AssertionError("wrong prev "+curr.prev +" - "+prev);
            prev= current;
            current = curr.next;
        }
        if(engine.get(expireHeads[segment],Serializer.LONG)!=prev)
            throw new AssertionError("wrong head");

    }

    /**
     * Make readonly snapshot view of current Map. Snapshot is immutable and not affected by modifications made by other threads.
     * Useful if you need consistent view on Map.
     * <p>
     * Maintaining snapshot have some overhead, underlying Engine is closed after Map view is GCed.
     * Please make sure to release reference to this Map view, so snapshot view can be garbage collected.
     *
     * @return snapshot
     */
    public Map<K,V> snapshot(){
        Engine snapshot = TxEngine.createSnapshotFor(engine);
        return new HTreeMap<K, V>(snapshot, counter==null?0:counter.recid,
                hashSalt, segmentRecids, keySerializer, valueSerializer,
                0L,0L,0L,0L,0L,
                null,null, null, null);
    }


    protected final Object modListenersLock = new Object();
    protected Bind.MapListener<K,V>[] modListeners = new Bind.MapListener[0];

    @Override
    public void modificationListenerAdd(Bind.MapListener<K, V> listener) {
        synchronized (modListenersLock){
            Bind.MapListener<K,V>[] modListeners2 =
                    Arrays.copyOf(modListeners,modListeners.length+1);
            modListeners2[modListeners2.length-1] = listener;
            modListeners = modListeners2;
        }

    }

    @Override
    public void modificationListenerRemove(Bind.MapListener<K, V> listener) {
        synchronized (modListenersLock){
            for(int i=0;i<modListeners.length;i++){
                if(modListeners[i]==listener) modListeners[i]=null;
            }
        }
    }

    protected void notify(K key, V oldValue, V newValue) {
        if(CC.PARANOID && ! (segmentLocks[hash(key)>>>28].isWriteLockedByCurrentThread()))
            throw new AssertionError();
        Bind.MapListener<K,V>[] modListeners2  = modListeners;
        for(Bind.MapListener<K,V> listener:modListeners2){
            if(listener!=null)
                listener.update(key, oldValue, newValue);
        }
    }

    /**
     * Release resources and shutdown background tasks
     */
    public void close(){
        shutdown = true;
        try {
            if(expirationThreadNum!=null)
                expirationThreadNum.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public Engine getEngine(){
        return engine;
    }

}
