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
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;

/**
 * Thread safe concurrent HashMap
 * <p/>
 * This map uses full 32bit hash from beginning, There is no initial load factor and rehash.
 * Technically it is not hash table, but hash tree with nodes expanding when they become full.
 * <p/>
 * This map is suitable for number of records  1e9 and over.
 * Larger number of records will increase hash collisions and performance
 * will degrade linearly with number of records (separate chaining).
 * <p/>
 * Concurrent scalability is achieved by splitting HashMap into 16 segments, each with separate lock.
 * Very similar to ConcurrentHashMap
 *
 * @author Jan Kotek
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class HTreeMap<K,V>   extends AbstractMap<K,V> implements ConcurrentMap<K, V>, Bind.MapWithModificationListener<K,V> {


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
    protected final Hasher<K> hasher;

    protected final Engine engine;

    protected final boolean expireFlag;
    protected final long expireTimeStart;
    protected final long expire;
    protected final boolean expireAccessFlag;
    protected final long expireAccess;
    protected final long expireMaxSize;
    protected final boolean expireMaxSizeFlag;

    protected final long[] expireHeads;
    protected final long[] expireTails;

    protected final Fun.Function1<V,K> valueCreator;



    /** node which holds key-value pair */
    protected static final class LinkedNode<K,V>{

        public final long next;
        public final long expireLinkNodeRecid;

        public final K key;
        public final V value;

        public LinkedNode(final long next, long expireLinkNodeRecid, final K key, final V value ){
            this.key = key;
            this.expireLinkNodeRecid = expireLinkNodeRecid;
            this.value = value;
            this.next = next;
        }
    }



    protected final Serializer<LinkedNode<K,V>> LN_SERIALIZER = new Serializer<LinkedNode<K,V>>() {
        @Override
        public void serialize(DataOutput out, LinkedNode<K,V> value) throws IOException {
            Utils.packLong(out, value.next);
            if(expireFlag)
                Utils.packLong(out, value.expireLinkNodeRecid);
            keySerializer.serialize(out,value.key);
            if(hasValues)
                valueSerializer.serialize(out,value.value);
        }

        @Override
        public LinkedNode<K,V> deserialize(DataInput in, int available) throws IOException {
            assert(available!=0);
            return new LinkedNode<K, V>(
                    Utils.unpackLong(in),
                    expireFlag?Utils.unpackLong(in):0L,
                    keySerializer.deserialize(in,-1),
                    hasValues? (V) valueSerializer.deserialize(in,-1) : (V) Utils.EMPTY_STRING
            );
        }
    };


    protected static final Serializer<long[][]>DIR_SERIALIZER = new Serializer<long[][]>() {
        @Override
        public void serialize(DataOutput out, long[][] value) throws IOException {
            if(value.length!=16) throw new InternalError();

            //first write mask which indicate subarray nullability
            int nulls = 0;
            for(int i = 0;i<16;i++){
                if(value[i]!=null){
                    for(long l:value[i]){
                        if(l!=0){
                            nulls |= 1<<i;
                            break;
                        }
                    }
                }
            }
            out.writeShort(nulls);

            //write non null subarrays
            for(int i = 0;i<16;i++){
                if(value[i]!=null){
                    if(value[i].length!=8) throw new InternalError();
                    for(long l:value[i]){
                        Utils.packLong(out, l);
                    }
                }
            }
        }


        @Override
        public long[][] deserialize(DataInput in, int available) throws IOException {

            final long[][] ret = new long[16][];

            //there are 16  subarrays, each bite indicates if subarray is null
            int nulls = in.readUnsignedShort();
            for(int i=0;i<16;i++){
                if((nulls & 1)!=0){
                    final long[] subarray = new long[8];
                    for(int j=0;j<8;j++){
                        subarray[j] = Utils.unpackLong(in);
                    }
                    ret[i] = subarray;
                }
                nulls = nulls>>>1;
            }

            return ret;
        }
    };

    /** list of segments, this is immutable*/
    protected final long[] segmentRecids;

    protected final ReentrantReadWriteLock[] segmentLocks = new ReentrantReadWriteLock[16];
    {
        for(int i=0;i< 16;i++)  segmentLocks[i]=new ReentrantReadWriteLock(CC.FAIR_LOCKS);
    }




    /**
     * Opens HTreeMap
     */
    public HTreeMap(Engine engine, long counterRecid, int hashSalt, long[] segmentRecids,
                    Serializer<K> keySerializer, Serializer<V> valueSerializer,
                    long expireTimeStart, long expire, long expireAccess, long expireMaxSize,
                    long[] expireHeads, long[] expireTails, Fun.Function1<V, K> valueCreator,
                    Hasher hasher) {
        if(counterRecid<0) throw new IllegalArgumentException();
        if(engine==null) throw new NullPointerException();
        if(segmentRecids==null) throw new NullPointerException();
        if(keySerializer==null) throw new NullPointerException();

        SerializerBase.assertSerializable(keySerializer);
        this.hasValues = valueSerializer!=null;
        if(hasValues) {
            SerializerBase.assertSerializable(valueSerializer);
        }


        if(segmentRecids.length!=16) throw new IllegalArgumentException();

        this.engine = engine;
        this.hashSalt = hashSalt;
        this.segmentRecids = Arrays.copyOf(segmentRecids,16);
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.hasher = hasher!=null ? hasher : Hasher.BASIC;
        if(expire==0 && expireAccess!=0){
            expire = expireAccess;
        }
        if(expireMaxSize!=0 && counterRecid==0){
            throw new IllegalArgumentException("expireMaxSize must have counter enabled");
        }


        this.expireFlag = expire !=0L || expireAccess!=0L || expireMaxSize!=0;
        this.expire = expire;
        this.expireTimeStart = expireTimeStart;
        this.expireAccessFlag = expireAccess !=0L || expireMaxSize!=0;
        this.expireAccess = expireAccess;
        this.expireHeads = expireHeads==null? null : Arrays.copyOf(expireHeads,16);
        this.expireTails = expireTails==null? null : Arrays.copyOf(expireTails,16);
        this.expireMaxSizeFlag = expireMaxSize!=0;
        this.expireMaxSize = expireMaxSize;
        this.valueCreator = valueCreator;

        if(counterRecid!=0){
            this.counter = new Atomic.Long(engine,counterRecid);
            Bind.size(this,counter);
        }else{
            this.counter = null;
        }

        if(expireFlag){
            Thread t = new Thread(new ExpireRunnable(this),"HTreeMap expirator");
            t.setDaemon(true);
            t.start();
        }

    }



    protected static long[] preallocateSegments(Engine engine){
        //prealocate segmentRecids, so we dont have to lock on those latter
        long[] ret = new long[16];
        for(int i=0;i<16;i++)
            ret[i] = engine.put(new long[16][], DIR_SERIALIZER);
        return ret;
    }



    @Override
    public boolean containsKey(final Object o){
        return getPeek(o)!=null;
    }

    @Override
    public int size() {
        long size = sizeLong();
        if(size>Integer.MAX_VALUE) return Integer.MAX_VALUE;
        return (int) size;
    }


    @Override
    public long sizeLong() {
        if(counter!=null)
            return counter.get();


        long counter = 0;

        //search tree, until we find first non null
        for(int i=0;i<16;i++){
            try{
                segmentLocks[i].readLock().lock();

                final long dirRecid = segmentRecids[i];
                counter+=recursiveDirCount(dirRecid);
            }finally {
                segmentLocks[i].readLock().unlock();
            }
        }


        return counter;
    }

    private long recursiveDirCount(final long dirRecid) {
        long[][] dir = engine.get(dirRecid, DIR_SERIALIZER);
        long counter = 0;
        for(long[] subdir:dir){
            if(subdir == null) continue;
            for(long recid:subdir){
                if(recid == 0) continue;
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
        }
        return counter;
    }

    @Override
    public boolean isEmpty() {
        //search tree, until we find first non null
        for(int i=0;i<16;i++){
            try{
                segmentLocks[i].readLock().lock();

                long dirRecid = segmentRecids[i];
                long[][] dir = engine.get(dirRecid, DIR_SERIALIZER);
                for(long[] d:dir){
                    if(d!=null) return false;
                }

            }finally {
                segmentLocks[i].readLock().unlock();
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
            if(ln==null) return null;
            if(expireAccessFlag) expireLinkBump(segment,ln.expireLinkNodeRecid,true);
        }finally {
            lock.unlock();
        }
        if(valueCreator==null|| ln.value!=null) return ln.value;

        V value = valueCreator.run((K) o);
        V prevVal = putIfAbsent((K) o,value);
        if(prevVal!=null) return prevVal;
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
            long[][] dir = engine.get(recid, DIR_SERIALIZER);
            if(dir == null) return null;
            int slot = (h>>>(level*7 )) & 0x7F;
            if(slot>=128) throw new InternalError();
            if(dir[slot>>>DIV8]==null) return null;
            recid = dir[slot>>>DIV8][slot&MOD8];
            if(recid == 0) return null;
            if((recid&1)!=0){ //last bite indicates if referenced record is LinkedNode
                recid = recid>>>1;
                while(true){
                    LinkedNode<K,V> ln = engine.get(recid, LN_SERIALIZER);
                    if(ln == null) return null;
                    if(hasher.equals(ln.key, (K) o)){
                        assert(hash(ln.key)==h);
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

    @Override
    public V put(final K key, final V value){
        if (key == null)
            throw new IllegalArgumentException("null key");

        if (value == null)
            throw new IllegalArgumentException("null value");

        Utils.checkMapValueIsNotCollecion(value);

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
            long[][] dir = engine.get(dirRecid, DIR_SERIALIZER);
            final int slot =  (h>>>(7*level )) & 0x7F;
            if(slot>127) throw new InternalError();

            if(dir == null ){
                //create new dir
                dir = new long[16][];
            }

            if(dir[slot>>>DIV8] == null){
                dir = Arrays.copyOf(dir, 16);
                dir[slot>>>DIV8] = new long[8];
            }

            int counter = 0;
            long recid = dir[slot>>>DIV8][slot&MOD8];

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
                    if(hasher.equals(ln.key,key)){
                        //found, replace value at this node
                        V oldVal = ln.value;
                        ln = new LinkedNode<K, V>(ln.next, ln.expireLinkNodeRecid, ln.key, value);
                        engine.update(recid, ln, LN_SERIALIZER);
                        if(expireFlag) expireLinkBump(segment,ln.expireLinkNodeRecid,false);
                        notify(key,  oldVal, value);
                        return oldVal;
                    }
                    recid = ln.next;
                    ln = recid==0? null : engine.get(recid, LN_SERIALIZER);
                    counter++;
                }
                //key was not found at linked list, so just append it to beginning
            }


            //check if linked list has overflow and needs to be expanded to new dir level
            if(counter>=BUCKET_OVERFLOW && level>=1){
                long[][] nextDir = new long[16][];

                {
                    final long expireNodeRecid = expireFlag? engine.preallocate():0L;
                    final LinkedNode<K,V> node = new LinkedNode<K, V>(0, expireNodeRecid, key, value);
                    final long newRecid = engine.put(node, LN_SERIALIZER);
                    //add newly inserted record
                    int pos =(h >>>(7*(level-1) )) & 0x7F;
                    nextDir[pos>>>DIV8] = new long[8];
                    nextDir[pos>>>DIV8][pos&MOD8] = ( newRecid<<1) | 1;
                    if(expireFlag) expireLinkAdd(segment,expireNodeRecid,newRecid,h);
                }


                //redistribute linked bucket into new dir
                long nodeRecid = dir[slot>>>DIV8][slot&MOD8]>>>1;
                while(nodeRecid!=0){
                    LinkedNode<K,V> n = engine.get(nodeRecid, LN_SERIALIZER);
                    final long nextRecid = n.next;
                    int pos = (hash(n.key) >>>(7*(level -1) )) & 0x7F;
                    if(nextDir[pos>>>DIV8]==null) nextDir[pos>>>DIV8] = new long[8];
                    n = new LinkedNode<K, V>(nextDir[pos>>>DIV8][pos&MOD8]>>>1, n.expireLinkNodeRecid, n.key, n.value);
                    nextDir[pos>>>DIV8][pos&MOD8] = (nodeRecid<<1) | 1;
                    engine.update(nodeRecid, n, LN_SERIALIZER);
                    nodeRecid = nextRecid;
                }

                //insert nextDir and update parent dir
                long nextDirRecid = engine.put(nextDir, DIR_SERIALIZER);
                int parentPos = (h>>>(7*level )) & 0x7F;
                dir = Arrays.copyOf(dir,16);
                dir[parentPos>>>DIV8] = Arrays.copyOf(dir[parentPos>>>DIV8],8);
                dir[parentPos>>>DIV8][parentPos&MOD8] = (nextDirRecid<<1) | 0;
                engine.update(dirRecid, dir, DIR_SERIALIZER);
                notify(key, null, value);
                return null;
            }else{
                // record does not exist in linked list, so create new one
                recid = dir[slot>>>DIV8][slot&MOD8]>>>1;
                final long expireNodeRecid = expireFlag? engine.put(ExpireLinkNode.EMPTY, ExpireLinkNode.SERIALIZER):0L;

                final long newRecid = engine.put(new LinkedNode<K, V>(recid, expireNodeRecid, key, value), LN_SERIALIZER);
                dir = Arrays.copyOf(dir,16);
                dir[slot>>>DIV8] = Arrays.copyOf(dir[slot>>>DIV8],8);
                dir[slot>>>DIV8][slot&MOD8] = (newRecid<<1) | 1;
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

            assert(segment==h>>>28);

            while(true){
                long[][] dir = engine.get(dirRecids[level], DIR_SERIALIZER);
                final int slot =  (h>>>(7*level )) & 0x7F;
                if(slot>127) throw new InternalError();

                if(dir == null ){
                    //create new dir
                    dir = new long[16][];
                }

                if(dir[slot>>>DIV8] == null){
                    dir = Arrays.copyOf(dir,16);
                    dir[slot>>>DIV8] = new long[8];
                }

//                int counter = 0;
                long recid = dir[slot>>>DIV8][slot&MOD8];

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
                        if(hasher.equals(ln.key, (K) key)){
                            //remove from linkedList
                            if(prevLn == null ){
                                //referenced directly from dir
                                if(ln.next==0){
                                    recursiveDirDelete(h, level, dirRecids, dir, slot);


                                }else{
                                    dir=Arrays.copyOf(dir,16);
                                    dir[slot>>>DIV8] = Arrays.copyOf(dir[slot>>>DIV8],8);
                                    dir[slot>>>DIV8][slot&MOD8] = (ln.next<<1)|1;
                                    engine.update(dirRecids[level], dir, DIR_SERIALIZER);
                                }

                            }else{
                                //referenced from LinkedNode
                                prevLn = new LinkedNode<K, V>(ln.next, prevLn.expireLinkNodeRecid,prevLn.key, prevLn.value);
                                engine.update(prevRecid, prevLn, LN_SERIALIZER);
                            }
                            //found, remove this node
                            assert(hash(ln.key)==h);
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


    private void recursiveDirDelete(int h, int level, long[] dirRecids, long[][] dir, int slot) {
        //was only item in linked list, so try to collapse the dir
        dir=Arrays.copyOf(dir,16);
        dir[slot>>>DIV8] = Arrays.copyOf(dir[slot>>>DIV8],8);
        dir[slot>>>DIV8][slot&MOD8] = 0;
        //one record was zeroed out, check if subarray can be collapsed to null
        boolean allZero = true;
        for(long l:dir[slot>>>DIV8]){
            if(l!=0){
                allZero = false;
                break;
            }
        }
        if(allZero){
            dir[slot>>>DIV8] = null;
        }
        allZero = true;
        for(long[] l:dir){
            if(l!=null){
                allZero = false;
                break;
            }
        }

        if(allZero){
            //delete from parent dir
            if(level==3){
                //parent is segment, recid of this dir can not be modified,  so just update to null
                engine.update(dirRecids[level], new long[16][], DIR_SERIALIZER);
            }else{
                engine.delete(dirRecids[level], DIR_SERIALIZER);

                final long[][] parentDir = engine.get(dirRecids[level + 1], DIR_SERIALIZER);
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
            engine.update(dirRecid, new long[16][], DIR_SERIALIZER);

            if(expireFlag)
                while(expireLinkRemoveLast(i)!=null){} //TODO speedup remove all

        }finally {
            segmentLocks[i].writeLock().unlock();
        }
    }

    private void recursiveDirClear(final long dirRecid) {
        final long[][] dir = engine.get(dirRecid, DIR_SERIALIZER);
        if(dir == null) return;
        for(long[] subdir:dir){
            if(subdir==null) continue;
            for(long recid:subdir){
                if(recid == 0) continue;
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
                        engine.delete(recid,LN_SERIALIZER);
                        notify((K)n.key, (V)n.value , null);
                        recid = n.next;
                    }
                }

            }
        }
    }


    @Override
    public boolean containsValue(Object value) {
        for (V v : values()) {
            if (v.equals(value)) return true;
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
                return HTreeMap.this.put(k, (V) Utils.EMPTY_STRING) == null;
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
            Iterator<K> it = iterator();
            while (it.hasNext()) {
                result += hasher.hashCode(it.next());
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

    private Set<Entry<K,V>> _entrySet = new AbstractSet<Entry<K,V>>(){

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
                return val!=null && val.equals(e.getValue());
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
        int h = hasher.hashCode((K) key) ^ hashSalt;
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
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

            int segment = lastHash >>>28;

            //two phases, first find old item and increase hash
            try{
                segmentLocks[segment].readLock().lock();

                long dirRecid = segmentRecids[segment];
                int level = 3;
                //dive into tree, finding last hash position
                while(true){
                    long[][] dir = engine.get(dirRecid, DIR_SERIALIZER);
                    int pos = (lastHash>>>(7 * level)) & 0x7F;

                    //check if we need to expand deeper
                    if(dir[pos>>>DIV8]==null || dir[pos>>>DIV8][pos&MOD8]==0 || (dir[pos>>>DIV8][pos&MOD8]&1)==1) {
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
                    dirRecid = dir[pos>>>DIV8][pos&MOD8]>>>1;
                    level--;
                }

            }finally {
                segmentLocks[segment].readLock().unlock();
            }
            return findNextLinkedNode(lastHash);


        }

        private LinkedNode[] findNextLinkedNode(int hash) {
            //second phase, start search from increased hash to find next items
            for(int segment = Math.max(hash >>>28, lastSegment); segment<16;segment++){
                final Lock lock = expireAccessFlag ? segmentLocks[segment].writeLock() :segmentLocks[segment].readLock() ;
                lock.lock();
                try{
                    lastSegment = Math.max(segment,lastSegment);
                    long dirRecid = segmentRecids[segment];
                    LinkedNode ret[] = findNextLinkedNodeRecur(dirRecid, hash, 3);
                    if(ret!=null) for(LinkedNode ln:ret){
                        assert hash(ln.key)>>>28==segment;
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
            long[][] dir = engine.get(dirRecid, DIR_SERIALIZER);
            if(dir == null) return null;
            int pos = (newHash>>>(level*7))  & 0x7F;
            boolean first = true;
            while(pos<128){
                if(dir[pos>>>DIV8]!=null){
                    long recid = dir[pos>>>DIV8][pos&MOD8];
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
                }
                first = false;
                pos++;
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
            return (o instanceof Entry) && hasher.equals(key, (K) ((Entry) o).getKey());
        }

        @Override
        public int hashCode() {
            final V value = HTreeMap.this.get(key);
            return (key == null ? 0 : hasher.hashCode(key)) ^
                    (value == null ? 0 : value.hashCode());
        }
    }


    @Override
    public V putIfAbsent(K key, V value) {
        if(key==null||value==null) throw new NullPointerException();
        Utils.checkMapValueIsNotCollecion(value);
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
            if (otherVal!=null && otherVal.value.equals(value)) {
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
            if (ln!=null && ln.value.equals(oldValue)) {
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

        public static ExpireLinkNode EMPTY = new ExpireLinkNode(0,0,0,0,0);

        public static final Serializer<ExpireLinkNode> SERIALIZER = new Serializer<ExpireLinkNode>() {
            @Override
            public void serialize(DataOutput out, ExpireLinkNode value) throws IOException {
                if(value == EMPTY) return;
                Utils.packLong(out,value.prev);
                Utils.packLong(out,value.next);
                Utils.packLong(out,value.keyRecid);
                Utils.packLong(out,value.time);
                out.writeInt(value.hash);
            }

            @Override
            public ExpireLinkNode deserialize(DataInput in, int available) throws IOException {
                if(available==0) return EMPTY;
                return new ExpireLinkNode(
                        Utils.unpackLong(in),Utils.unpackLong(in),Utils.unpackLong(in),Utils.unpackLong(in),
                        in.readInt()
                );
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
        assert(segmentLocks[segment].writeLock().isHeldByCurrentThread());
        assert(expireNodeRecid>0);
        assert(keyRecid>0);

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
        assert(segmentLocks[segment].writeLock().isHeldByCurrentThread());

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
        assert(segmentLocks[segment].writeLock().isHeldByCurrentThread());

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
        assert(segmentLocks[segment].writeLock().isHeldByCurrentThread());

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
            while(true){
                HTreeMap map = mapRef.get();
                if(map==null||map.engine.isClosed()) return;
                try{
                    //TODO what if store gets closed while working on this?
                    map.expirePurge();
                    if(!map.expireMaxSizeFlag || map.size()<map.expireMaxSize)
                        Thread.sleep(1000);
                }catch(Throwable e){
                    //TODO exception handling
                    e.printStackTrace();
                    Utils.LOG.log(Level.SEVERE, "HTreeMap expirator failed", e);
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
            }
        }

        for(int seg=0;seg<16;seg++){
            exirePurgeSegment(seg,removePerSegment);
        }

    }

    protected void exirePurgeSegment(int seg, long removePerSegment) {
        segmentLocks[seg].writeLock().lock();
        try{
//            expireCheckSegment(seg);
            long recid = engine.get(expireTails[seg],Serializer.LONG);
            long counter=0;
            ExpireLinkNode last =null,n=null;
            while(recid!=0){
                n = engine.get(recid, ExpireLinkNode.SERIALIZER);
                assert(n!=ExpireLinkNode.EMPTY);
                assert( n.hash>>>28 == seg);

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
                return;
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
//            expireCheckSegment(seg);
        }finally{
            segmentLocks[seg].writeLock().unlock();
        }
    }


    protected void expireCheckSegment(int segment){
        long current = engine.get(expireTails[segment],Serializer.LONG);
        if(current==0){
            if(engine.get(expireHeads[segment],Serializer.LONG)!=0)
                throw new InternalError("head not 0");
            return;
        }

        long prev = 0;
        while(current!=0){
            ExpireLinkNode curr = engine.get(current,ExpireLinkNode.SERIALIZER);
            if(curr.prev!=prev)
                throw new InternalError("wrong prev "+curr.prev +" - "+prev);
            prev= current;
            current = curr.next;
        }
        if(engine.get(expireHeads[segment],Serializer.LONG)!=prev)
            throw new InternalError("wrong head");

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
                hashSalt, segmentRecids, keySerializer, valueSerializer,0L,0L,0L,0L,null,null, null, null);
    }


    protected final Object modListenersLock = new Object();
    protected Bind.MapListener<K,V>[] modListeners = new Bind.MapListener[0];

    @Override
    public void addModificationListener(Bind.MapListener<K,V> listener) {
        synchronized (modListenersLock){
            Bind.MapListener<K,V>[] modListeners2 =
                    Arrays.copyOf(modListeners,modListeners.length+1);
            modListeners2[modListeners2.length-1] = listener;
            modListeners = modListeners2;
        }

    }

    @Override
    public void removeModificationListener(Bind.MapListener<K,V> listener) {
        synchronized (modListenersLock){
            for(int i=0;i<modListeners.length;i++){
                if(modListeners[i]==listener) modListeners[i]=null;
            }
        }
    }

    protected void notify(K key, V oldValue, V newValue) {
        Bind.MapListener<K,V>[] modListeners2  = modListeners;
        for(Bind.MapListener<K,V> listener:modListeners2){
            if(listener!=null)
                listener.update(key, oldValue, newValue);
        }
    }

    /**
     * Closes underlying storage and releases all resources.
     * Used mostly with temporary collections where engine is not accessible.
     */
    public void close(){
        engine.close();
    }

}
