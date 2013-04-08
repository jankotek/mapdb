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
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    /** is this a Map or Set?  if false, entries do not have values, only keys are allowed*/
    protected final boolean hasValues;

    /**
     * Salt added to hash before rehashing, so it is harder to trigger hash collision attack.
     */
    protected final int hashSalt;

    protected final Atomic.Long counter;

    protected final Serializer<K> keySerializer;
    protected final Serializer<V> valueSerializer;

    protected final Serializer defaultSerialzierForSnapshots;




    /** node which holds key-value pair */
    protected static class LinkedNode<K,V>{
        final K key;
        final V value;
        final long next;

        LinkedNode(final long next, final K key, final V value ){
            this.key = key;
            this.value = value;
            this.next = next;
        }
    }

    static class HashRootSerializer implements Serializer<HashRoot>{

        private Serializer defaultSerializer;

        public HashRootSerializer(Serializer defaultSerializer) {
            this.defaultSerializer = defaultSerializer;
        }

        @Override
        public void serialize(DataOutput out, HashRoot value) throws IOException {
            out.writeBoolean(value.hasValues);
            out.writeInt(value.hashSalt);
            out.writeLong(value.counterRecid);
            for(int i=0;i<16;i++){
                Utils.packLong(out, value.segmentRecids[i]);
            }
            defaultSerializer.serialize(out,value.keySerializer);
            defaultSerializer.serialize(out,value.valueSerializer);

        }

        @Override
        public HashRoot deserialize(DataInput in, int available) throws IOException {
            if(available==0) return null;
            HashRoot r = new HashRoot();
            r.hasValues = in.readBoolean();
            r.hashSalt = in.readInt();
            r.counterRecid = in.readLong();
            r.segmentRecids = new long[16];
            for(int i=0;i<16;i++){
                r.segmentRecids[i] = Utils.unpackLong(in);
            }
            r.keySerializer = (Serializer) defaultSerializer.deserialize(in, -1);
            r.valueSerializer = (Serializer) defaultSerializer.deserialize(in, -1);
            return r;
        }

    }


    static class HashRoot{
        long[] segmentRecids;
        boolean hasValues;
        int hashSalt;
        long counterRecid;
        Serializer keySerializer;
        Serializer valueSerializer;

    }


    final Serializer<LinkedNode<K,V>> LN_SERIALIZER = new Serializer<LinkedNode<K,V>>() {
        @Override
        public void serialize(DataOutput out, LinkedNode<K,V> value) throws IOException {
            Utils.packLong(out, value.next);
            keySerializer.serialize(out,value.key);
            if(hasValues)
                valueSerializer.serialize(out,value.value);
        }

        @Override
        public LinkedNode<K,V> deserialize(DataInput in, int available) throws IOException {
            return new LinkedNode<K, V>(
                    Utils.unpackLong(in),
                    (K) keySerializer.deserialize(in,-1),
                    hasValues? (V) valueSerializer.deserialize(in,-1) : (V) Utils.EMPTY_STRING
            );
        }
    };


    static final Serializer<long[][]>DIR_SERIALIZER = new Serializer<long[][]>() {
        @Override
        public void serialize(DataOutput out, long[][] value) throws IOException {
            if(value.length!=16) throw new InternalError();

            //first write mask which indicate subarray nullability
            int nulls = 0;
            for(int i = 0;i<16;i++){
                if(value[i]!=null)
                    nulls |= 1<<i;
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
        for(int i=0;i< 16;i++)  segmentLocks[i]=new ReentrantReadWriteLock();
    }

    protected final Engine engine;
    public final long rootRecid;


    /**
     * Constructor used to create new HTreeMap without existing record (recid) in Engine.
     * This constructor creates new record and saves all configuration parameters there.
     * Constructor args are defining HTreeMap format, are stored in db and can not be changed latter.
     *
     * @param engine used for persistence
     * @param hasValues is Map or Set? If true only keys will be stored, no values
     * @param defaultSerializer serialier used to serialize/deserialize other serializers. May be null for default value.
     * @param keySerializer Serializier used for keys. May be null for default value.
     * @param valueSerializer Serializer used for values. May be null for default value
     */
    public HTreeMap(Engine engine, boolean hasValues, boolean keepCounter, int hashSalt, Serializer defaultSerializer, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.engine = engine;
        this.hasValues = hasValues;
        this.hashSalt = hashSalt;


        SerializerBase.assertSerializable(keySerializer);
        SerializerBase.assertSerializable(valueSerializer);


        if(defaultSerializer == null) defaultSerializer = Serializer.BASIC_SERIALIZER;
        this.defaultSerialzierForSnapshots = defaultSerializer;
        this.keySerializer = keySerializer==null ? (Serializer<K>) defaultSerializer : keySerializer;
        this.valueSerializer = valueSerializer==null ? (Serializer<V>) defaultSerializer : valueSerializer;


        //prealocate segmentRecids, so we dont have to lock on those latter
        segmentRecids = new long[16];
        for(int i=0;i<16;i++)
            segmentRecids[i] = engine.put(new long[16][], DIR_SERIALIZER);

        long counterRecid = 0;
        if(keepCounter){
            counterRecid = engine.put(0L, Serializer.LONG_SERIALIZER);
            this.counter = new Atomic.Long(engine,counterRecid);
            Bind.size(this,counter);
        }else{
            this.counter = null;
        }

        HashRoot r = new HashRoot();
        r.hasValues = hasValues;
        r.hashSalt = hashSalt;
        r.counterRecid = counterRecid;
        r.segmentRecids = segmentRecids;
        r.keySerializer = this.keySerializer;
        r.valueSerializer = this.valueSerializer;
        this.rootRecid = engine.put(r, new HashRootSerializer(defaultSerializer));

    }

    /**
     * Constructor used to load existing HTreeMap (with assigned recid).
     * Map was already created and saved to Engine, this constructor just loads it.
     *
     * @param engine used for persistence
     * @param rootRecid under which BTreeMap was stored
     * @param defaultSerializer used to deserialize other serializers and comparator
     */
    public HTreeMap(Engine engine, long rootRecid, Serializer defaultSerializer) {
        if(rootRecid == 0) throw new IllegalArgumentException("recid is 0");
        this.engine = engine;
        this.rootRecid = rootRecid;
        //load all fields from store
        if(defaultSerializer==null) defaultSerializer = Serializer.BASIC_SERIALIZER;
        this.defaultSerialzierForSnapshots = defaultSerializer;
        HashRoot r = engine.get(rootRecid, new HashRootSerializer(defaultSerializer));
        this.segmentRecids = r.segmentRecids;
        this.hasValues = r.hasValues;
        this.hashSalt = r.hashSalt;
        this.keySerializer = r.keySerializer;
        this.valueSerializer = r.valueSerializer;

        if(r.counterRecid!=0){
            counter = new Atomic.Long(engine,r.counterRecid);
            Bind.size(this,counter);
        }else{
            this.counter = null;
        }
    }

    /** hack used for Dir Name*/
    static final Map<String, Long> preinitNamedDir(Engine engine){
        HashRootSerializer serializer = new HashRootSerializer(Serializer.BASIC_SERIALIZER);
        //check if record already exist
        HashRoot r = engine.get(Engine.NAME_DIR_RECID, serializer);
        if(r!=null)
            return new HTreeMap<String, Long>(engine, Engine.NAME_DIR_RECID, Serializer.BASIC_SERIALIZER);

        if(engine.isReadOnly())
            return Collections.unmodifiableMap(new HashMap<String, Long>());

        //prealocate segmentRecids
        long[] segmentRecids = new long[16];
        for(int i=0;i<16;i++)
            segmentRecids[i] = engine.put(new long[16][], DIR_SERIALIZER);
        r = new HashRoot();
        r.hasValues = true;
        r.segmentRecids = segmentRecids;
        r.keySerializer = Serializer.BASIC_SERIALIZER;
        r.valueSerializer = Serializer.BASIC_SERIALIZER;
        engine.update(Engine.NAME_DIR_RECID, r, serializer);
        //and now load it
        return new HTreeMap<String, Long>(engine, Engine.NAME_DIR_RECID, Serializer.BASIC_SERIALIZER);

    }

    @Override
    public boolean containsKey(final Object o){
        return get(o)!=null;
    }

    @Override
    public int size() {
        if(counter!=null)
            return (int) counter.get(); //TODO larger then MAX_INT


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

        if(counter>Integer.MAX_VALUE)
            return Integer.MAX_VALUE;

        return (int) counter;
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
        try{
            segmentLocks[segment].readLock().lock();
            long recid = segmentRecids[segment];
            for(int level=3;level>=0;level--){
                long[][] dir = engine.get(recid, DIR_SERIALIZER);
                if(dir == null) return null;
                int slot = (h>>>(level*7 )) & 0x7F;
                if(slot>=128) throw new InternalError();
                if(dir[slot/8]==null) return null;
                recid = dir[slot/8][slot%8];
                if(recid == 0) return null;
                if((recid&1)!=0){ //last bite indicates if referenced record is LinkedNode
                    recid = recid>>>1;
                    while(true){
                        LinkedNode<K,V> ln = engine.get(recid, LN_SERIALIZER);
                        if(ln == null) return null;
                        if(ln.key.equals(o)) return ln.value;
                        if(ln.next==0) return null;
                        recid = ln.next;
                    }
                }

                recid = recid>>>1;
            }

            return null;
        }finally {
            segmentLocks[segment].readLock().unlock();
        }
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
        try{
            segmentLocks[segment].writeLock().lock();
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

                if(dir[slot/8] == null){
                    dir[slot/8] = new long[8];
                }

                int counter = 0;
                long recid = dir[slot/8][slot%8];

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
                        if(ln.key.equals(key)){
                            //found, replace value at this node
                            V oldVal = ln.value;
                            ln = new LinkedNode<K, V>(ln.next, ln.key, value);
                            engine.update(recid, ln, LN_SERIALIZER);
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
                        //add newly inserted record
                        int pos =(h >>>(7*(level-1) )) & 0x7F;
                        nextDir[pos/8] = new long[8];
                        nextDir[pos/8][pos%8] = (engine.put(new LinkedNode<K, V>(0, key, value), LN_SERIALIZER) <<1) | 1;
                    }


                    //redistribute linked bucket into new dir
                    long nodeRecid = dir[slot/8][slot%8]>>>1;
                    while(nodeRecid!=0){
                        LinkedNode<K,V> n = engine.get(nodeRecid, LN_SERIALIZER);
                        final long nextRecid = n.next;
                        int pos = (hash(n.key) >>>(7*(level -1) )) & 0x7F;
                        if(nextDir[pos/8]==null) nextDir[pos/8] = new long[8];
                        n = new LinkedNode<K, V>(nextDir[pos/8][pos%8]>>>1, n.key, n.value);
                        nextDir[pos/8][pos%8] = (nodeRecid<<1) | 1;
                        engine.update(nodeRecid, n, LN_SERIALIZER);
                        nodeRecid = nextRecid;
                    }

                    //insert nextDir and update parent dir
                    long nextDirRecid = engine.put(nextDir, DIR_SERIALIZER);
                    int parentPos = (h>>>(7*level )) & 0x7F;
                    dir[parentPos/8][parentPos%8] = (nextDirRecid<<1) | 0;
                    engine.update(dirRecid, dir, DIR_SERIALIZER);
                    notify(key, null, value);
                    return null;
                }else{
                    // record does not exist in linked list, so create new one
                    recid = dir[slot/8][slot%8]>>>1;
                    long newRecid = engine.put(new LinkedNode<K, V>(recid, key, value), LN_SERIALIZER);
                    dir[slot/8][slot%8] = (newRecid<<1) | 1;
                    engine.update(dirRecid, dir, DIR_SERIALIZER);
                    notify(key, null, value);
                    return null;
                }
            }

        }finally {
            segmentLocks[segment].writeLock().unlock();
        }
    }


    @Override
    public V remove(Object key){

        final int h = hash(key);
        final int segment = h >>>28;
        try{
            segmentLocks[segment].writeLock().lock();

            final  long[] dirRecids = new long[4];
            int level = 3;
            dirRecids[level] = segmentRecids[segment];

            while(true){
                long[][] dir = engine.get(dirRecids[level], DIR_SERIALIZER);
                final int slot =  (h>>>(7*level )) & 0x7F;
                if(slot>127) throw new InternalError();

                if(dir == null ){
                    //create new dir
                    dir = new long[16][];
                }

                if(dir[slot/8] == null){
                    dir[slot/8] = new long[8];
                }

//                int counter = 0;
                long recid = dir[slot/8][slot%8];

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
                        if(ln.key.equals(key)){
                            //remove from linkedList
                            if(prevLn == null ){
                                //referenced directly from dir
                                if(ln.next==0){
                                    recursiveDirDelete(h, level, dirRecids, dir, slot);


                                }else{
                                    dir[slot/8][slot%8] = (ln.next<<1)|1;
                                    engine.update(dirRecids[level], dir, DIR_SERIALIZER);
                                }

                            }else{
                                //referenced from LinkedNode
                                prevLn = new LinkedNode<K, V>(ln.next, prevLn.key, prevLn.value);
                                engine.update(prevRecid, prevLn, LN_SERIALIZER);
                            }
                            //found, remove this node
                            engine.delete(recid, LN_SERIALIZER);
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
        }finally {
            segmentLocks[segment].writeLock().unlock();
        }
    }


    private void recursiveDirDelete(int h, int level, long[] dirRecids, long[][] dir, int slot) {
        //was only item in linked list, so try to collapse the dir
        dir[slot/8][slot%8] = 0;
        //one record was zeroed out, check if subarray can be collapsed to null
        boolean allZero = true;
        for(long l:dir[slot/8]){
            if(l!=0){
                allZero = false;
                break;
            }
        }
        if(allZero)
            dir[slot/8] = null;
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
                //parentDir[parentPos/8][parentPos%8] = 0;
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

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for(Entry<? extends K, ? extends V> e:m.entrySet()){
            put(e.getKey(),e.getValue());
        }
    }


    private final Set<K> _keySet = new AbstractSet<K>() {

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
    };

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


    protected  int hash(final Object key) {
        int h = key.hashCode() ^ hashSalt;
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
    }


    abstract class HashIterator{

        protected Object[] currentLinkedList;
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
            lastReturnedKey = (K) currentLinkedList[currentLinkedListPos];
            currentLinkedListPos+=2;
            if(currentLinkedListPos==currentLinkedList.length){
                final int lastHash = hash(lastReturnedKey);
                currentLinkedList = advance(lastHash);
                currentLinkedListPos = 0;
            }
        }

        private Object[] advance(int lastHash){

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
                    if(dir[pos/8]==null || dir[pos/8][pos%8]==0 || (dir[pos/8][pos%8]&1)==1) {
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
                    dirRecid = dir[pos/8][pos%8]>>>1;
                    level--;
                }

            }finally {
                segmentLocks[segment].readLock().unlock();
            }
            return findNextLinkedNode(lastHash);


        }

        private Object[] findNextLinkedNode(int hash) {
            //second phase, start search from increased hash to find next items
            for(int segment = Math.max(hash >>>28, lastSegment); segment<16;segment++)try{

                lastSegment = Math.max(segment,lastSegment);
                segmentLocks[segment].readLock().lock();

                long dirRecid = segmentRecids[segment];
                Object ret[] = findNextLinkedNodeRecur(dirRecid, hash, 3);
                //System.out.println(Arrays.asList(ret));
                if(ret !=null) return ret;
                hash = 0;
            }finally {
                segmentLocks[segment].readLock().unlock();
            }

            return null;
        }

        private Object[] findNextLinkedNodeRecur(long dirRecid, int newHash, int level){
            long[][] dir = engine.get(dirRecid, DIR_SERIALIZER);
            if(dir == null) return null;
            int pos = (newHash>>>(level*7))  & 0x7F;
            boolean first = true;
            while(pos<128){
                if(dir[pos/8]!=null){
                    long recid = dir[pos/8][pos%8];
                    if(recid!=0){
                        if((recid&1) == 1){
                            recid = recid>>1;
                            //found linked list, load it into array and return
                            Object[] array = new Object[2];
                            int arrayPos = 0;
                            while(recid!=0){
                                LinkedNode ln = engine.get(recid, LN_SERIALIZER);
                                if(ln==null){
                                    recid = 0;
                                    continue;
                                }
                                //increase array size if needed
                                if(arrayPos == array.length)
                                    array = Arrays.copyOf(array, array.length+2);
                                array[arrayPos++] = ln.key;
                                array[arrayPos++] = ln.value;
                                recid = ln.next;
                            }
                            return array;
                        }else{
                            //found another dir, continue dive
                            recid = recid>>1;
                            Object[] ret = findNextLinkedNodeRecur(recid, first ? newHash : 0, level - 1);
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
            K key = (K) currentLinkedList[currentLinkedListPos];
            moveToNext();
            return key;
        }
    }

    class ValueIterator extends HashIterator implements  Iterator<V>{

        @Override
        public V next() {
        	if(currentLinkedList == null)
        		throw new NoSuchElementException();
            V value = (V) currentLinkedList[currentLinkedListPos+1];
            moveToNext();
            return value;
        }
    }

    class EntryIterator extends HashIterator implements  Iterator<Entry<K,V>>{

        @Override
        public Entry<K, V> next() {
        	if(currentLinkedList == null)
        		throw new NoSuchElementException();
            K key = (K) currentLinkedList[currentLinkedListPos];
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
            return (o instanceof Entry) && key.equals(((Entry) o).getKey());
        }

        @Override
        public int hashCode() {
            final V value = HTreeMap.this.get(key);
            return (key == null ? 0 : key.hashCode()) ^
                    (value == null ? 0 : value.hashCode());
        }
    }


    @Override
    public V putIfAbsent(K key, V value) {
        if(key==null||value==null) throw new NullPointerException();
        Utils.checkMapValueIsNotCollecion(value);
        final int segment = HTreeMap.this.hash(key) >>>28;
        try{
            segmentLocks[segment].writeLock().lock();

            if (!containsKey(key))
                 return put(key, value);
            else
                 return get(key);

        }finally {
            segmentLocks[segment].writeLock().unlock();
        }
    }

    @Override
    public boolean remove(Object key, Object value) {
        if(key==null||value==null) throw new NullPointerException();
        final int segment = HTreeMap.this.hash(key) >>>28;
        try{
            segmentLocks[segment].writeLock().lock();

            if (containsKey(key) && get(key).equals(value)) {
                remove(key);
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
        final int segment = HTreeMap.this.hash(key) >>>28;
        try{
            segmentLocks[segment].writeLock().lock();

            if (containsKey(key) && get(key).equals(oldValue)) {
                 put(key, newValue);
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
        final int segment = HTreeMap.this.hash(key) >>>28;
        try{
            segmentLocks[segment].writeLock().lock();

            if (containsKey(key))
                return put(key, value);
            else
                return null;
        }finally {
            segmentLocks[segment].writeLock().unlock();
        }
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
        Engine snapshot = SnapshotEngine.createSnapshotFor(engine);
        return new HTreeMap<K, V>(snapshot,rootRecid, defaultSerialzierForSnapshots);
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
