
/*
 * NOTE: some code (and javadoc) used in this class
 * comes from Apache Harmony with following copyright:
 *
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */


package org.mapdb;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * A scalable concurrent {@link ConcurrentNavigableMap} implementation.
 * The map is sorted according to the {@linkplain Comparable natural
 * ordering} of its keys, or by a {@link Comparator} provided at map
 * creation time.
 *
 * <p>Insertion, removal,
 * update, and access operations safely execute concurrently by
 * multiple threads.  Iterators are <i>weakly consistent</i>, returning
 * elements reflecting the state of the map at some point at or since
 * the creation of the iterator.  They do <em>not</em> throw {@link
 * ConcurrentModificationException}, and may proceed concurrently with
 * other operations. Ascending key ordered views and their iterators
 * are faster than descending ones.
 *
 * <p>All <tt>Map.Entry</tt> pairs returned by methods in this class
 * and its views represent snapshots of mappings at the time they were
 * produced. They do <em>not</em> support the <tt>Entry.setValue</tt>
 * method. (Note however that it is possible to change mappings in the
 * associated map using <tt>put</tt>, <tt>putIfAbsent</tt>, or
 * <tt>replace</tt>, depending on exactly which effect you need.)
 *
 * <p>Beware that, unlike in most collections, the <tt>size</tt>
 * method is <em>not</em> a constant-time operation. Because of the
 * asynchronous nature of these maps, determining the current number
 * of elements requires a traversal of the elements.  Additionally,
 * the bulk operations <tt>putAll</tt>, <tt>equals</tt>, and
 * <tt>clear</tt> are <em>not</em> guaranteed to be performed
 * atomically. For example, an iterator operating concurrently with a
 * <tt>putAll</tt> operation might view only some of the added
 * elements.
 *
 * <p>This class and its views and iterators implement all of the
 * <em>optional</em> methods of the {@link Map} and {@link Iterator}
 * interfaces. Like most other concurrent collections, this class does
 * <em>not</em> permit the use of <tt>null</tt> keys or values because some
 * null return values cannot be reliably distinguished from the absence of
 * elements.
 *
 */
@SuppressWarnings("unchecked")
public class BTreeMap<K,V> extends AbstractMap<K,V>
        implements ConcurrentNavigableMap<K,V>{


    public static final int DEFAULT_MAX_NODE_SIZE = 32;


    protected long rootRecid;

    protected final Serializer keySerializer;
    protected final Serializer<V> valueSerializer;
    protected final Comparator comparator;

    protected final LongConcurrentHashMap<Thread> nodeWriteLocks = new LongConcurrentHashMap<Thread>();

    protected final int maxNodeSize;

    protected final Engine engine;

    protected final boolean hasValues;

    protected final boolean valsOutsideNodes;

    protected long treeRecid;

    private final BTreeRootSerializer btreeRootSerializer;


    private final KeySet keySet;

    private final EntrySet entrySet = new EntrySet(this);

    private final Values values = new Values(this);


    static class BTreeRootSerializer implements  Serializer<BTreeRoot>{
        private final Serializer defaultSerializer;

        BTreeRootSerializer(Serializer defaultSerializer) {
            this.defaultSerializer = defaultSerializer;
        }

        @Override
        public void serialize(DataOutput out, BTreeRoot value) throws IOException {
            out.writeLong(value.rootRecid);
            out.writeBoolean(value.hasValues);
            out.writeBoolean(value.valsOutsideNodes);
            out.writeInt(value.maxNodeSize);
            defaultSerializer.serialize(out, value.keySerializer);
            defaultSerializer.serialize(out, value.valueSerializer);
            defaultSerializer.serialize(out, value.comparator);

        }

        @Override
        public BTreeRoot deserialize(DataInput in, int available) throws IOException {
            BTreeRoot ret = new BTreeRoot();
            ret.rootRecid = in.readLong();
            ret.hasValues = in.readBoolean();
            ret.valsOutsideNodes = in.readBoolean();
            ret.maxNodeSize = in.readInt();
            ret.keySerializer = (Serializer) defaultSerializer.deserialize(in, -1);
            ret.valueSerializer = (Serializer) defaultSerializer.deserialize(in, -1);
            ret.comparator = (Comparator) defaultSerializer.deserialize(in, -1);
            return ret;
        }
    }
    static class BTreeRoot{
        long rootRecid;
        boolean hasValues;
        boolean valsOutsideNodes;
        int maxNodeSize;
        Serializer keySerializer;
        Serializer valueSerializer;
        Comparator comparator;



    }

    protected static final class ValRef{
        final long recid;
        public ValRef(long recid) {
            this.recid = recid;
        }

        @Override
        public boolean equals(Object obj) {
            throw new InternalError();
        }

        @Override
        public int hashCode() {
            throw new InternalError();
        }

    }


    protected interface BNode{
        boolean isLeaf();
        Object[] keys();
        Object[] vals();
        Object highKey();
        long[] child();

        long next();
    }

    protected final static class DirNode implements BNode{
        final Object[] keys;
        final long[] child;

        DirNode(Object[] keys, long[] child) {
            this.keys = keys;
            this.child = child;
        }

        @Override public boolean isLeaf() { return false;}

        @Override public Object[] keys() { return keys;}
        @Override public Object[] vals() { return null;}

        @Override public Object highKey() {return keys[keys.length-1];}

        @Override public long[] child() { return child;}

        @Override public long next() {return child[child().length-1];}

        @Override public String toString(){
            return "Dir(K"+Arrays.toString(keys)+", C"+Arrays.toString(child)+")";
        }

    }


    protected final static class LeafNode implements BNode{
        final Object[] keys;
        final Object[] vals;
        final long next;

        LeafNode(Object[] keys, Object[] vals, long next) {
            this.keys = keys;
            this.vals = vals;
            this.next = next;
        }

        @Override public boolean isLeaf() { return true;}

        @Override public Object[] keys() { return keys;}
        @Override public Object[] vals() { return vals;}

        @Override public Object highKey() {return keys[keys.length-1];}

        @Override public long[] child() { return null;}
        @Override public long next() {return next;}

        @Override public String toString(){
            return "Leaf(K"+Arrays.toString(keys)+", V"+Arrays.toString(vals)+", L="+next+")";
        }
    }


    final Serializer<BNode> nodeSerializer = new Serializer<BNode>() {
        @Override
        public void serialize(DataOutput out, BNode value) throws IOException {
            final boolean isLeaf = value.isLeaf();

            //first byte encodes if is leaf (first bite) and length (last seven bites)
            if(CC.ASSERT && value.keys().length>127) throw new InternalError();
            if(CC.ASSERT && !isLeaf && value.child().length!= value.keys().length) throw new InternalError();
            if(CC.ASSERT && isLeaf && hasValues && value.vals().length!= value.keys().length) throw new InternalError();

            final int header = (isLeaf?0x80:0)  | value.keys().length;
            out.write(header);

            //longs go first, so it is possible to reconstruct tree without serializer
            if(isLeaf){
                Utils.packLong(out, ((LeafNode) value).next);
            }else{
                for(long child : ((DirNode)value).child)
                    Utils.packLong(out, child);
            }

            //write keys
            keySerializer.serialize(out, (K[]) value.keys());

            if(isLeaf && hasValues){
                for(int i=0; i<value.vals().length; i++){
                    Object val = value.vals()[i];
                    if(valsOutsideNodes){
                        long recid = val!=null?  ((ValRef)val).recid :0;
                        Utils.packLong(out, recid);
                    }else{
                        valueSerializer.serialize(out, (V) val);
                    }
                }
            }
        }

        @Override
        public BNode deserialize(DataInput in, int available) throws IOException {
            int size = in.readUnsignedByte();
            //first bite indicates leaf
            final boolean isLeaf = (size & 0x80) != 0;
            //rest is for node size
            size = size & 0x7f;

            if(isLeaf){
                long next = Utils.unpackLong(in);
                Object[] keys = (Object[]) keySerializer.deserialize(in, size);
                if(keys.length!=size) throw new InternalError();
                Object[] vals  = null;
                if(hasValues){
                    vals = new Object[size];
                    for(int i=0;i<size;i++){
                        if(valsOutsideNodes){
                            long recid = Utils.unpackLong(in);
                            vals[i] = recid==0? null: new ValRef(recid);
                        }else{
                            vals[i] = valueSerializer.deserialize(in, size-1);
                        }
                    }
                }
                return new LeafNode(keys, vals, next);
            }else{
                long[] child = new long[size];
                for(int i=0;i<size;i++)
                    child[i] = Utils.unpackLong(in);
                Object[] keys = (Object[]) keySerializer.deserialize(in, size);
                if(keys.length!=size) throw new InternalError();
                return new DirNode(keys, child);
            }
        }
    };


    /** constructor used to create new tree*/
    public BTreeMap(Engine engine, int maxNodeSize, boolean hasValues, boolean valsOutsideNodes,
                    Serializer defaultSerializer,
                    Serializer<K[]> keySerializer, Serializer<V> valueSerializer, Comparator<K> comparator) {
        if(maxNodeSize%2!=0) throw new IllegalArgumentException("maxNodeSize must be dividable by 2");
        if(maxNodeSize<6) throw new IllegalArgumentException("maxNodeSize too low");
        if(maxNodeSize>126) throw new IllegalArgumentException("maxNodeSize too high");
        if(defaultSerializer==null) defaultSerializer = Serializer.BASIC_SERIALIZER;
        this.btreeRootSerializer = new BTreeRootSerializer(defaultSerializer);
        this.hasValues = hasValues;
        this.valsOutsideNodes = valsOutsideNodes;
        this.engine = engine;
        this.maxNodeSize = maxNodeSize;
        this.comparator = comparator==null? Utils.COMPARABLE_COMPARATOR : comparator;
        this.keySerializer = keySerializer==null ?  defaultSerializer :  keySerializer;
        this.valueSerializer = valueSerializer==null ? (Serializer<V>) defaultSerializer : valueSerializer;

        this.keySet = new KeySet(this, hasValues);

        LeafNode emptyRoot = new LeafNode(new Object[]{null, null}, new Object[]{null, null}, 0);
        this.rootRecid = engine.recordPut(emptyRoot, nodeSerializer);

        saveTreeInfo();
    }

    protected void saveTreeInfo() {
        BTreeRoot r = new BTreeRoot();
        r.hasValues = hasValues;
        r.valsOutsideNodes = valsOutsideNodes;
        r.rootRecid = rootRecid;
        r.maxNodeSize = maxNodeSize;
        r.keySerializer = keySerializer;
        r.valueSerializer = valueSerializer;
        r.comparator = comparator;
        if(treeRecid == 0){
            treeRecid = engine.recordPut(r,btreeRootSerializer);
        }else{
            engine.recordUpdate(treeRecid, r, btreeRootSerializer);
        }
    }


    /**
     * Constructor used to load existing tree
     */
    public BTreeMap(Engine engine, long recid, Serializer defaultSerializer) {
        this.engine = engine;
        this.treeRecid = recid;
        if(defaultSerializer==null) defaultSerializer = Serializer.BASIC_SERIALIZER;
        this.btreeRootSerializer = new BTreeRootSerializer(defaultSerializer);
        BTreeRoot r = engine.recordGet(recid, btreeRootSerializer);
        this.hasValues = r.hasValues;
        this.rootRecid = r.rootRecid;
        this.maxNodeSize = r.maxNodeSize;
        this.keySerializer = r.keySerializer;
        this.valueSerializer = r.valueSerializer;
        this.comparator = r.comparator;
        this.valsOutsideNodes = r.valsOutsideNodes;

        this.keySet = new KeySet(this, hasValues);
    }


    protected void unlockNode(final long nodeRecid) {
        if(CC.BTREEMAP_LOG_NODE_LOCKS)
            Utils.LOG.finest("BTreeMap UNLOCK R:"+nodeRecid+" T:"+Thread.currentThread().getId());

        final Thread t = nodeWriteLocks.remove(nodeRecid);
        if(t!=Thread.currentThread())
            throw new InternalError("unlocked wrong thread");

    }

    protected void assertNoLocks(){
        if(CC.PARANOID){
            LongMap.LongMapIterator<Thread> i = nodeWriteLocks.longMapIterator();
            while(i.moveToNext()){
                if(i.value()==Thread.currentThread()){
                    throw new InternalError("Node "+i.key()+" is still locked");
                }
            }
        }
    }

    protected void lockNode(final long nodeRecid) {
        if(CC.BTREEMAP_LOG_NODE_LOCKS)
            Utils.LOG.finest("BTreeMap TRYLOCK R:"+nodeRecid+" T:"+Thread.currentThread().getId());

        //feel free to rewrite, if you know better (more efficient) way
        if(CC.ASSERT && nodeWriteLocks.get(nodeRecid)==Thread.currentThread()){
            //check node is not already locked by this thread
            throw new InternalError("node already locked by current thread: "+nodeRecid);
        }


        while(nodeWriteLocks.putIfAbsent(nodeRecid, Thread.currentThread()) != null){
            Thread.yield();
        }
        if(CC.BTREEMAP_LOG_NODE_LOCKS)
            Utils.LOG.finest("BTreeMap LOCK R:"+nodeRecid+" T:"+Thread.currentThread().getId());

    }

    /**
     * Find the first children node with a key equal or greater than the given key.
     * If all items are smaller it returns `keys.length`
     */
    protected final int findChildren(final Object key, final Object[] keys) {
        int left = 0;
        if(keys[0] == null) left++;
        int right = keys[keys.length-1] == null ? keys.length-1 :  keys.length;

        int middle;

        // binary search
        while (true) {
            middle = (left + right) / 2;
            if(keys[middle]==null) return middle; //null is positive infinitive
            if (comparator.compare(keys[middle], key) < 0) {
                left = middle + 1;
            } else {
                right = middle;
            }
            if (left >= right) {
                return  right;
            }
        }

    }

    public V get(Object key){
        if(key==null) return null;
        K v = (K) key;
        long current = rootRecid;
        BNode A = engine.recordGet(current, nodeSerializer);

        //dive until  leaf
        while(!A.isLeaf()){
            current = nextDir((DirNode) A, v);
            A = engine.recordGet(current, nodeSerializer);
        }

        //now at leaf level
        LeafNode leaf = (LeafNode) A;
        int pos = findChildren(v, leaf.keys);
        while(pos == leaf.keys.length){
            //follow next link on leaf until necessary
            leaf = (LeafNode) engine.recordGet(leaf.next, nodeSerializer);
            pos = findChildren(v, leaf.keys);
        }

        if(pos == 1 && leaf.keys.length==2){
            return null; //empty node
        }
        //finish search
        if(v.equals(leaf.keys[pos])){
            Object ret = (hasValues? leaf.vals[pos] : Utils.EMPTY_STRING);
            return valExpand(ret);
        }else
            return null;
    }

    protected V valExpand(Object ret) {
        if(valsOutsideNodes && ret!=null) {
            long recid = ((ValRef)ret).recid;
            ret = engine.recordGet(recid, valueSerializer);
        }
        return (V) ret;
    }

    protected long nextDir(DirNode d, Object key) {
        int pos = findChildren(key, d.keys) - 1;
        if(pos<0) pos = 0;
        return d.child[pos];
    }


    @Override
    public V put(K key, V value){
        return put2(key,value, false);
    }

    protected V put2(K v, V value, final boolean putOnlyIfAbsent){
        if(v == null) throw new IllegalArgumentException("null key");
        if(value == null) throw new IllegalArgumentException("null value");

        if(valsOutsideNodes){
            long recid = engine.recordPut(value, valueSerializer);
            value = (V) new ValRef(recid);
        }

        int stackPos = -1;
        long[] stackVals = new long[4];

        long current = rootRecid;

        BNode A = engine.recordGet(current, nodeSerializer);
        while(!A.isLeaf()){
            long t = current;
            current = nextDir((DirNode) A, v);
            if(current == A.child()[A.child().length-1]){
                //is link, do nothing
            }else{
                //stack push t
                stackPos++;
                if(stackVals.length == stackPos) //grow if needed
                    stackVals = Arrays.copyOf(stackVals, stackVals.length*2);
                stackVals[stackPos] = t;
            }
            A = engine.recordGet(current, nodeSerializer);
        }
        int level = 1;

        long p=0;

        while(true){
            boolean found;
            do{
                lockNode(current);
                found = true;
                A = engine.recordGet(current, nodeSerializer);
                int pos = findChildren(v, A.keys());
                if(pos<A.keys().length-1 && v.equals(A.keys()[pos])){

                    Object oldVal =  (hasValues? A.vals()[pos] : Utils.EMPTY_STRING);
                    if(putOnlyIfAbsent){
                        //is not absent, so quit
                        unlockNode(current);
                        assertNoLocks();
                        return valExpand(oldVal);
                    }
                    //insert new
                    Object[] vals = null;
                    if(hasValues){
                        vals = Arrays.copyOf(A.vals(), A.vals().length);
                        vals[pos] = value;
                    }

                    A = new LeafNode(Arrays.copyOf(A.keys(), A.keys().length), vals, ((LeafNode)A).next);
                    engine.recordUpdate(current, A, nodeSerializer);
                    //already in here
                    unlockNode(current);
                    assertNoLocks();
                    return valExpand(oldVal);
                }

                if(A.highKey() != null && comparator.compare(v, A.highKey())>0){
                    //follow link until necessary
                    unlockNode(current);
                    found = false;
                    int pos2 = findChildren(v, A.keys());
                    while(A!=null && pos2 == A.keys().length){
                        //TODO lock?
                        long next = A.next();

                        if(next==0) break;
                        current = next;
                        A = engine.recordGet(current, nodeSerializer);
                    }

                }


            }while(!found);

            // can be new item inserted into A without splitting it?
            if(A.keys().length - (A.isLeaf()?2:1)<maxNodeSize){
                int pos = findChildren(v, A.keys());
                Object[] keys = Utils.arrayPut(A.keys(), pos, v);

                if(A.isLeaf()){
                    Object[] vals = hasValues? Utils.arrayPut(A.vals(), pos, value): null;
                    LeafNode n = new LeafNode(keys, vals, ((LeafNode)A).next);
                    engine.recordUpdate(current, n, nodeSerializer);
                }else{
                    if(CC.ASSERT && p==0)
                        throw new InternalError();
                    long[] child = Utils.arrayLongPut(A.child(), pos, p);
                    DirNode d = new DirNode(keys, child);
                    engine.recordUpdate(current, d, nodeSerializer);
                }

                unlockNode(current);
                assertNoLocks();
                return null;
            }else{
                //node is not safe, it requires splitting
                final boolean isRoot = (current == rootRecid);

                final int pos = findChildren(v, A.keys());
                final Object[] keys = Utils.arrayPut(A.keys(), pos, v);
                final Object[] vals = (A.isLeaf() && hasValues)? Utils.arrayPut(A.vals(), pos, value) : null;
                final long[] child = A.isLeaf()? null : Utils.arrayLongPut(A.child(), pos, p);
                final int splitPos = keys.length/2;
                BNode B;
                if(A.isLeaf()){
                    Object[] vals2 = null;
                    if(hasValues){
                        vals2 = Arrays.copyOfRange(vals, splitPos, vals.length);
                        vals2[0] = null;
                    }

                    B = new LeafNode(
                                Arrays.copyOfRange(keys, splitPos, keys.length),
                                vals2,
                                ((LeafNode)A).next);
                }else{
                    B = new DirNode(Arrays.copyOfRange(keys, splitPos, keys.length),
                                Arrays.copyOfRange(child, splitPos, keys.length));
                }
                long q = engine.recordPut(B, nodeSerializer);
                if(A.isLeaf()){  //  splitPos+1 is there so A gets new high  value (key)
                    Object[] keys2 = Arrays.copyOf(keys, splitPos+2);
                    keys2[keys2.length-1] = keys2[keys2.length-2];
                    Object[] vals2 = null;
                    if(hasValues){
                        vals2 = Arrays.copyOf(vals, splitPos+2);
                        vals2[vals2.length-1] = null;
                    }

                    //TODO check high/low keys overlap
                    A = new LeafNode(keys2, vals2, q);
                }else{
                    long[] child2 = Arrays.copyOf(child, splitPos+1);
                    child2[splitPos] = q;
                    A = new DirNode(Arrays.copyOf(keys, splitPos+1), child2);
                }
                engine.recordUpdate(current, A, nodeSerializer);

                if(!isRoot){
                    unlockNode(current);
                    p = q;
                    v = (K) A.highKey();
                    level = level+1;
                    if(stackPos!=-1){ //if stack is not empty
                        current = stackVals[stackPos--];
                    }else{
                        current = -1; //TODO pointer to left most node at level level
                        throw new InternalError();
                    }
                }else{
                    BNode R = new DirNode(
                            new Object[]{A.keys()[0], A.highKey(), B.highKey()},
                            new long[]{current,q, 0});
                    rootRecid = engine.recordPut(R, nodeSerializer);
                    saveTreeInfo();
                    //TODO update tree levels
                    unlockNode(current);
                    assertNoLocks();
                    return null;
                }
            }
        }
    }


    class BTreeIterator{
        LeafNode currentLeaf;
        K lastReturnedKey;
        int currentPos;

        BTreeIterator(){
            //find left-most leaf
            BNode node = engine.recordGet(rootRecid, nodeSerializer);
            while(!node.isLeaf()){
                node = engine.recordGet(node.child()[0], nodeSerializer);
            }
            currentLeaf = (LeafNode) node;
            currentPos = 1;

            while(currentLeaf.keys.length==2){
                //follow link until leaf is not empty
                if(currentLeaf.next == 0){
                    currentLeaf = null;
                    return;
                }
                currentLeaf = (LeafNode) engine.recordGet(currentLeaf.next, nodeSerializer);
            }
        }

        public boolean hasNext(){
            return currentLeaf!=null;
        }

        public void remove(){
            if(lastReturnedKey==null) throw new IllegalStateException();
            BTreeMap.this.remove(lastReturnedKey);
            lastReturnedKey = null;
        }

        protected void moveToNext(){
            if(currentLeaf==null) return;
            lastReturnedKey = (K) currentLeaf.keys[currentPos];
            currentPos++;
            if(currentPos == currentLeaf.keys.length-1){
                //move to next leaf
                if(currentLeaf.next==0){
                    currentLeaf = null;
                    currentPos=-1;
                    return;
                }
                currentPos = 1;
                currentLeaf = (LeafNode) engine.recordGet(currentLeaf.next, nodeSerializer);
                while(currentLeaf.keys.length==2){
                    if(currentLeaf.next ==0){
                        currentLeaf = null;
                        currentPos=-1;
                        return;
                    }
                    currentLeaf = (LeafNode) engine.recordGet(currentLeaf.next, nodeSerializer);
                }
            }
        }
    }

    public V remove(Object key) {
        return remove2(key, null);
    }

    private V remove2(Object key, Object value) {
        long current = rootRecid;
        BNode A = engine.recordGet(current, nodeSerializer);
        while(!A.isLeaf()){
            current = nextDir((DirNode) A, key);
            A = engine.recordGet(current, nodeSerializer);
        }

        while(true){

            lockNode(current);
            A = engine.recordGet(current, nodeSerializer);
            int pos = findChildren(key, A.keys());
            if(pos<A.keys().length&& key.equals(A.keys()[pos])){
                //delete from node
                Object oldVal = hasValues? A.vals()[pos] : Utils.EMPTY_STRING;
                oldVal = valExpand(oldVal);
                if(value!=null && !value.equals(oldVal))
                    return null;

                Object[] keys2 = new Object[A.keys().length-1];
                System.arraycopy(A.keys(),0,keys2, 0, pos);
                System.arraycopy(A.keys(), pos+1, keys2, pos, keys2.length-pos);

                Object[] vals2 = null;
                if(hasValues){
                    vals2 = new Object[A.vals().length-1];
                    System.arraycopy(A.vals(),0,vals2, 0, pos);
                    System.arraycopy(A.vals(), pos+1, vals2, pos, vals2.length-pos);
                }

                A = new LeafNode(keys2, vals2, ((LeafNode)A).next);
                engine.recordUpdate(current, A, nodeSerializer);
                unlockNode(current);
                return (V) oldVal;
            }else{
                unlockNode(current);
                //follow link until necessary
                if(A.highKey() != null && comparator.compare(key, A.highKey())>0){
                    int pos2 = findChildren(key, A.keys());
                    while(pos2 == A.keys().length){
                        //TODO lock?
                        current = ((LeafNode)A).next;
                        A = engine.recordGet(current, nodeSerializer);
                    }
                }else{
                    return null;
                }
            }
        }

    }


    @Override
    public void clear() {
        Iterator iter = keyIterator();
        while(iter.hasNext()){
            iter.next();
            iter.remove();
        }
    }


    class BTreeKeyIterator extends BTreeIterator implements Iterator<K>{

        @Override
        public K next() {
            if(currentLeaf == null) throw new NoSuchElementException();
            K ret = (K) currentLeaf.keys[currentPos];
            moveToNext();
            return ret;
        }
    }

    class BTreeValueIterator extends BTreeIterator implements Iterator<V>{

        @Override
        public V next() {
            if(currentLeaf == null) throw new NoSuchElementException();
            Object ret = currentLeaf.vals[currentPos];
            moveToNext();
            return valExpand(ret);
        }

    }

    class BTreeEntryIterator extends BTreeIterator implements  Iterator<Entry<K, V>>{

        @Override
        public Entry<K, V> next() {
            if(currentLeaf == null) throw new NoSuchElementException();
            K ret = (K) currentLeaf.keys[currentPos];
            Object val = currentLeaf.vals[currentPos];
            moveToNext();
            return makeEntry(ret, valExpand(val));

        }
    }






    protected Entry<K, V> makeEntry(Object key, Object value) {
        if(CC.ASSERT && value instanceof ValRef) throw new InternalError();
        return new SimpleImmutableEntry<K, V>((K)key,  (V)value);
    }


    @Override
    public boolean isEmpty() {
        return !keyIterator().hasNext();
    }

    @Override
    public int size(){
        long size = 0;
        BTreeIterator iter = new BTreeIterator();
        while(iter.hasNext()){
            iter.moveToNext();
            size++;
        }
        return (int) size;
    }

    @Override
    public V putIfAbsent(K key, V value) {
        if(key == null || value == null) throw new NullPointerException();
        return put2(key, value, true);
    }

    @Override
    public boolean remove(Object key, Object value) {
        if(key == null || value == null) return false;
        return remove2(key, value)!=null;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        if(key == null || oldValue == null || newValue == null ) throw new NullPointerException();

        long current = rootRecid;
        BNode node = engine.recordGet(current, nodeSerializer);
        //dive until leaf is found
        while(!node.isLeaf()){
            current = nextDir((DirNode) node, key);
            node = engine.recordGet(current, nodeSerializer);
        }

        lockNode(current);
        LeafNode leaf = (LeafNode) engine.recordGet(current, nodeSerializer);

        int pos = findChildren(key, node.keys());
        while(pos==leaf.keys.length){
            //follow leaf link until necessary
            lockNode(leaf.next);
            unlockNode(current);
            current = leaf.next;
            leaf = (LeafNode) engine.recordGet(current, nodeSerializer);
            pos = findChildren(key, node.keys());
        }

        boolean ret = false;
        if(key.equals(leaf.keys[pos])){
            Object val  = leaf.vals[pos];
            val = valExpand(val);
            if(oldValue.equals(val)){
                Object[] vals = Arrays.copyOf(leaf.vals, leaf.vals.length);
                if(valsOutsideNodes){
                    long recid = engine.recordPut(newValue, valueSerializer);
                    newValue = (V) new ValRef(recid);
                }
                vals[pos] = newValue;
                leaf = new LeafNode(Arrays.copyOf(leaf.keys, leaf.keys.length), vals, leaf.next);

                engine.recordUpdate(current, leaf, nodeSerializer);

                ret = true;
            }
        }
        unlockNode(current);
        return ret;
    }

    @Override
    public V replace(K key, V value) {
        if(key == null || value == null) throw new NullPointerException();

        long current = rootRecid;
        BNode node = engine.recordGet(current, nodeSerializer);
        //dive until leaf is found
        while(!node.isLeaf()){
            current = nextDir((DirNode) node, key);
            node = engine.recordGet(current, nodeSerializer);
        }

        lockNode(current);
        LeafNode leaf = (LeafNode) engine.recordGet(current, nodeSerializer);

        int pos = findChildren(key, node.keys());
        while(pos==leaf.keys.length){
            //follow leaf link until necessary
            lockNode(leaf.next);
            unlockNode(current);
            current = leaf.next;
            leaf = (LeafNode) engine.recordGet(current, nodeSerializer);
            pos = findChildren(key, node.keys());
        }

        Object ret = null;
        if(key.equals(leaf.keys[pos])){
            Object[] vals = Arrays.copyOf(leaf.vals, leaf.vals.length);
            Object oldVal = vals[pos];
            if(valsOutsideNodes && value!=null){
                long recid = engine.recordPut(value, valueSerializer);
                value = (V) new ValRef(recid);
            }
            vals[pos] = value;
            leaf = new LeafNode(Arrays.copyOf(leaf.keys, leaf.keys.length), vals, leaf.next);
            engine.recordUpdate(current, leaf, nodeSerializer);

            ret =  valExpand(oldVal);
        }
        unlockNode(current);
        return (V)ret;
    }


    @Override
    public Comparator<? super K> comparator() {
        return comparator;
    }


    @Override
    public Map.Entry<K,V> firstEntry() {
        BNode n = engine.recordGet(rootRecid, nodeSerializer);
        while(!n.isLeaf()){
            n = engine.recordGet(n.child()[0], nodeSerializer);
        }
        LeafNode l = (LeafNode) n;
        //follow link until necessary
        while(l.keys.length==2){
            if(l.next==0) return null;
            l = (LeafNode) engine.recordGet(l.next, nodeSerializer);
        }
        return makeEntry(l.keys[1], valExpand(l.vals[1]));
    }


    @Override
    public Entry<K, V> pollFirstEntry() {
        while(true){
            Entry<K, V> e = firstEntry();
            if(e==null || remove(e.getKey(),e.getValue())){
                return e;
            }
        }
    }

    @Override
    public Entry<K, V> pollLastEntry() {
        while(true){
            Entry<K, V> e = lastEntry();
            if(e==null || remove(e.getKey(),e.getValue())){
                return e;
            }
        }
    }


    protected Entry<K,V> findSmaller(K key,boolean inclusive){
        BNode n = engine.recordGet(rootRecid, nodeSerializer);
        return findSmallerRecur(n, key, inclusive);
    }

    private Entry<K, V> findSmallerRecur(final BNode n, final K key, final boolean inclusive) {
        if(n.isLeaf()){
            final int p = inclusive?0:1;
            for(int i=0;i<n.keys().length;i++){
                //find first item not matching condition
                Object key2 = n.keys()[i];
                if(key2!=null && comparator.compare(key, key2) <p){
                    //and return previous
                    return makeEntry(n.keys()[i-1], valExpand(n.vals()[i-1]));
                }
            }

            //TODO follow next link if nothing found?
        }else{
            //TODO follow next
            final int pos = findChildren(key,n.keys());
            for(int i=pos; i>=0; i--){
                BNode n2 = engine.recordGet(n.child()[i], nodeSerializer);
                Entry<K,V> ret = findSmallerRecur(n2, key, inclusive);
                if(ret == null) return ret;
            }
        }
        return null;
    }


    @Override
    public Map.Entry<K,V> lastEntry() {
        BNode n = engine.recordGet(rootRecid, nodeSerializer);
        return lastEntryRecur(n);
    }


    private Map.Entry<K,V> lastEntryRecur(BNode n){
        if(n.isLeaf()){
            //follow next node if available
            if(n.next()!=0){
                BNode n2 = engine.recordGet(n.next(), nodeSerializer);
                return lastEntryRecur(n2);
            }

            //iterate over keys to find last non null key
            for(int i=n.keys().length-1; i>=0;i--){
                Object k = n.keys()[i];
                if(k!=null) return makeEntry(k, valExpand(n.vals()[i]));
            }
        }else{
            //dir node, dive deeper
            for(int i=n.child().length-1; i>=0;i--){
                long childRecid = n.child()[i];
                if(childRecid==0) continue;
                BNode n2 = engine.recordGet(childRecid, nodeSerializer);
                Entry<K,V> ret = lastEntryRecur(n2);
                if(ret!=null) return ret;
            }
        }
        return null;
    }



    public Map.Entry<K,V> lowerEntry(K key) {
        return findSmaller(key, false);
    }



    public K lowerKey(K key) {
        Entry<K,V> n = lowerEntry(key);
        return (n == null)? null : n.getKey();
    }

    public Map.Entry<K,V> floorEntry(K key) {
        return findSmaller(key, true);
    }

    public K floorKey(K key) {
        Entry<K,V> n = floorEntry(key);
        return (n == null)? null : n.getKey();
    }

    public Map.Entry<K,V> ceilingEntry(K key) {
        return findLarger(key, true);
    }

    protected Entry<K, V> findLarger(K key, boolean inclusive) {
        if(key==null) return null;
        K v = (K) key;
        long current = rootRecid;
        BNode A = engine.recordGet(current, nodeSerializer);

        //dive until  leaf
        while(!A.isLeaf()){
            current = nextDir((DirNode) A, v);
            A = engine.recordGet(current, nodeSerializer);
        }

        //now at leaf level
        LeafNode leaf = (LeafNode) A;
        //follow link until first matching node is found
        final int comp = inclusive?1:0;
        while(true){
            for(int i=0;i<leaf.keys.length;i++){
                if(leaf.vals[i] == null|| leaf.keys[i]==null) continue;

                if(comparator.compare(key, leaf.keys[i])<comp)
                    return makeEntry(leaf.keys[i], leaf.vals[i]);
            }
            if(leaf.next==0) return null; //reached end
            leaf = (LeafNode) engine.recordGet(leaf.next, nodeSerializer);
        }

    }


    public K ceilingKey(K key) {
        Entry<K,V> n = ceilingEntry(key);
        return (n == null)? null : n.getKey();
    }


    public Map.Entry<K,V> higherEntry(K key) {
        return findLarger(key, false);
    }

    public K higherKey(K key) {
        Entry<K,V> n = higherEntry(key);
        return (n == null)? null : n.getKey();
    }





    @Override
    public boolean containsKey(Object key) {
        return get(key)!=null;
    }

    @Override
    public boolean containsValue(Object value){
        if(value ==null) return false;
        Iterator<V> valueIter = valueIterator();
        while(valueIter.hasNext()){
            if(value.equals(valueIter.next()))
                return true;
        }
        return false;
    }


    @Override
    public K firstKey() {
        Entry<K,V> e = firstEntry();
        return e==null? null : e.getKey();
    }

    @Override
    public K lastKey() {
        Entry<K,V> e = lastEntry();
        return e==null? null : e.getKey();
    }


    @Override
    public ConcurrentNavigableMap<K,V> subMap(K fromKey,
                                              boolean fromInclusive,
                                              K toKey,
                                              boolean toInclusive) {
        if (fromKey == null || toKey == null)
            throw new NullPointerException();
        return new SubMap<K,V>
                ( this, fromKey, fromInclusive, toKey, toInclusive);
    }

    @Override
    public ConcurrentNavigableMap<K,V> headMap(K toKey,
                                               boolean inclusive) {
        if (toKey == null)
            throw new NullPointerException();
        return new SubMap<K,V>
                (this, null, false, toKey, inclusive);
    }

    @Override
    public ConcurrentNavigableMap<K,V> tailMap(K fromKey,
                                               boolean inclusive) {
        if (fromKey == null)
            throw new NullPointerException();
        return new SubMap<K,V>
                (this, fromKey, inclusive, null, false);
    }

    @Override
    public ConcurrentNavigableMap<K,V> subMap(K fromKey, K toKey) {
        return subMap(fromKey, true, toKey, false);
    }

    @Override
    public ConcurrentNavigableMap<K,V> headMap(K toKey) {
        return headMap(toKey, false);
    }

    @Override
    public ConcurrentNavigableMap<K,V> tailMap(K fromKey) {
        return tailMap(fromKey, true);
    }


    Iterator<K> keyIterator() {
        return new BTreeKeyIterator();
    }

    Iterator<V> valueIterator() {
        return new BTreeValueIterator();
    }

    Iterator<Map.Entry<K,V>> entryIterator() {
        return new BTreeEntryIterator();
    }


    /* ---------------- View methods -------------- */

    public NavigableSet<K> keySet() {
        return keySet;
    }

    public NavigableSet<K> navigableKeySet() {
        return keySet;
    }

    public Collection<V> values() {
        return values;
    }

    public Set<Map.Entry<K,V>> entrySet() {
        return entrySet;
    }

    public ConcurrentNavigableMap<K,V> descendingMap() {
        throw new UnsupportedOperationException("descending not supported");
    }

    public NavigableSet<K> descendingKeySet() {
        throw new UnsupportedOperationException("descending not supported");
    }

    static final <E> List<E> toList(Collection<E> c) {
        // Using size() here would be a pessimization.
        List<E> list = new ArrayList<E>();
        for (E e : c){
            list.add(e);
        }
        return list;
    }



    static final class KeySet<E> extends AbstractSet<E> implements NavigableSet<E> {
        private final ConcurrentNavigableMap<E,Object> m;
        private final boolean hasValues;
        KeySet(ConcurrentNavigableMap<E,Object> map, boolean hasValues) {
            m = map;
            this.hasValues = hasValues;
        }
        public int size() { return m.size(); }
        public boolean isEmpty() { return m.isEmpty(); }
        public boolean contains(Object o) { return m.containsKey(o); }
        public boolean remove(Object o) { return m.remove(o) != null; }
        public void clear() { m.clear(); }
        public E lower(E e) { return m.lowerKey(e); }
        public E floor(E e) { return m.floorKey(e); }
        public E ceiling(E e) { return m.ceilingKey(e); }
        public E higher(E e) { return m.higherKey(e); }
        public Comparator<? super E> comparator() { return m.comparator(); }
        public E first() { return m.firstKey(); }
        public E last() { return m.lastKey(); }
        public E pollFirst() {
            Map.Entry<E,Object> e = m.pollFirstEntry();
            return e == null? null : e.getKey();
        }
        public E pollLast() {
            Map.Entry<E,Object> e = m.pollLastEntry();
            return e == null? null : e.getKey();
        }
        public Iterator<E> iterator() {
            if (m instanceof BTreeMap)
                return ((BTreeMap<E,Object>)m).keyIterator();
            else
                return ((BTreeMap.SubMap<E,Object>)m).keyIterator();
        }
        public boolean equals(Object o) {
            if (o == this)
                return true;
            if (!(o instanceof Set))
                return false;
            Collection<?> c = (Collection<?>) o;
            try {
                return containsAll(c) && c.containsAll(this);
            } catch (ClassCastException unused)   {
                return false;
            } catch (NullPointerException unused) {
                return false;
            }
        }
        public Object[] toArray()     { return toList(this).toArray();  }
        public <T> T[] toArray(T[] a) { return toList(this).toArray(a); }
        public Iterator<E> descendingIterator() {
            return descendingSet().iterator();
        }
        public NavigableSet<E> subSet(E fromElement,
                                      boolean fromInclusive,
                                      E toElement,
                                      boolean toInclusive) {
            return new KeySet<E>(m.subMap(fromElement, fromInclusive,
                    toElement,   toInclusive),true);
        }
        public NavigableSet<E> headSet(E toElement, boolean inclusive) {
            return new KeySet<E>(m.headMap(toElement, inclusive),hasValues);
        }
        public NavigableSet<E> tailSet(E fromElement, boolean inclusive) {
            return new KeySet<E>(m.tailMap(fromElement, inclusive),hasValues);
        }
        public NavigableSet<E> subSet(E fromElement, E toElement) {
            return subSet(fromElement, true, toElement, false);
        }
        public NavigableSet<E> headSet(E toElement) {
            return headSet(toElement, false);
        }
        public NavigableSet<E> tailSet(E fromElement) {
            return tailSet(fromElement, true);
        }
        public NavigableSet<E> descendingSet() {
            return new KeySet(m.descendingMap(),hasValues);
        }

        @Override
        public boolean add(E k) {
            if(hasValues)
                throw new UnsupportedOperationException();
            else
                return m.put(k,  Utils.EMPTY_STRING) == null;
        }
    }

    static final class Values<E> extends AbstractCollection<E> {
        private final ConcurrentNavigableMap<Object, E> m;
        Values(ConcurrentNavigableMap<Object, E> map) {
            m = map;
        }
        public Iterator<E> iterator() {
            if (m instanceof BTreeMap)
                return ((BTreeMap<Object,E>)m).valueIterator();
            else
                return ((SubMap<Object,E>)m).valueIterator();
        }
        public boolean isEmpty() {
            return m.isEmpty();
        }
        public int size() {
            return m.size();
        }
        public boolean contains(Object o) {
            return m.containsValue(o);
        }
        public void clear() {
            m.clear();
        }
        public Object[] toArray()     { return toList(this).toArray();  }
        public <T> T[] toArray(T[] a) { return toList(this).toArray(a); }
    }

    static final class EntrySet<K1,V1> extends AbstractSet<Map.Entry<K1,V1>> {
        private final ConcurrentNavigableMap<K1, V1> m;
        EntrySet(ConcurrentNavigableMap<K1, V1> map) {
            m = map;
        }

        public Iterator<Map.Entry<K1,V1>> iterator() {
            if (m instanceof BTreeMap)
                return ((BTreeMap<K1,V1>)m).entryIterator();
            else
                return ((SubMap<K1,V1>)m).entryIterator();
        }

        public boolean contains(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<K1,V1> e = (Map.Entry<K1,V1>)o;
            V1 v = m.get(e.getKey());
            return v != null && v.equals(e.getValue());
        }
        public boolean remove(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<K1,V1> e = (Map.Entry<K1,V1>)o;
            return m.remove(e.getKey(),
                    e.getValue());
        }
        public boolean isEmpty() {
            return m.isEmpty();
        }
        public int size() {
            return m.size();
        }
        public void clear() {
            m.clear();
        }
        public boolean equals(Object o) {
            if (o == this)
                return true;
            if (!(o instanceof Set))
                return false;
            Collection<?> c = (Collection<?>) o;
            try {
                return containsAll(c) && c.containsAll(this);
            } catch (ClassCastException unused)   {
                return false;
            } catch (NullPointerException unused) {
                return false;
            }
        }
        public Object[] toArray()     { return toList(this).toArray();  }
        public <T> T[] toArray(T[] a) { return toList(this).toArray(a); }
    }



    static protected  class SubMap<K,V> extends AbstractMap<K,V> implements  ConcurrentNavigableMap<K,V> {

        protected final BTreeMap<K,V> m;

        protected final K lo;
        protected final boolean loInclusive;

        protected final K hi;
        protected final boolean hiInclusive;

        public SubMap(BTreeMap<K,V> m, K lo, boolean loInclusive, K hi, boolean hiInclusive) {
            this.m = m;
            this.lo = lo;
            this.loInclusive = loInclusive;
            this.hi = hi;
            this.hiInclusive = hiInclusive;
            if(CC.ASSERT && lo!=null && hi!=null && m.comparator.compare(lo, hi)>0)
                throw new IllegalArgumentException();
        }




/* ----------------  Map API methods -------------- */

        public boolean containsKey(Object key) {
            if (key == null) throw new NullPointerException();
            K k = (K)key;
            return inBounds(k) && m.containsKey(k);
        }

        public V get(Object key) {
            if (key == null) throw new NullPointerException();
            K k = (K)key;
            return ((!inBounds(k)) ? null : m.get(k));
        }

        public V put(K key, V value) {
            checkKeyBounds(key);
            return m.put(key, value);
        }

        public V remove(Object key) {
            K k = (K)key;
            return (!inBounds(k))? null : m.remove(k);
        }

        @Override
        public int size() {
            Iterator<K> i = keyIterator();
            int counter = 0;
            while(i.hasNext()){
                counter++;
                i.next();
            }
            return counter;
        }

        public boolean isEmpty() {
            return !keyIterator().hasNext();
        }

        public boolean containsValue(Object value) {
            if(value==null) return false;
            Iterator<V> i = valueIterator();
            while(i.hasNext()){
                if(value.equals(i.next()))
                    return true;
            }
            return false;
        }

        public void clear() {
            Iterator<K> i = keyIterator();
            while(i.hasNext()){
                i.next();
                i.remove();
            }
        }


        /* ----------------  ConcurrentMap API methods -------------- */

        public V putIfAbsent(K key, V value) {
            checkKeyBounds(key);
            return m.putIfAbsent(key, value);
        }

        public boolean remove(Object key, Object value) {
            K k = (K)key;
            return inBounds(k) && m.remove(k, value);
        }

        public boolean replace(K key, V oldValue, V newValue) {
            checkKeyBounds(key);
            return m.replace(key, oldValue, newValue);
        }

        public V replace(K key, V value) {
            checkKeyBounds(key);
            return m.replace(key, value);
        }

        /* ----------------  SortedMap API methods -------------- */

        public Comparator<? super K> comparator() {
            return m.comparator();
        }

        /* ----------------  Relational methods -------------- */

        public Map.Entry<K,V> lowerEntry(K key) {
            Entry<K,V> r = m.lowerEntry(key);
            return r!=null && inBounds(r.getKey()) ? r : null;
        }

        public K lowerKey(K key) {
            Entry<K,V> n = lowerEntry(key);
            return (n == null)? null : n.getKey();
        }

        public Map.Entry<K,V> floorEntry(K key) {
            Entry<K,V> r = m.floorEntry(key);
            return r!=null && inBounds(r.getKey()) ? r : null;
        }

        public K floorKey(K key) {
            Entry<K,V> n = floorEntry(key);
            return (n == null)? null : n.getKey();
        }

        public Map.Entry<K,V> ceilingEntry(K key) {
            Entry<K,V> r = m.ceilingEntry(key);
            return r!=null && inBounds(r.getKey()) ? r : null;
        }

        @Override
        public K ceilingKey(K key) {
            Entry<K,V> k = ceilingEntry(key);
            return k!=null? k.getKey():null;
        }

        @Override
        public Entry<K, V> higherEntry(K key) {
            Entry<K,V> r = m.higherEntry(key);
            return r!=null && inBounds(r.getKey()) ? r : null;
        }

        @Override
        public K higherKey(K key) {
            Entry<K,V> k = higherEntry(key);
            return k!=null? k.getKey():null;
        }


        public K firstKey() {
            Entry<K,V> k = firstEntry();
            return k!=null? k.getKey() : null;
        }

        public K lastKey() {
            Entry<K,V> k = lastEntry();
            return k!=null? k.getKey() : null;
        }


        public Map.Entry<K,V> firstEntry() {
            Entry<K,V> k =
                    lo==null ?
                    m.firstEntry():
                    m.findLarger(lo, loInclusive);
            return k!=null && inBounds(k.getKey())? k : null;

        }

        public Map.Entry<K,V> lastEntry() {
            Entry<K,V> k =
                    hi==null ?
                    m.lastEntry():
                    m.findSmaller(hi, hiInclusive);
            return k!=null && inBounds(k.getKey())? k : null;
        }

        @Override
        public Entry<K, V> pollFirstEntry() {
            while(true){
                Entry<K, V> e = firstEntry();
                if(e==null || remove(e.getKey(),e.getValue())){
                    return e;
                }
            }
        }

        @Override
        public Entry<K, V> pollLastEntry() {
            while(true){
                Entry<K, V> e = lastEntry();
                if(e==null || remove(e.getKey(),e.getValue())){
                    return e;
                }
            }
        }




        /**
         * Utility to create submaps, where given bounds override
         * unbounded(null) ones and/or are checked against bounded ones.
         */
        private SubMap<K,V> newSubMap(K fromKey,
                                      boolean fromInclusive,
                                      K toKey,
                                      boolean toInclusive) {
            if (lo != null) {
                if (fromKey == null) {
                    fromKey = lo;
                    fromInclusive = loInclusive;
                }
                else {
                    int c = m.comparator.compare(fromKey, lo);
                    if (c < 0 || (c == 0 && !loInclusive && fromInclusive))
                        throw new IllegalArgumentException("key out of range");
                }
            }
            if (hi != null) {
                if (toKey == null) {
                    toKey = hi;
                    toInclusive = hiInclusive;
                }
                else {
                    int c = m.comparator.compare(toKey, hi);
                    if (c > 0 || (c == 0 && !hiInclusive && toInclusive))
                        throw new IllegalArgumentException("key out of range");
                }
            }
            return new SubMap<K,V>(m, fromKey, fromInclusive,
                    toKey, toInclusive);
        }

        public SubMap<K,V> subMap(K fromKey,
                                  boolean fromInclusive,
                                  K toKey,
                                  boolean toInclusive) {
            if (fromKey == null || toKey == null)
                throw new NullPointerException();
            return newSubMap(fromKey, fromInclusive, toKey, toInclusive);
        }

        public SubMap<K,V> headMap(K toKey,
                                   boolean inclusive) {
            if (toKey == null)
                throw new NullPointerException();
            return newSubMap(null, false, toKey, inclusive);
        }

        public SubMap<K,V> tailMap(K fromKey,
                                   boolean inclusive) {
            if (fromKey == null)
                throw new NullPointerException();
            return newSubMap(fromKey, inclusive, null, false);
        }

        public SubMap<K,V> subMap(K fromKey, K toKey) {
            return subMap(fromKey, true, toKey, false);
        }

        public SubMap<K,V> headMap(K toKey) {
            return headMap(toKey, false);
        }

        public SubMap<K,V> tailMap(K fromKey) {
            return tailMap(fromKey, true);
        }

        public SubMap<K,V> descendingMap() {
            throw new UnsupportedOperationException();
        }

        @Override
        public NavigableSet<K> navigableKeySet() {
            return new KeySet<K>((ConcurrentNavigableMap<K,Object>) this,m.hasValues);
        }


        /* ----------------  Utilities -------------- */



        private boolean tooLow(K key) {
            if (lo != null) {
                int c = m.comparator.compare(key, lo);
                if (c < 0 || (c == 0 && !loInclusive))
                    return true;
            }
            return false;
        }

        private boolean tooHigh(K key) {
            if (hi != null) {
                int c = m.comparator.compare(key, hi);
                if (c > 0 || (c == 0 && !hiInclusive))
                    return true;
            }
            return false;
        }

        private boolean inBounds(K key) {
            return !tooLow(key) && !tooHigh(key);
        }

        private void checkKeyBounds(K key) throws IllegalArgumentException {
            if (key == null)
                throw new NullPointerException();
            if (!inBounds(key))
                throw new IllegalArgumentException("key out of range");
        }





        @Override
        public NavigableSet<K> keySet() {
            return new KeySet<K>((ConcurrentNavigableMap<K,Object>) this, m.hasValues);
        }

        @Override
        public NavigableSet<K> descendingKeySet() {
            throw new UnsupportedOperationException("Descending not supported");
        }



        @Override
        public Set<Entry<K, V>> entrySet() {
            return new EntrySet<K, V>(this);
        }


        /*
         * ITERATORS
         */

        abstract class Iter<E> implements Iterator<E> {
            Entry<K,V> current = SubMap.this.firstEntry();
            Entry<K,V> last = null;


            public boolean hasNext() {
                return current!=null;
            }


            public void advance() {
                if(current==null) throw new NoSuchElementException();
                last = current;
                current = SubMap.this.higherEntry(current.getKey());
            }

            public void remove() {
                if(last==null) throw new IllegalStateException();
                SubMap.this.remove(last.getKey());
                last = null;
            }

        }
        Iterator<K> keyIterator() {
            return new Iter<K>() {
                @Override
                public K next() {
                    advance();
                    return last.getKey();
                }
            };
        }

        Iterator<V> valueIterator() {
            return new Iter<V>() {

                @Override
                public V next() {
                    advance();
                    return last.getValue();
                }
            };
        }

        Iterator<Map.Entry<K,V>> entryIterator() {
            return new Iter<Entry<K, V>>() {
                @Override
                public Entry<K, V> next() {
                    advance();
                    return last;
                }
            };
        }

    }


}
