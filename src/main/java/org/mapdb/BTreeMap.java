
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

    protected long treeRecid;
    private final BTreeRootSerializer btreeRootSerializer;

    static class BTreeRootSerializer implements  Serializer<BTreeRoot>{
        private final Serializer defaultSerializer;

        BTreeRootSerializer(Serializer defaultSerializer) {
            this.defaultSerializer = defaultSerializer;
        }

        @Override
        public void serialize(DataOutput out, BTreeRoot value) throws IOException {
            out.writeLong(value.rootRecid);
            out.writeBoolean(value.hasValues);
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
        int maxNodeSize;
        Serializer keySerializer;
        Serializer valueSerializer;
        Comparator comparator;



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
    /** Reference to record stored in database and lazily loaded on first request. */
    protected final static class LazyRef{
        protected final long recid;

        public LazyRef(long recid) {
            this.recid = recid;
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
                JdbmUtil.packLong(out,((LeafNode)value).next);
            }else{

                for(long child : ((DirNode)value).child)
                    JdbmUtil.packLong(out,child);
            }

            //write keys
            keySerializer.serialize(out, (K[]) value.keys());

            if(isLeaf && hasValues){
                DataOutput2 out2 = new DataOutput2();
                for(int i=0; i<value.vals().length; i++){
                    Object val = value.vals()[i];
                    out2.pos = 0;
                    valueSerializer.serialize(out2, (V) val);
                    if(out2.pos>CC.MAX_BTREE_INLINE_VALUE_SIZE){
                        //store value as separate node
                        long recid = engine.recordPut(out2.copyBytes(), Serializer.BYTE_ARRAY_SERIALIZER);
                        JdbmUtil.packInt(out,0);  //zero indicates reference
                        JdbmUtil.packLong(out,recid);
                    }else{
                        JdbmUtil.packInt(out,out2.pos+1); //zero is reserved for reference
                        out.write(out2.buf, 0, out2.pos);
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
                long next = JdbmUtil.unpackLong(in);
                Object[] keys = (Object[]) keySerializer.deserialize(in, size);
                if(keys.length!=size) throw new InternalError();
                Object[] vals  = null;
                if(hasValues){
                    vals = new Object[size];
                    for(int i=0;i<size;i++){
                        int valueSize = JdbmUtil.unpackInt(in);
                        if(valueSize ==0){
                            //zero is reference
                            vals[i] = new LazyRef(JdbmUtil.unpackLong(in));
                        }else{
                            vals[i] = valueSerializer.deserialize(in, size-1);
                        }
                    }
                }
                return new LeafNode(keys, vals, next);
            }else{
                long[] child = new long[size];
                for(int i=0;i<size;i++)
                    child[i] = JdbmUtil.unpackLong(in);
                Object[] keys = (Object[]) keySerializer.deserialize(in, size);
                if(keys.length!=size) throw new InternalError();
                return new DirNode(keys, child);
            }
        }
    };


    /** constructor used to create new tree*/
    public BTreeMap(Engine engine, int maxNodeSize, boolean hasValues, Serializer defaultSerializer,
                    Serializer<K[]> keySerializer, Serializer<V> valueSerializer, Comparator<K> comparator) {
        if(maxNodeSize%2!=0) throw new IllegalArgumentException("maxNodeSize must be dividable by 2");
        if(maxNodeSize<6) throw new IllegalArgumentException("maxNodeSize too low");
        if(maxNodeSize>126) throw new IllegalArgumentException("maxNodeSize too high");
        if(defaultSerializer==null) defaultSerializer = Serializer.BASIC_SERIALIZER;
        this.btreeRootSerializer = new BTreeRootSerializer(defaultSerializer);
        this.hasValues = hasValues;
        this.engine = engine;
        this.maxNodeSize = maxNodeSize;
        this.comparator = comparator==null? JdbmUtil.COMPARABLE_COMPARATOR : comparator;
        this.keySerializer = keySerializer==null ?  defaultSerializer :  keySerializer;
        this.valueSerializer = valueSerializer==null ? (Serializer<V>) defaultSerializer : valueSerializer;

        LeafNode emptyRoot = new LeafNode(new Object[]{null, null}, new Object[]{null, null}, 0);
        this.rootRecid = engine.recordPut(emptyRoot, nodeSerializer);

        saveTreeInfo();
    }

    protected void saveTreeInfo() {
        BTreeRoot r = new BTreeRoot();
        r.hasValues = hasValues;
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
    }


    protected void unlockNode(final long nodeRecid) {
        if(CC.BTREEMAP_LOG_NODE_LOCKS)
            JdbmUtil.LOG.finest("BTreeMap UNLOCK R:"+nodeRecid+" T:"+Thread.currentThread().getId());

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
            JdbmUtil.LOG.finest("BTreeMap TRYLOCK R:"+nodeRecid+" T:"+Thread.currentThread().getId());

        //feel free to rewrite, if you know better (more efficient) way
        if(CC.ASSERT && nodeWriteLocks.get(nodeRecid)==Thread.currentThread()){
            //check node is not already locked by this thread
            throw new InternalError("node already locked by current thread: "+nodeRecid);
        }


        while(nodeWriteLocks.putIfAbsent(nodeRecid, Thread.currentThread()) != null){
            Thread.yield();
        }
        if(CC.BTREEMAP_LOG_NODE_LOCKS)
            JdbmUtil.LOG.finest("BTreeMap LOCK R:"+nodeRecid+" T:"+Thread.currentThread().getId());

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
            Object ret = (hasValues? leaf.vals[pos] : JdbmUtil.EMPTY_STRING);
            if(ret instanceof  LazyRef)
                ret = engine.recordGet(((LazyRef)ret).recid, valueSerializer);
            return (V)ret;
        }else
            return null;
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

    protected V put2(K v, final V value, final boolean putOnlyIfAbsent){
        if(v == null) throw new IllegalArgumentException("null key");
        if(value == null) throw new IllegalArgumentException("null value");

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

                    Object oldVal =  (hasValues? A.vals()[pos] : JdbmUtil.EMPTY_STRING);
                    if(putOnlyIfAbsent){
                        //is not absent, so quit
                        unlockNode(current);
                        if(oldVal instanceof  LazyRef){
                            oldVal = engine.recordGet(((LazyRef)oldVal).recid,valueSerializer);
                        }
                        assertNoLocks();
                        return (V) oldVal;
                    }
                    //insert new
                    Object[] vals = null;
                    if(hasValues){
                        vals = Arrays.copyOf(A.vals(), A.vals().length);
                        vals[pos] = value;
                    }

                    A = new LeafNode(Arrays.copyOf(A.keys(), A.keys().length), vals, ((LeafNode)A).next);
                    engine.recordUpdate(current, A, nodeSerializer);
                    //delete old lazy ref if necessary
                    if(oldVal instanceof  LazyRef){
                        long recid = ((LazyRef)oldVal).recid;
                        oldVal = engine.recordGet(recid,valueSerializer);
                        engine.recordDelete(recid);
                    }
                    //already in here
                    unlockNode(current);
                    assertNoLocks();
                    return (V) oldVal;
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
                Object[] keys = JdbmUtil.arrayPut(A.keys(), pos, v);

                if(A.isLeaf()){
                    Object[] vals = hasValues? JdbmUtil.arrayPut(A.vals(), pos, value): null;
                    LeafNode n = new LeafNode(keys, vals, ((LeafNode)A).next);
                    engine.recordUpdate(current, n, nodeSerializer);
                }else{
                    if(CC.ASSERT && p==0)
                        throw new InternalError();
                    long[] child = JdbmUtil.arrayLongPut(A.child(), pos, p);
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
                final Object[] keys = JdbmUtil.arrayPut(A.keys(), pos, v);
                final Object[] vals = (A.isLeaf() && hasValues)? JdbmUtil.arrayPut(A.vals(), pos, value) : null;
                final long[] child = A.isLeaf()? null : JdbmUtil.arrayLongPut(A.child(), pos, p);
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
                Object oldVal = hasValues? A.vals()[pos] : JdbmUtil.EMPTY_STRING;
                Object lazyOldVal = !(oldVal instanceof LazyRef)? null :
                        engine.recordGet(((LazyRef)oldVal).recid, valueSerializer);

                if(value!=null && !value.equals(lazyOldVal!=null? lazyOldVal: oldVal))
                    return null;

                Object[] keys2 = new Object[A.keys().length-1];
                System.arraycopy(A.keys(),0,keys2, 0, pos);
                System.arraycopy(A.keys(), pos+1, keys2, pos, keys2.length-pos);

                Object[] vals2 = null;
                if(hasValues){
                    vals2 = new Object[A.vals().length-1];
                    System.arraycopy(A.vals(),0,vals2, 0, pos);
                    System.arraycopy(A.vals(), pos+1, vals2, pos, vals2.length-pos);
                    if(lazyOldVal!=null){
                        engine.recordDelete(((LazyRef)oldVal).recid);
                        oldVal = lazyOldVal;
                    }
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
        Iterator iter = keySet().iterator();
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
            Object ret =  currentLeaf.vals[currentPos];
            if(ret instanceof LazyRef )
                ret = engine.recordGet(((LazyRef)ret).recid, valueSerializer);
            moveToNext();
            return (V) ret;
        }

    }

    class BTreeEntryIterator extends BTreeIterator implements  Iterator<Entry<K, V>>{

        @Override
        public Entry<K, V> next() {
            if(currentLeaf == null) throw new NoSuchElementException();
            K ret = (K) currentLeaf.keys[currentPos];
            Object val = currentLeaf.vals[currentPos];
            moveToNext();
            return makeEntry(ret, val);

        }
    }





    final private NavigableSet<K> keySet = new AbstractSet2<K>(){

        @Override
        public boolean isEmpty() {
            return BTreeMap.this.isEmpty();
        }

        @Override
        public int size() {
            return BTreeMap.this.size();
        }

        @Override
        public boolean contains(Object o) {
            return BTreeMap.this.containsKey(o);
        }

        @Override
        public K lower(K k) {
            return notYetImplemented();
        }

        @Override
        public K floor(K k) {
            return notYetImplemented();
        }

        @Override
        public K ceiling(K k) {
            return notYetImplemented();
        }

        @Override
        public K higher(K k) {
            return notYetImplemented();
        }

        @Override
        public K pollFirst() {
            return notYetImplemented();
        }

        @Override
        public K pollLast() {
            return notYetImplemented();
        }

        @Override
        public Iterator<K> iterator() {
            return new BTreeKeyIterator();
        }

        @Override
        public NavigableSet<K> descendingSet() {
            throw new UnsupportedOperationException("Descending iteration not supported.");
        }

        @Override
        public Iterator<K> descendingIterator() {
            throw new UnsupportedOperationException("Descending iteration not supported.");
        }

        @Override
        public NavigableSet<K> subSet(K fromElement, boolean fromInclusive, K toElement, boolean toInclusive) {
            return notYetImplemented();
        }

        @Override
        public NavigableSet<K> headSet(K toElement, boolean inclusive) {
            return subSet(null, true, toElement, inclusive);
        }

        @Override
        public NavigableSet<K> tailSet(K fromElement, boolean inclusive) {
            return subSet(fromElement, inclusive, null, false);
        }

        @Override
        public boolean add(K k) {
            if(BTreeMap.this.hasValues)
                throw new UnsupportedOperationException();
            else
                return BTreeMap.this.put(k, (V) JdbmUtil.EMPTY_STRING) == null;
        }

        @Override
        public boolean remove(Object o) {
            return BTreeMap.this.remove(o)!=null;
        }

        @Override
        public void clear() {
            BTreeMap.this.clear();
        }

        @Override
        public Comparator<? super K> comparator() {
            return BTreeMap.this.comparator();
        }

        @Override
        public SortedSet<K> subSet(K fromElement, K toElement) {
            return subMap(fromElement, toElement).keySet();
        }

        @Override
        public SortedSet<K> headSet(K toElement) {
            return headMap(toElement).keySet();
        }

        @Override
        public SortedSet<K> tailSet(K fromElement) {
            return tailMap(fromElement).keySet();
        }

        @Override
        public K first() {
            return firstKey();
        }

        @Override
        public K last() {
            return lastKey();
        }
    };

    @Override
    public NavigableSet<K> keySet() {
        return keySet;
    }

    protected Entry<K, V> makeEntry(Object key, Object value) {
        if(value instanceof  LazyRef){
            value = engine.recordGet(((LazyRef)value).recid, valueSerializer);
        }
        return new SimpleImmutableEntry<K, V>((K)key,  (V)value);
    }

    @Override
    public NavigableSet<K> descendingKeySet() {
        throw new UnsupportedOperationException("Descending iteration not supported.");
    }

    final private Collection<V> values = new AbstractCollection<V>() {
        @Override
        public int size() {
            return BTreeMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return BTreeMap.this.isEmpty();
        }


        @Override
        public Iterator<V> iterator() {
            return new BTreeValueIterator();
        }


        @Override
        public boolean add(V v) {
            return false;
        }



        @Override
        public void clear() {
            BTreeMap.this.clear();
        }
    };

    @Override
    public Collection<V> values() {
        return values;
    }

    private final SortedSet<Entry<K, V>> entrySet = new AbstractSet3<Entry<K, V>>(){

        @Override
        public int size() {
            return BTreeMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return BTreeMap.this.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            if(o instanceof  Entry){
                Entry e = (Entry) o;
                Object val = BTreeMap.this.get(e.getKey());
                return val!=null && val.equals(e.getValue());
            }else
                return false;
        }

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return new BTreeEntryIterator();
        }
        @Override
        public boolean add(Entry<K, V> kvEntry) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            if(o instanceof Entry){
                Entry e = (Entry) o;
                Object key = e.getKey();
                return key != null && BTreeMap.this.remove(key, e.getValue());
            }
            return false;
        }

        @Override
        public void clear() {
            BTreeMap.this.clear();
        }

        @Override
        public Comparator<? super Entry<K, V>> comparator() {
            return new Comparator<Entry<K,V>>() {
                @Override
                public int compare(Entry<K, V> o1, Entry<K, V> o2) {
                    return BTreeMap.this.comparator().compare(o1.getKey(), o2.getKey());
                }
            };
        }

        @Override
        public SortedSet<Entry<K, V>> subSet(Entry<K, V> fromElement, Entry<K, V> toElement) {
            return (SortedSet<Entry<K, V>>) subMap(fromElement.getKey(), toElement.getKey()).entrySet();
        }

        @Override
        public SortedSet<Entry<K, V>> headSet(Entry<K, V> toElement) {
            return (SortedSet<Entry<K, V>>) headMap(toElement.getKey()).entrySet();
        }

        @Override
        public SortedSet<Entry<K, V>> tailSet(Entry<K, V> fromElement) {
            return (SortedSet<Entry<K, V>>) tailMap(fromElement.getKey()).entrySet();
        }

        @Override
        public Entry<K, V> first() {
            return firstEntry();
        }

        @Override
        public Entry<K, V> last() {
            return lastEntry();
        }
    };

    @Override
    public SortedSet<Entry<K, V>> entrySet() {
        return entrySet;
    }

    @Override
    public boolean isEmpty() {
        return !keySet.iterator().hasNext();
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
        if(key == null || value == null) throw new NullPointerException();
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
            Object val2 = val instanceof LazyRef?
                    engine.recordGet(((LazyRef)val).recid, valueSerializer):
                    val;
            if(oldValue.equals(val2)){
                Object[] vals = Arrays.copyOf(leaf.vals, leaf.vals.length);
                vals[pos] = newValue;
                leaf = new LeafNode(Arrays.copyOf(leaf.keys, leaf.keys.length), vals, leaf.next);
                //delete old node if lazyref
                if(val instanceof LazyRef){
                    engine.recordDelete(((LazyRef) val).recid);
                }

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
            if(oldVal instanceof LazyRef){
                //delete old val
                long recid = ((LazyRef)oldVal).recid;
                oldVal = engine.recordGet(recid,valueSerializer);
                engine.recordDelete(recid);
            }
            vals[pos] = value;
            leaf = new LeafNode(Arrays.copyOf(leaf.keys, leaf.keys.length), vals, leaf.next);
            engine.recordUpdate(current, leaf, nodeSerializer);

            ret =  oldVal;
        }
        unlockNode(current);
        return (V)ret;
    }


    @Override
    public Comparator<? super K> comparator() {
        return comparator;
    }

    @Override
    public ConcurrentNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        return notYetImplemented();
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
        return makeEntry(l.keys[1], l.vals[1]);
    }


    @Override
    public Entry<K, V> pollFirstEntry() {
        return notYetImplemented();
    }

    @Override
    public Entry<K, V> pollLastEntry() {
        return notYetImplemented();
    }


    protected Entry<K,V> findSmaller(K key,boolean inclusive){
        BNode n = engine.recordGet(rootRecid, nodeSerializer);
        return findSmallerRecur(n, key, inclusive);
    }

    private Entry<K, V> findSmallerRecur(BNode n, K key, boolean inclusive) {
        return notYetImplemented();
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
                if(k!=null) return makeEntry(k, n.vals()[i]);
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


    @Override
    public ConcurrentNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
        return subMap(null, true, toKey,  inclusive);
    }

    @Override
    public ConcurrentNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
        return subMap(fromKey, inclusive, null, false);
    }

    @Override
    public ConcurrentNavigableMap<K, V> subMap(K fromKey, K toKey) {
        return subMap(fromKey, true, toKey, false);
    }

    @Override
    public ConcurrentNavigableMap<K, V> headMap(K toKey) {
        return subMap(null, true, toKey, false);
    }

    @Override
    public ConcurrentNavigableMap<K, V> tailMap(K fromKey) {
        return subMap(fromKey, true, null, false);
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
        return notYetImplemented();
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
    public ConcurrentNavigableMap<K, V> descendingMap() {
        return notYetImplemented();
    }

    @Override
    public NavigableSet<K> navigableKeySet() {
        return notYetImplemented();
    }

    protected <E> E notYetImplemented() {
        throw new InternalError("not yet implemented");
    }


    @Override
    public boolean containsKey(Object key) {
        return get(key)!=null;
    }

    @Override
    public boolean containsValue(Object value){
        return values().contains(value);
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



    private abstract class AbstractSet2<E> extends AbstractSet<E> implements NavigableSet<E> {
    }

    private abstract class AbstractSet3<E> extends AbstractSet<E> implements SortedSet<E> {
    }


}
