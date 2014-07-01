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

/*
 * NOTE: some code (and javadoc) used in this class
 * comes from JSR-166 group with following copyright:
 *
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

package org.mapdb;


import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.LockSupport;


/**
 * A scalable concurrent {@link ConcurrentNavigableMap} implementation.
 * The map is sorted according to the {@linkplain Comparable natural
 * ordering} of its keys, or by a {@link Comparator} provided at map
 * creation time.
 *
 * Insertion, removal,
 * update, and access operations safely execute concurrently by
 * multiple threads.  Iterators are <i>weakly consistent</i>, returning
 * elements reflecting the state of the map at some point at or since
 * the creation of the iterator.  They do <em>not</em> throw {@link
 * ConcurrentModificationException}, and may proceed concurrently with
 * other operations. Ascending key ordered views and their iterators
 * are faster than descending ones.
 * 
 * It is possible to obtain <i>consistent</i> iterator by using <code>snapshot()</code>
 * method.
 *
 * All <tt>Map.Entry</tt> pairs returned by methods in this class
 * and its views represent snapshots of mappings at the time they were
 * produced. They do <em>not</em> support the <tt>Entry.setValue</tt>
 * method. (Note however that it is possible to change mappings in the
 * associated map using <tt>put</tt>, <tt>putIfAbsent</tt>, or
 * <tt>replace</tt>, depending on exactly which effect you need.)
 *
 * This collection has optional size counter. If this is enabled Map size is
 * kept in {@link Atomic.Long} variable. Keeping counter brings considerable
 * overhead on inserts and removals.
 * If the size counter is not enabled the <tt>size</tt> method is <em>not</em> a constant-time operation.
 * Determining the current number of elements requires a traversal of the elements.
 *
 * Additionally, the bulk operations <tt>putAll</tt>, <tt>equals</tt>, and
 * <tt>clear</tt> are <em>not</em> guaranteed to be performed
 * atomically. For example, an iterator operating concurrently with a
 * <tt>putAll</tt> operation might view only some of the added
 * elements. NOTE: there is an optional 
 *
 * This class and its views and iterators implement all of the
 * <em>optional</em> methods of the {@link Map} and {@link Iterator}
 * interfaces. Like most other concurrent collections, this class does
 * <em>not</em> permit the use of <tt>null</tt> keys or values because some
 * null return values cannot be reliably distinguished from the absence of
 * elements.
 *
 * Theoretical design of BTreeMap is based on <a href="http://www.cs.cornell.edu/courses/cs4411/2009sp/blink.pdf">paper</a>
 * from Philip L. Lehman and S. Bing Yao. More practical aspects of BTreeMap implementation are based on <a href="http://www.doc.ic.ac.uk/~td202/">notes</a>
 * and <a href="http://www.doc.ic.ac.uk/~td202/btree/">demo application</a> from Thomas Dinsdale-Young.
 * B-Linked-Tree used here does not require locking for read. Updates and inserts locks only one, two or three nodes.

 * This B-Linked-Tree structure does not support removal well, entry deletion does not collapse tree nodes. Massive
 * deletion causes empty nodes and performance lost. There is workaround in form of compaction process, but it is not
 * implemented yet.
 *
 * @author Jan Kotek
 * @author some parts by Doug Lea and JSR-166 group
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class BTreeMap<K,V> extends AbstractMap<K,V>
        implements ConcurrentNavigableMap<K,V>, Bind.MapWithModificationListener<K,V>, Closeable {

    @SuppressWarnings("rawtypes")
    public static final Comparator COMPARABLE_COMPARATOR = new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
            return o1.compareTo(o2);
        }
    };


    protected static final Object EMPTY = new Object();

    protected static final int B_TREE_NODE_LEAF_LR = 180;
    protected static final int B_TREE_NODE_LEAF_L = 181;
    protected static final int B_TREE_NODE_LEAF_R = 182;
    protected static final int B_TREE_NODE_LEAF_C = 183;
    protected static final int B_TREE_NODE_DIR_LR = 184;
    protected static final int B_TREE_NODE_DIR_L = 185;
    protected static final int B_TREE_NODE_DIR_R = 186;
    protected static final int B_TREE_NODE_DIR_C = 187;



    /** recid under which reference to rootRecid is stored */
    protected final long rootRecidRef;

    /** Serializer used to convert keys from/into binary form. */
    protected final BTreeKeySerializer keySerializer;

    /** Serializer used to convert keys from/into binary form*/
    protected final Serializer<V> valueSerializer;

    /** keys are sorted by this*/
    protected final Comparator comparator;

    /** holds node level locks*/
    protected final LongConcurrentHashMap<Thread> nodeLocks = new LongConcurrentHashMap<Thread>();

    /** maximal node size allowed in this BTree*/
    protected final int maxNodeSize;

    /** DB Engine in which entries are persisted */
    protected final Engine engine;

    /** is this a Map or Set?  if false, entries do not have values, only keys are allowed*/
    protected final boolean hasValues;

    /** store values as part of BTree nodes */
    protected final boolean valsOutsideNodes;

    protected final List<Long> leftEdges;


    private final KeySet keySet;

    private final EntrySet entrySet = new EntrySet(this);

    private final Values values = new Values(this);

    private final ConcurrentNavigableMap<K,V> descendingMap = new DescendingMap(this, null,true, null, false);

    protected final Atomic.Long counter;

    protected final int numberOfNodeMetas;

    /** hack used for DB Catalog*/
    protected static SortedMap<String, Object> preinitCatalog(DB db) {

        Long rootRef = db.getEngine().get(Engine.CATALOG_RECID, Serializer.LONG);

        if(rootRef==null){
            if(db.getEngine().isReadOnly())
                return Collections.unmodifiableSortedMap(new TreeMap<String, Object>());

            NodeSerializer rootSerializer = new NodeSerializer(false,BTreeKeySerializer.STRING,
                    db.getDefaultSerializer(),COMPARABLE_COMPARATOR, 0);
            BNode root = new LeafNode(new Object[]{null, null}, new Object[]{}, 0);
            rootRef = db.getEngine().put(root, rootSerializer);
            db.getEngine().update(Engine.CATALOG_RECID,rootRef, Serializer.LONG);
            db.getEngine().commit();
        }
        return new BTreeMap<String, Object>(db.engine,Engine.CATALOG_RECID,32,false,0,
                BTreeKeySerializer.STRING,
                db.getDefaultSerializer(),
                COMPARABLE_COMPARATOR,0, false);
    }



    /** if <code>valsOutsideNodes</code> is true, this class is used instead of values.
     * It contains reference to actual value. It also supports assertions from preventing it to leak outside of Map*/
    protected static final class ValRef{
        /** reference to actual value */
        final long recid;
        public ValRef(long recid) {
            this.recid = recid;
        }

        @Override
        public boolean equals(Object obj) {
            throw new IllegalAccessError();
        }

        @Override
        public int hashCode() {
            throw new IllegalAccessError();
        }

        @Override
        public String toString() {
            return "BTreeMap-ValRer["+recid+"]";
        }
    }


    /** common interface for BTree node */
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

        DirNode(Object[] keys, List<Long> child) {
            this.keys = keys;
            this.child = new long[child.size()];
            for(int i=0;i<child.size();i++){
                this.child[i] = child.get(i);
            }
        }


        @Override public boolean isLeaf() { return false;}

        @Override public Object[] keys() { return keys;}
        @Override public Object[] vals() { return null;}

        @Override public Object highKey() {return keys[keys.length-1];}

        @Override public long[] child() { return child;}

        @Override public long next() {return child[child.length-1];}

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
            assert(vals==null||keys.length == vals.length+2);
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


    protected final Serializer<BNode> nodeSerializer;

    protected static class NodeSerializer<A,B> implements  Serializer<BNode>{

        protected final boolean hasValues;
        protected final boolean valsOutsideNodes;
        protected final BTreeKeySerializer keySerializer;
        protected final Serializer<Object> valueSerializer;
        protected final Comparator comparator;
        protected final int numberOfNodeMetas;

        public NodeSerializer(boolean valsOutsideNodes, BTreeKeySerializer keySerializer, Serializer valueSerializer, Comparator comparator, int numberOfNodeMetas) {
            assert(keySerializer!=null);
            assert(comparator!=null);
            this.hasValues = valueSerializer!=null;
            this.valsOutsideNodes = valsOutsideNodes;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            this.comparator = comparator;
            this.numberOfNodeMetas = numberOfNodeMetas;
        }

        @Override
        public void serialize(DataOutput out, BNode value) throws IOException {
            final boolean isLeaf = value.isLeaf();

            //first byte encodes if is leaf (first bite) and length (last seven bites)
            assert(value.keys().length<=255);
            assert(!(!isLeaf && value.child().length!= value.keys().length));
            assert(!(isLeaf && hasValues && value.vals().length!= value.keys().length-2));
            assert(!(!isLeaf && value.highKey()!=null && value.child()[value.child().length-1]==0));

            //check node integrity in paranoid mode
            if(CC.PARANOID){
                int len = value.keys().length;
                for(int i=value.keys()[0]==null?2:1;
                  i<(value.keys()[len-1]==null?len-1:len);
                  i++){
                    int comp = comparator.compare(value.keys()[i-1], value.keys()[i]);
                    int limit = i==len-1 ? 1:0 ;
                    if(comp>=limit){
                        throw new AssertionError("BTreeNode format error, wrong key order at #"+i+"\n"+value);
                    }
                }

            }


            final boolean left = value.keys()[0] == null;
            final boolean right = value.keys()[value.keys().length-1] == null;


            final int header;

            if(isLeaf){
                if(right){
                    if(left)
                        header = B_TREE_NODE_LEAF_LR;
                    else
                        header = B_TREE_NODE_LEAF_R;
                }else{
                    if(left)
                        header = B_TREE_NODE_LEAF_L;
                    else
                        header = B_TREE_NODE_LEAF_C;
                }
            }else{
                if(right){
                    if(left)
                        header = B_TREE_NODE_DIR_LR;
                    else
                        header = B_TREE_NODE_DIR_R;
                }else{
                    if(left)
                        header = B_TREE_NODE_DIR_L;
                    else
                        header = B_TREE_NODE_DIR_C;
                }
            }



            out.write(header);
            out.write(value.keys().length);

            //write node metas, right now this is ignored, but in future it could be used for counted btrees or aggregations
            for(int i=0;i<numberOfNodeMetas;i++){
                DataOutput2.packLong(out,0);
            }

            //longs go first, so it is possible to reconstruct tree without serializer
            if(isLeaf){
                DataOutput2.packLong(out, ((LeafNode) value).next);
            }else{
                for(long child : ((DirNode)value).child)
                    DataOutput2.packLong(out, child);
            }


            keySerializer.serialize(out,left?1:0,
                    right?value.keys().length-1:value.keys().length,
                    value.keys());

            if(isLeaf){
                if(hasValues){
                    for(Object val:value.vals()){
                        assert(val!=null);
                        if(valsOutsideNodes){
                            long recid = ((ValRef)val).recid;
                            DataOutput2.packLong(out, recid);
                        }else{
                            valueSerializer.serialize(out,  val);
                        }
                    }
                }else{
                    //write bits if values are null
                    boolean[] bools = new boolean[value.vals().length];
                    for(int i=0;i<bools.length;i++){
                        bools[i] = value.vals()[i]!=null;
                    }
                    //pack
                    byte[] bb = SerializerBase.booleanToByteArray(bools);
                    out.write(bb);
                }

            }
        }

        @Override
        public BNode deserialize(DataInput in, int available) throws IOException {
            final int header = in.readUnsignedByte();
            final int size = in.readUnsignedByte();

            //read node metas, right now this is ignored, but in future it could be used for counted btrees or aggregations
            for(int i=0;i<numberOfNodeMetas;i++){
                DataInput2.unpackLong(in);
            }


            //first bite indicates leaf
            final boolean isLeaf =
                    header == B_TREE_NODE_LEAF_C  || header == B_TREE_NODE_LEAF_L ||
                    header == B_TREE_NODE_LEAF_LR || header == B_TREE_NODE_LEAF_R;
            final int start =
                (header==B_TREE_NODE_LEAF_L  || header == B_TREE_NODE_LEAF_LR || header==B_TREE_NODE_DIR_L  || header == B_TREE_NODE_DIR_LR) ?
                1:0;

            final int end =
                (header==B_TREE_NODE_LEAF_R  || header == B_TREE_NODE_LEAF_LR || header==B_TREE_NODE_DIR_R  || header == B_TREE_NODE_DIR_LR) ?
                size-1:size;


            if(isLeaf){
                long next = DataInput2.unpackLong(in);
                Object[] keys = keySerializer.deserialize(in, start,end,size);
                assert(keys.length==size);
                Object[] vals = new Object[size-2];
                if(hasValues){
                    for(int i=0;i<size-2;i++){
                        if(valsOutsideNodes){
                            long recid = DataInput2.unpackLong(in);
                            vals[i] = recid==0? null: new ValRef(recid);
                        }else{
                            vals[i] = valueSerializer.deserialize(in, -1);
                        }
                    }
                }else{
                    //restore values which were deleted
                    boolean[] bools = SerializerBase.readBooleanArray(vals.length,in);
                    for(int i=0;i<bools.length;i++){
                        if(bools[i])
                            vals[i]=EMPTY;
                    }
                }
                return new LeafNode(keys, vals, next);
            }else{
                long[] child = new long[size];
                for(int i=0;i<size;i++)
                    child[i] = DataInput2.unpackLong(in);
                Object[] keys = keySerializer.deserialize(in, start,end,size);
                assert(keys.length==size);
                return new DirNode(keys, child);
            }
        }

        @Override
        public int fixedSize() {
            return -1;
        }

    }


    /** Constructor used to create new BTreeMap.
     *
     * @param engine used for persistence
     * @param rootRecidRef reference to root recid
     * @param maxNodeSize maximal BTree Node size. Node will split if number of entries is higher
     * @param valsOutsideNodes Store Values outside of BTree Nodes in separate record?
     * @param counterRecid recid under which `Atomic.Long` is stored, or `0` for no counter
     * @param keySerializer Serializer used for keys. May be null for default value.
     * @param valueSerializer Serializer used for values. May be null for default value
     * @param comparator Comparator to sort keys in this BTree, may be null.
     * @param numberOfNodeMetas number of meta records associated with each BTree node
     * @param disableLocks makes class thread-unsafe but bit faster
     */
    public BTreeMap(Engine engine, long rootRecidRef,int maxNodeSize, boolean valsOutsideNodes, long counterRecid,
                    BTreeKeySerializer<K> keySerializer, Serializer<V> valueSerializer, Comparator<K> comparator,
                    int numberOfNodeMetas, boolean disableLocks) {
        if(maxNodeSize%2!=0) throw new IllegalArgumentException("maxNodeSize must be dividable by 2");
        if(maxNodeSize<6) throw new IllegalArgumentException("maxNodeSize too low");
        if(maxNodeSize>126) throw new IllegalArgumentException("maxNodeSize too high");
        if(rootRecidRef<=0||counterRecid<0 || numberOfNodeMetas<0) throw new IllegalArgumentException();
        if(keySerializer==null) throw new NullPointerException();
        if(comparator==null) throw new NullPointerException();
        SerializerBase.assertSerializable(keySerializer);
        SerializerBase.assertSerializable(valueSerializer);
        SerializerBase.assertSerializable(comparator);



        this.rootRecidRef = rootRecidRef;
        this.hasValues = valueSerializer!=null;
        this.valsOutsideNodes = valsOutsideNodes;
        this.engine = engine;
        this.maxNodeSize = maxNodeSize;
        this.comparator = comparator;
        this.numberOfNodeMetas = numberOfNodeMetas;

        {
            Comparator requiredComparator = keySerializer.getComparator();
            if(requiredComparator!=null && !requiredComparator.equals(comparator))
                throw new IllegalArgumentException("KeySerializers requires its own comparator");
        }

        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;


        this.nodeSerializer = new NodeSerializer(valsOutsideNodes,keySerializer,valueSerializer,comparator,numberOfNodeMetas);

        this.keySet = new KeySet(this, hasValues);


        if(counterRecid!=0){
            this.counter = new Atomic.Long(engine,counterRecid);
            Bind.size(this,counter);
        }else{
            this.counter = null;
        }

        //load left edge refs
        ArrayList leftEdges2 = new ArrayList<Long>();
        long r = engine.get(rootRecidRef,Serializer.LONG);
        for(;;){
            BNode n= engine.get(r,nodeSerializer);
            leftEdges2.add(r);
            if(n.isLeaf()) break;
            r = n.child()[0];
        }
        Collections.reverse(leftEdges2);
        leftEdges = new CopyOnWriteArrayList<Long>(leftEdges2);
    }

    /** creates empty root node and returns recid of its reference*/
    static protected long createRootRef(Engine engine, BTreeKeySerializer keySer, Serializer valueSer, Comparator comparator, int numberOfNodeMetas){
        final LeafNode emptyRoot = new LeafNode(new Object[]{null, null}, new Object[]{}, 0);
        //empty root is serializer simpler way, so we can use dummy values
        long rootRecidVal = engine.put(emptyRoot,  new NodeSerializer(false,keySer, valueSer, comparator, numberOfNodeMetas));
        return engine.put(rootRecidVal,Serializer.LONG);
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

    @Override
	public V get(Object key){
    	return (V) get(key, true);
    }

    protected Object get(Object key, boolean expandValue) {
        if(key==null) throw new NullPointerException();
        K v = (K) key;
        long current = engine.get(rootRecidRef, Serializer.LONG); //get root

        BNode A = engine.get(current, nodeSerializer);

        //dive until  leaf
        while(!A.isLeaf()){
            current = nextDir((DirNode) A, v);
            A = engine.get(current, nodeSerializer);
        }

        //now at leaf level
        LeafNode leaf = (LeafNode) A;
        int pos = findChildren(v, leaf.keys);
        while(pos == leaf.keys.length){
            //follow next link on leaf until necessary
            leaf = (LeafNode) engine.get(leaf.next, nodeSerializer);
            pos = findChildren(v, leaf.keys);
        }

        if(pos==leaf.keys.length-1){
            return null; //last key is always deleted
        }
        //finish search
        if(leaf.keys[pos]!=null && 0==comparator.compare(v,leaf.keys[pos])){
            Object ret = leaf.vals[pos-1];
            return expandValue ? valExpand(ret) : ret;
        }else
            return null;
    }

    protected V valExpand(Object ret) {
        if(valsOutsideNodes && ret!=null) {
            long recid = ((ValRef)ret).recid;
            ret = engine.get(recid, valueSerializer);
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
        if(key==null||value==null) throw new NullPointerException();
        return put2(key,value, false);
    }

    protected V put2(final K key, final V value2, final boolean putOnlyIfAbsent){
        K v = key;
        if(v == null) throw new IllegalArgumentException("null key");
        if(value2 == null) throw new IllegalArgumentException("null value");

        V value = value2;
        if(valsOutsideNodes){
            long recid = engine.put(value2, valueSerializer);
            value = (V) new ValRef(recid);
        }

        int stackPos = -1;
        long[] stackVals = new long[4];

        final long rootRecid = engine.get(rootRecidRef, Serializer.LONG);
        long current = rootRecid;

        BNode A = engine.get(current, nodeSerializer);
        while(!A.isLeaf()){
            long t = current;
            current = nextDir((DirNode) A, v);
            assert(current>0) : A;
            if(current == A.child()[A.child().length-1]){
                //is link, do nothing
            }else{
                //stack push t
                stackPos++;
                if(stackVals.length == stackPos) //grow if needed
                    stackVals = Arrays.copyOf(stackVals, stackVals.length*2);
                stackVals[stackPos] = t;
            }
            A = engine.get(current, nodeSerializer);
        }
        int level = 1;

        long p=0;
        try{
        while(true){
            boolean found;
            do{
                lock(nodeLocks, current);
                found = true;
                A = engine.get(current, nodeSerializer);
                int pos = findChildren(v, A.keys());
                //check if keys is already in tree
                if(pos<A.keys().length-1 &&  v!=null && A.keys()[pos]!=null &&
                        0==comparator.compare(v,A.keys()[pos])){
                    //yes key is already in tree
                    Object oldVal = A.vals()[pos-1];
                    if(putOnlyIfAbsent){
                        //is not absent, so quit
                        unlock(nodeLocks, current);
                        if(CC.PARANOID) assertNoLocks(nodeLocks);
                        return valExpand(oldVal);
                    }
                    //insert new
                    Object[] vals = Arrays.copyOf(A.vals(), A.vals().length);
                    vals[pos-1] = value;

                    A = new LeafNode(Arrays.copyOf(A.keys(), A.keys().length), vals, ((LeafNode)A).next);
                    assert(nodeLocks.get(current)==Thread.currentThread());
                    engine.update(current, A, nodeSerializer);
                    //already in here
                    V ret =  valExpand(oldVal);
                    notify(key,ret, value2);
                    unlock(nodeLocks, current);
                    if(CC.PARANOID) assertNoLocks(nodeLocks);
                    return ret;
                }

                //if v > highvalue(a)
                if(A.highKey() != null && comparator.compare(v, A.highKey())>0){
                    //follow link until necessary
                    unlock(nodeLocks, current);
                    found = false;
                    int pos2 = findChildren(v, A.keys());
                    while(A!=null && pos2 == A.keys().length){
                        //TODO lock?
                        long next = A.next();

                        if(next==0) break;
                        current = next;
                        A = engine.get(current, nodeSerializer);
                        pos2 = findChildren(v, A.keys());
                    }

                }


            }while(!found);

            // can be new item inserted into A without splitting it?
            if(A.keys().length - (A.isLeaf()?2:1)<maxNodeSize){
                int pos = findChildren(v, A.keys());
                Object[] keys = arrayPut(A.keys(), pos, v);

                if(A.isLeaf()){
                    Object[] vals = arrayPut(A.vals(), pos-1, value);
                    LeafNode n = new LeafNode(keys, vals, ((LeafNode)A).next);
                    assert(nodeLocks.get(current)==Thread.currentThread());
                    engine.update(current, n, nodeSerializer);
                }else{
                    assert(p!=0);
                    long[] child = arrayLongPut(A.child(), pos, p);
                    DirNode d = new DirNode(keys, child);
                    assert(nodeLocks.get(current)==Thread.currentThread());
                    engine.update(current, d, nodeSerializer);
                }

                notify(key,  null, value2);
                unlock(nodeLocks, current);
                if(CC.PARANOID) assertNoLocks(nodeLocks);
                return null;
            }else{
                //node is not safe, it requires splitting
                final int pos = findChildren(v, A.keys());
                final Object[] keys = arrayPut(A.keys(), pos, v);
                final Object[] vals = (A.isLeaf())? arrayPut(A.vals(), pos-1, value) : null;
                final long[] child = A.isLeaf()? null : arrayLongPut(A.child(), pos, p);
                final int splitPos = keys.length/2;
                BNode B;
                if(A.isLeaf()){
                    Object[] vals2 = Arrays.copyOfRange(vals, splitPos, vals.length);

                    B = new LeafNode(
                                Arrays.copyOfRange(keys, splitPos, keys.length),
                                vals2,
                                ((LeafNode)A).next);
                }else{
                    B = new DirNode(Arrays.copyOfRange(keys, splitPos, keys.length),
                                Arrays.copyOfRange(child, splitPos, keys.length));
                }
                long q = engine.put(B, nodeSerializer);
                if(A.isLeaf()){  //  splitPos+1 is there so A gets new high  value (key)
                    Object[] keys2 = Arrays.copyOf(keys, splitPos+2);
                    keys2[keys2.length-1] = keys2[keys2.length-2];
                    Object[] vals2 = Arrays.copyOf(vals, splitPos);

                    //TODO check high/low keys overlap
                    A = new LeafNode(keys2, vals2, q);
                }else{
                    long[] child2 = Arrays.copyOf(child, splitPos+1);
                    child2[splitPos] = q;
                    A = new DirNode(Arrays.copyOf(keys, splitPos+1), child2);
                }
                assert(nodeLocks.get(current)==Thread.currentThread());
                engine.update(current, A, nodeSerializer);

                if((current != rootRecid)){ //is not root
                    unlock(nodeLocks, current);
                    p = q;
                    v = (K) A.highKey();
                    level = level+1;
                    if(stackPos!=-1){ //if stack is not empty
                        current = stackVals[stackPos--];
                    }else{
                        //current := the left most node at level
                        current = leftEdges.get(level-1);
                      }
                    assert(current>0);
                }else{
                    BNode R = new DirNode(
                            new Object[]{A.keys()[0], A.highKey(), B.isLeaf()?null:B.highKey()},
                            new long[]{current,q, 0});

                    lock(nodeLocks, rootRecidRef);
                    unlock(nodeLocks, current);
                    long newRootRecid = engine.put(R, nodeSerializer);

                    assert(nodeLocks.get(rootRecidRef)==Thread.currentThread());
                    engine.update(rootRecidRef, newRootRecid, Serializer.LONG);
                    //add newRootRecid into leftEdges
                    leftEdges.add(newRootRecid);

                    notify(key, null, value2);
                    unlock(nodeLocks, rootRecidRef);
                    if(CC.PARANOID) assertNoLocks(nodeLocks);
                    return null;
                }
            }
        }
        }catch(RuntimeException e){
            unlockAll(nodeLocks);
            throw e;
        }catch(Exception e){
            unlockAll(nodeLocks);
            throw new RuntimeException(e);
        }
    }


    protected static class BTreeIterator{
        final BTreeMap m;

        LeafNode currentLeaf;
        Object lastReturnedKey;
        int currentPos;
        final Object hi;
        final boolean hiInclusive;

        /** unbounded iterator*/
        BTreeIterator(BTreeMap m){
            this.m = m;
            hi=null;
            hiInclusive=false;
            pointToStart();
        }

        /** bounder iterator, args may be null for partially bounded*/
        BTreeIterator(BTreeMap m, Object lo, boolean loInclusive, Object hi, boolean hiInclusive){
            this.m = m;
            if(lo==null){
                pointToStart();
            }else{
                Fun.Tuple2<Integer, LeafNode> l = m.findLargerNode(lo, loInclusive);
                currentPos = l!=null? l.a : -1;
                currentLeaf = l!=null ? l.b : null;
            }

            this.hi = hi;
            this.hiInclusive = hiInclusive;
            if(hi!=null && currentLeaf!=null){
                //check in bounds
                Object key =  currentLeaf.keys[currentPos];
                int c = m.comparator.compare(key, hi);
                if (c > 0 || (c == 0 && !hiInclusive)){
                    //out of high bound
                    currentLeaf=null;
                    currentPos=-1;
                }
            }

        }


        private void pointToStart() {
            //find left-most leaf
            final long rootRecid = m.engine.get(m.rootRecidRef, Serializer.LONG);
            BNode node = (BNode) m.engine.get(rootRecid, m.nodeSerializer);
            while(!node.isLeaf()){
                node = (BNode) m.engine.get(node.child()[0], m.nodeSerializer);
            }
            currentLeaf = (LeafNode) node;
            currentPos = 1;

            while(currentLeaf.keys.length==2){
                //follow link until leaf is not empty
                if(currentLeaf.next == 0){
                    currentLeaf = null;
                    return;
                }
                currentLeaf = (LeafNode) m.engine.get(currentLeaf.next, m.nodeSerializer);
            }
        }


        public boolean hasNext(){
            return currentLeaf!=null;
        }

        public void remove(){
            if(lastReturnedKey==null) throw new IllegalStateException();
            m.remove(lastReturnedKey);
            lastReturnedKey = null;
        }

        protected void advance(){
            if(currentLeaf==null) return;
            lastReturnedKey =  currentLeaf.keys[currentPos];
            currentPos++;
            if(currentPos == currentLeaf.keys.length-1){
                //move to next leaf
                if(currentLeaf.next==0){
                    currentLeaf = null;
                    currentPos=-1;
                    return;
                }
                currentPos = 1;
                currentLeaf = (LeafNode) m.engine.get(currentLeaf.next, m.nodeSerializer);
                while(currentLeaf.keys.length==2){
                    if(currentLeaf.next ==0){
                        currentLeaf = null;
                        currentPos=-1;
                        return;
                    }
                    currentLeaf = (LeafNode) m.engine.get(currentLeaf.next, m.nodeSerializer);
                }
            }
            if(hi!=null && currentLeaf!=null){
                //check in bounds
                Object key =  currentLeaf.keys[currentPos];
                int c = m.comparator.compare(key, hi);
                if (c > 0 || (c == 0 && !hiInclusive)){
                    //out of high bound
                    currentLeaf=null;
                    currentPos=-1;
                }
            }
        }
    }

    @Override
	public V remove(Object key) {
        return remove2(key, null);
    }

    private V remove2(final Object key, final Object value) {
        long current = engine.get(rootRecidRef, Serializer.LONG);

        BNode A = engine.get(current, nodeSerializer);
        while(!A.isLeaf()){
            current = nextDir((DirNode) A, key);
            A = engine.get(current, nodeSerializer);
        }

        try{
        while(true){

            lock(nodeLocks, current);
            A = engine.get(current, nodeSerializer);
            int pos = findChildren(key, A.keys());
            if(pos<A.keys().length&& key!=null && A.keys()[pos]!=null &&
                    0==comparator.compare(key,A.keys()[pos])){
                //check for last node which was already deleted
                if(pos == A.keys().length-1 && value == null){
                    unlock(nodeLocks, current);
                    return null;
                }

                //delete from node
                Object oldVal =   A.vals()[pos-1];
                oldVal = valExpand(oldVal);
                if(value!=null && !value.equals(oldVal)){
                    unlock(nodeLocks, current);
                    return null;
                }

                Object[] keys2 = new Object[A.keys().length-1];
                System.arraycopy(A.keys(),0,keys2, 0, pos);
                System.arraycopy(A.keys(), pos+1, keys2, pos, keys2.length-pos);

                Object[] vals2 = new Object[A.vals().length-1];
                System.arraycopy(A.vals(),0,vals2, 0, pos-1);
                System.arraycopy(A.vals(), pos, vals2, pos-1, vals2.length-(pos-1));


                A = new LeafNode(keys2, vals2, ((LeafNode)A).next);
                assert(nodeLocks.get(current)==Thread.currentThread());
                engine.update(current, A, nodeSerializer);
                notify((K)key, (V)oldVal, null);
                unlock(nodeLocks, current);
                return (V) oldVal;
            }else{
                unlock(nodeLocks, current);
                //follow link until necessary
                if(A.highKey() != null && comparator.compare(key, A.highKey())>0){
                    int pos2 = findChildren(key, A.keys());
                    while(pos2 == A.keys().length){
                        //TODO lock?
                        current = ((LeafNode)A).next;
                        A = engine.get(current, nodeSerializer);
                    }
                }else{
                    return null;
                }
            }
        }
        }catch(RuntimeException e){
            unlockAll(nodeLocks);
            throw e;
        }catch(Exception e){
            unlockAll(nodeLocks);
            throw new RuntimeException(e);
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


    static class BTreeKeyIterator<K> extends BTreeIterator implements Iterator<K>{

        BTreeKeyIterator(BTreeMap m) {
            super(m);
        }

        BTreeKeyIterator(BTreeMap m, Object lo, boolean loInclusive, Object hi, boolean hiInclusive) {
            super(m, lo, loInclusive, hi, hiInclusive);
        }

        @Override
        public K next() {
            if(currentLeaf == null) throw new NoSuchElementException();
            K ret = (K) currentLeaf.keys[currentPos];
            advance();
            return ret;
        }
    }

    static  class BTreeValueIterator<V> extends BTreeIterator implements Iterator<V>{

        BTreeValueIterator(BTreeMap m) {
            super(m);
        }

        BTreeValueIterator(BTreeMap m, Object lo, boolean loInclusive, Object hi, boolean hiInclusive) {
            super(m, lo, loInclusive, hi, hiInclusive);
        }

        @Override
        public V next() {
            if(currentLeaf == null) throw new NoSuchElementException();
            Object ret = currentLeaf.vals[currentPos-1];
            advance();
            return (V) m.valExpand(ret);
        }

    }

    static  class BTreeEntryIterator<K,V> extends BTreeIterator implements  Iterator<Entry<K, V>>{

        BTreeEntryIterator(BTreeMap m) {
            super(m);
        }

        BTreeEntryIterator(BTreeMap m, Object lo, boolean loInclusive, Object hi, boolean hiInclusive) {
            super(m, lo, loInclusive, hi, hiInclusive);
        }

        @Override
        public Entry<K, V> next() {
            if(currentLeaf == null) throw new NoSuchElementException();
            K ret = (K) currentLeaf.keys[currentPos];
            Object val = currentLeaf.vals[currentPos-1];
            advance();
            return m.makeEntry(ret, m.valExpand(val));

        }
    }






    protected Entry<K, V> makeEntry(Object key, Object value) {
        assert(!(value instanceof ValRef));
        return new SimpleImmutableEntry<K, V>((K)key,  (V)value);
    }


    @Override
    public boolean isEmpty() {
        return !keyIterator().hasNext();
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

        long size = 0;
        BTreeIterator iter = new BTreeIterator(this);
        while(iter.hasNext()){
            iter.advance();
            size++;
        }
        return size;
    }

    @Override
    public V putIfAbsent(K key, V value) {
        if(key == null || value == null) throw new NullPointerException();
        return put2(key, value, true);
    }

    @Override
    public boolean remove(Object key, Object value) {
        if(key == null) throw new NullPointerException();
        if(value == null) return false;
        return remove2(key, value)!=null;
    }

    @Override
    public boolean replace(final K key, final V oldValue, final V newValue) {
        if(key == null || oldValue == null || newValue == null ) throw new NullPointerException();

        long current = engine.get(rootRecidRef, Serializer.LONG);

        BNode node = engine.get(current, nodeSerializer);
        //dive until leaf is found
        while(!node.isLeaf()){
            current = nextDir((DirNode) node, key);
            node = engine.get(current, nodeSerializer);
        }

        lock(nodeLocks, current);
        try{

        LeafNode leaf = (LeafNode) engine.get(current, nodeSerializer);
        int pos = findChildren(key, leaf.keys);

        while(pos==leaf.keys.length){
            //follow leaf link until necessary
            lock(nodeLocks, leaf.next);
            unlock(nodeLocks, current);
            current = leaf.next;
            leaf = (LeafNode) engine.get(current, nodeSerializer);
            pos = findChildren(key, leaf.keys);
        }

        boolean ret = false;
        if( key!=null && leaf.keys[pos]!=null &&
                comparator.compare(key,leaf.keys[pos])==0){
            Object val  = leaf.vals[pos-1];
            val = valExpand(val);
            if(oldValue.equals(val)){
                Object[] vals = Arrays.copyOf(leaf.vals, leaf.vals.length);
                notify(key, oldValue, newValue);
                vals[pos-1] = newValue;
                if(valsOutsideNodes){
                    long recid = engine.put(newValue, valueSerializer);
                    vals[pos-1] =  new ValRef(recid);
                }

                leaf = new LeafNode(Arrays.copyOf(leaf.keys, leaf.keys.length), vals, leaf.next);

                assert(nodeLocks.get(current)==Thread.currentThread());
                engine.update(current, leaf, nodeSerializer);

                ret = true;
            }
        }
        unlock(nodeLocks, current);
        return ret;
        }catch(RuntimeException e){
            unlockAll(nodeLocks);
            throw e;
        }catch(Exception e){
            unlockAll(nodeLocks);
            throw new RuntimeException(e);
        }
    }

    @Override
    public V replace(final K key, final V value) {
        if(key == null || value == null) throw new NullPointerException();
        long current = engine.get(rootRecidRef,Serializer.LONG);

        BNode node = engine.get(current, nodeSerializer);
        //dive until leaf is found
        while(!node.isLeaf()){
            current = nextDir((DirNode) node, key);
            node = engine.get(current, nodeSerializer);
        }

        lock(nodeLocks, current);
        try{

        LeafNode leaf = (LeafNode) engine.get(current, nodeSerializer);
        int pos = findChildren(key, leaf.keys);

        while(pos==leaf.keys.length){
            //follow leaf link until necessary
            lock(nodeLocks, leaf.next);
            unlock(nodeLocks, current);
            current = leaf.next;
            leaf = (LeafNode) engine.get(current, nodeSerializer);
            pos = findChildren(key, leaf.keys);
        }

        Object ret = null;
        if( key!=null && leaf.keys()[pos]!=null &&
                0==comparator.compare(key,leaf.keys[pos])){
            Object[] vals = Arrays.copyOf(leaf.vals, leaf.vals.length);
            Object oldVal = vals[pos-1];
            ret =  valExpand(oldVal);
            notify(key, (V)ret, value);
            vals[pos-1] = value;
            if(valsOutsideNodes && value!=null){
                long recid = engine.put(value, valueSerializer);
                vals[pos-1] = new ValRef(recid);
            }

            leaf = new LeafNode(Arrays.copyOf(leaf.keys, leaf.keys.length), vals, leaf.next);
            assert(nodeLocks.get(current)==Thread.currentThread());
            engine.update(current, leaf, nodeSerializer);


        }
        unlock(nodeLocks, current);
        return (V)ret;
        }catch(RuntimeException e){
            unlockAll(nodeLocks);
            throw e;
        }catch(Exception e){
            unlockAll(nodeLocks);
            throw new RuntimeException(e);
        }

    }


    @Override
    public Comparator<? super K> comparator() {
        return comparator;
    }


    @Override
    public Map.Entry<K,V> firstEntry() {
        final long rootRecid = engine.get(rootRecidRef, Serializer.LONG);
        BNode n = engine.get(rootRecid, nodeSerializer);
        while(!n.isLeaf()){
            n = engine.get(n.child()[0], nodeSerializer);
        }
        LeafNode l = (LeafNode) n;
        //follow link until necessary
        while(l.keys.length==2){
            if(l.next==0) return null;
            l = (LeafNode) engine.get(l.next, nodeSerializer);
        }
        return makeEntry(l.keys[1], valExpand(l.vals[0]));
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
        if(key==null) throw new NullPointerException();
        final long rootRecid = engine.get(rootRecidRef, Serializer.LONG);
        BNode n = engine.get(rootRecid, nodeSerializer);

        Entry<K,V> k = findSmallerRecur(n, key, inclusive);
        if(k==null || (k.getValue()==null)) return null;
        return k;
    }

    private Entry<K, V> findSmallerRecur(BNode n, K key, boolean inclusive) {
        final boolean leaf = n.isLeaf();
        final int start = leaf ? n.keys().length-2 : n.keys().length-1;
        final int end = leaf?1:0;
        final int res = inclusive? 1 : 0;
        for(int i=start;i>=end; i--){
            final Object key2 = n.keys()[i];
            int comp = (key2==null)? -1 : comparator.compare(key2, key);
            if(comp<res){
                if(leaf){
                    return key2==null ? null :
                            makeEntry(key2, valExpand(n.vals()[i-1]));
                }else{
                    final long recid = n.child()[i];
                    if(recid==0) continue;
                    BNode n2 = engine.get(recid, nodeSerializer);
                    Entry<K,V> ret = findSmallerRecur(n2, key, inclusive);
                    if(ret!=null) return ret;
                }
            }
        }

        return null;
    }


    @Override
    public Map.Entry<K,V> lastEntry() {
        final long rootRecid = engine.get(rootRecidRef, Serializer.LONG);
        BNode n = engine.get(rootRecid, nodeSerializer);
        Entry e = lastEntryRecur(n);
        if(e!=null && e.getValue()==null) return null;
        return e;
    }


    private Map.Entry<K,V> lastEntryRecur(BNode n){
        if(n.isLeaf()){
            //follow next node if available
            if(n.next()!=0){
                BNode n2 = engine.get(n.next(), nodeSerializer);
                Map.Entry<K,V> ret = lastEntryRecur(n2);
                if(ret!=null)
                    return ret;
            }

            //iterate over keys to find last non null key
            for(int i=n.keys().length-2; i>0;i--){
                Object k = n.keys()[i];
                if(k!=null && n.vals().length>0) {
                    Object val = valExpand(n.vals()[i-1]);
                    if(val!=null){
                        return makeEntry(k, val);
                    }
                }
            }
        }else{
            //dir node, dive deeper
            for(int i=n.child().length-1; i>=0;i--){
                long childRecid = n.child()[i];
                if(childRecid==0) continue;
                BNode n2 = engine.get(childRecid, nodeSerializer);
                Entry<K,V> ret = lastEntryRecur(n2);
                if(ret!=null)
                    return ret;
            }
        }
        return null;
    }

    @Override
	public Map.Entry<K,V> lowerEntry(K key) {
        if(key==null) throw new NullPointerException();
        return findSmaller(key, false);
    }

    @Override
	public K lowerKey(K key) {
        Entry<K,V> n = lowerEntry(key);
        return (n == null)? null : n.getKey();
    }

    @Override
	public Map.Entry<K,V> floorEntry(K key) {
        if(key==null) throw new NullPointerException();
        return findSmaller(key, true);
    }

    @Override
	public K floorKey(K key) {
        Entry<K,V> n = floorEntry(key);
        return (n == null)? null : n.getKey();
    }

    @Override
	public Map.Entry<K,V> ceilingEntry(K key) {
        if(key==null) throw new NullPointerException();
        return findLarger(key, true);
    }

    protected Entry<K, V> findLarger(final K key, boolean inclusive) {
        if(key==null) return null;

        long current = engine.get(rootRecidRef, Serializer.LONG);

        BNode A = engine.get(current, nodeSerializer);

        //dive until  leaf
        while(!A.isLeaf()){
            current = nextDir((DirNode) A, key);
            A = engine.get(current, nodeSerializer);
        }

        //now at leaf level
        LeafNode leaf = (LeafNode) A;
        //follow link until first matching node is found
        final int comp = inclusive?1:0;
        while(true){
            for(int i=1;i<leaf.keys.length-1;i++){
                if(leaf.keys[i]==null) continue;

                if(comparator.compare(key, leaf.keys[i])<comp){
                    return makeEntry(leaf.keys[i], valExpand(leaf.vals[i-1]));
                }


            }
            if(leaf.next==0) return null; //reached end
            leaf = (LeafNode) engine.get(leaf.next, nodeSerializer);
        }

    }

    protected Fun.Tuple2<Integer,LeafNode> findLargerNode(final K key, boolean inclusive) {
        if(key==null) return null;

        long current = engine.get(rootRecidRef, Serializer.LONG);

        BNode A = engine.get(current, nodeSerializer);

        //dive until  leaf
        while(!A.isLeaf()){
            current = nextDir((DirNode) A, key);
            A = engine.get(current, nodeSerializer);
        }

        //now at leaf level
        LeafNode leaf = (LeafNode) A;
        //follow link until first matching node is found
        final int comp = inclusive?1:0;
        while(true){
            for(int i=1;i<leaf.keys.length-1;i++){
                if(leaf.keys[i]==null) continue;

                if(comparator.compare(key, leaf.keys[i])<comp){
                    return Fun.t2(i, leaf);
                }
            }
            if(leaf.next==0) return null; //reached end
            leaf = (LeafNode) engine.get(leaf.next, nodeSerializer);
        }

    }


    @Override
	public K ceilingKey(K key) {
        if(key==null) throw new NullPointerException();
        Entry<K,V> n = ceilingEntry(key);
        return (n == null)? null : n.getKey();
    }

    @Override
	public Map.Entry<K,V> higherEntry(K key) {
        if(key==null) throw new NullPointerException();
        return findLarger(key, false);
    }

    @Override
	public K higherKey(K key) {
        if(key==null) throw new NullPointerException();
        Entry<K,V> n = higherEntry(key);
        return (n == null)? null : n.getKey();
    }

    @Override
    public boolean containsKey(Object key) {
        if(key==null) throw new NullPointerException();
        return get(key, false)!=null;
    }

    @Override
    public boolean containsValue(Object value){
        if(value ==null) throw new NullPointerException();
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
        if(e==null) throw new NoSuchElementException();
        return e.getKey();
    }

    @Override
    public K lastKey() {
        Entry<K,V> e = lastEntry();
        if(e==null) throw new NoSuchElementException();
        return e.getKey();
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
        return new BTreeKeyIterator(this);
    }

    Iterator<V> valueIterator() {
        return new BTreeValueIterator(this);
    }

    Iterator<Map.Entry<K,V>> entryIterator() {
        return new BTreeEntryIterator(this);
    }


    /* ---------------- View methods -------------- */

    @Override
	public NavigableSet<K> keySet() {
        return keySet;
    }

    @Override
	public NavigableSet<K> navigableKeySet() {
        return keySet;
    }

    @Override
	public Collection<V> values() {
        return values;
    }

    @Override
	public Set<Map.Entry<K,V>> entrySet() {
        return entrySet;
    }

    @Override
	public ConcurrentNavigableMap<K,V> descendingMap() {
        return descendingMap;
    }

    @Override
	public NavigableSet<K> descendingKeySet() {
        return descendingMap.keySet();
    }

    static <E> List<E> toList(Collection<E> c) {
        // Using size() here would be a pessimization.
        List<E> list = new ArrayList<E>();
        for (E e : c){
            list.add(e);
        }
        return list;
    }



    static final class KeySet<E> extends AbstractSet<E> implements NavigableSet<E> {

        protected final ConcurrentNavigableMap<E,Object> m;
        private final boolean hasValues;
        KeySet(ConcurrentNavigableMap<E,Object> map, boolean hasValues) {
            m = map;
            this.hasValues = hasValues;
        }
        @Override
		public int size() { return m.size(); }
        @Override
		public boolean isEmpty() { return m.isEmpty(); }
        @Override
		public boolean contains(Object o) { return m.containsKey(o); }
        @Override
		public boolean remove(Object o) { return m.remove(o) != null; }
        @Override
		public void clear() { m.clear(); }
        @Override
		public E lower(E e) { return m.lowerKey(e); }
        @Override
		public E floor(E e) { return m.floorKey(e); }
        @Override
		public E ceiling(E e) { return m.ceilingKey(e); }
        @Override
		public E higher(E e) { return m.higherKey(e); }
        @Override
		public Comparator<? super E> comparator() { return m.comparator(); }
        @Override
		public E first() { return m.firstKey(); }
        @Override
		public E last() { return m.lastKey(); }
        @Override
		public E pollFirst() {
            Map.Entry<E,Object> e = m.pollFirstEntry();
            return e == null? null : e.getKey();
        }
        @Override
		public E pollLast() {
            Map.Entry<E,Object> e = m.pollLastEntry();
            return e == null? null : e.getKey();
        }
        @Override
		public Iterator<E> iterator() {
            if (m instanceof BTreeMap)
                return ((BTreeMap<E,Object>)m).keyIterator();
            else if(m instanceof SubMap)
                return ((BTreeMap.SubMap<E,Object>)m).keyIterator();
            else
                return ((BTreeMap.DescendingMap<E,Object>)m).keyIterator();
        }
        @Override
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
        @Override
		public Object[] toArray()     { return toList(this).toArray();  }
        @Override
		public <T> T[] toArray(T[] a) { return toList(this).toArray(a); }
        @Override
		public Iterator<E> descendingIterator() {
            return descendingSet().iterator();
        }
        @Override
		public NavigableSet<E> subSet(E fromElement,
                                      boolean fromInclusive,
                                      E toElement,
                                      boolean toInclusive) {
            return new KeySet<E>(m.subMap(fromElement, fromInclusive,
                    toElement,   toInclusive),hasValues);
        }
        @Override
		public NavigableSet<E> headSet(E toElement, boolean inclusive) {
            return new KeySet<E>(m.headMap(toElement, inclusive),hasValues);
        }
        @Override
		public NavigableSet<E> tailSet(E fromElement, boolean inclusive) {
            return new KeySet<E>(m.tailMap(fromElement, inclusive),hasValues);
        }
        @Override
		public NavigableSet<E> subSet(E fromElement, E toElement) {
            return subSet(fromElement, true, toElement, false);
        }
        @Override
		public NavigableSet<E> headSet(E toElement) {
            return headSet(toElement, false);
        }
        @Override
		public NavigableSet<E> tailSet(E fromElement) {
            return tailSet(fromElement, true);
        }
        @Override
		public NavigableSet<E> descendingSet() {
            return new KeySet(m.descendingMap(),hasValues);
        }

        @Override
        public boolean add(E k) {
            if(hasValues)
                throw new UnsupportedOperationException();
            else
                return m.put(k, EMPTY ) == null;
        }
    }

    static final class Values<E> extends AbstractCollection<E> {
        private final ConcurrentNavigableMap<Object, E> m;
        Values(ConcurrentNavigableMap<Object, E> map) {
            m = map;
        }
        @Override
		public Iterator<E> iterator() {
            if (m instanceof BTreeMap)
                return ((BTreeMap<Object,E>)m).valueIterator();
            else
                return ((SubMap<Object,E>)m).valueIterator();
        }
        @Override
		public boolean isEmpty() {
            return m.isEmpty();
        }
        @Override
		public int size() {
            return m.size();
        }
        @Override
		public boolean contains(Object o) {
            return m.containsValue(o);
        }
        @Override
		public void clear() {
            m.clear();
        }
        @Override
		public Object[] toArray()     { return toList(this).toArray();  }
        @Override
		public <T> T[] toArray(T[] a) { return toList(this).toArray(a); }
    }

    static final class EntrySet<K1,V1> extends AbstractSet<Map.Entry<K1,V1>> {
        private final ConcurrentNavigableMap<K1, V1> m;
        EntrySet(ConcurrentNavigableMap<K1, V1> map) {
            m = map;
        }

        @Override
		public Iterator<Map.Entry<K1,V1>> iterator() {
            if (m instanceof BTreeMap)
                return ((BTreeMap<K1,V1>)m).entryIterator();
            else if(m instanceof  SubMap)
                return ((SubMap<K1,V1>)m).entryIterator();
            else
                return ((DescendingMap<K1,V1>)m).entryIterator();
        }

        @Override
		public boolean contains(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<K1,V1> e = (Map.Entry<K1,V1>)o;
            K1 key = e.getKey();
            if(key == null) return false;
            V1 v = m.get(key);
            return v != null && v.equals(e.getValue());
        }
        @Override
		public boolean remove(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<K1,V1> e = (Map.Entry<K1,V1>)o;
            K1 key = e.getKey();
            if(key == null) return false;
            return m.remove(key,
                    e.getValue());
        }
        @Override
		public boolean isEmpty() {
            return m.isEmpty();
        }
        @Override
		public int size() {
            return m.size();
        }
        @Override
		public void clear() {
            m.clear();
        }
        @Override
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
        @Override
		public Object[] toArray()     { return toList(this).toArray();  }
        @Override
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
            if(lo!=null && hi!=null && m.comparator.compare(lo, hi)>0){
                    throw new IllegalArgumentException();
            }


        }


/* ----------------  Map API methods -------------- */

        @Override
		public boolean containsKey(Object key) {
            if (key == null) throw new NullPointerException();
            K k = (K)key;
            return inBounds(k) && m.containsKey(k);
        }

        @Override
		public V get(Object key) {
            if (key == null) throw new NullPointerException();
            K k = (K)key;
            return ((!inBounds(k)) ? null : m.get(k));
        }

        @Override
		public V put(K key, V value) {
            checkKeyBounds(key);
            return m.put(key, value);
        }

        @Override
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

        @Override
		public boolean isEmpty() {
            return !keyIterator().hasNext();
        }

        @Override
		public boolean containsValue(Object value) {
            if(value==null) throw new NullPointerException();
            Iterator<V> i = valueIterator();
            while(i.hasNext()){
                if(value.equals(i.next()))
                    return true;
            }
            return false;
        }

        @Override
		public void clear() {
            Iterator<K> i = keyIterator();
            while(i.hasNext()){
                i.next();
                i.remove();
            }
        }


        /* ----------------  ConcurrentMap API methods -------------- */

        @Override
		public V putIfAbsent(K key, V value) {
            checkKeyBounds(key);
            return m.putIfAbsent(key, value);
        }

        @Override
		public boolean remove(Object key, Object value) {
            K k = (K)key;
            return inBounds(k) && m.remove(k, value);
        }

        @Override
		public boolean replace(K key, V oldValue, V newValue) {
            checkKeyBounds(key);
            return m.replace(key, oldValue, newValue);
        }

        @Override
		public V replace(K key, V value) {
            checkKeyBounds(key);
            return m.replace(key, value);
        }

        /* ----------------  SortedMap API methods -------------- */

        @Override
		public Comparator<? super K> comparator() {
            return m.comparator();
        }

        /* ----------------  Relational methods -------------- */

        @Override
		public Map.Entry<K,V> lowerEntry(K key) {
            if(key==null)throw new NullPointerException();
            if(tooLow(key))return null;

            if(tooHigh(key))
                return lastEntry();

            Entry<K,V> r = m.lowerEntry(key);
            return r!=null && !tooLow(r.getKey()) ? r :null;
        }

        @Override
		public K lowerKey(K key) {
            Entry<K,V> n = lowerEntry(key);
            return (n == null)? null : n.getKey();
        }

        @Override
		public Map.Entry<K,V> floorEntry(K key) {
            if(key==null) throw new NullPointerException();
            if(tooLow(key)) return null;

            if(tooHigh(key)){
                return lastEntry();
            }

            Entry<K,V> ret = m.floorEntry(key);
            if(ret!=null && tooLow(ret.getKey())) return null;
            return ret;

        }

        @Override
		public K floorKey(K key) {
            Entry<K,V> n = floorEntry(key);
            return (n == null)? null : n.getKey();
        }

        @Override
		public Map.Entry<K,V> ceilingEntry(K key) {
            if(key==null) throw new NullPointerException();
            if(tooHigh(key)) return null;

            if(tooLow(key)){
                return firstEntry();
            }

            Entry<K,V> ret = m.ceilingEntry(key);
            if(ret!=null && tooHigh(ret.getKey())) return null;
            return ret;
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


        @Override
		public K firstKey() {
            Entry<K,V> e = firstEntry();
            if(e==null) throw new NoSuchElementException();
            return e.getKey();
        }

        @Override
		public K lastKey() {
            Entry<K,V> e = lastEntry();
            if(e==null) throw new NoSuchElementException();
            return e.getKey();
        }


        @Override
		public Map.Entry<K,V> firstEntry() {
            Entry<K,V> k =
                    lo==null ?
                    m.firstEntry():
                    m.findLarger(lo, loInclusive);
            return k!=null && inBounds(k.getKey())? k : null;

        }

        @Override
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

//            if(fromKey!=null && toKey!=null){
//                int comp = m.comparator.compare(fromKey, toKey);
//                if((fromInclusive||!toInclusive) && comp==0)
//                    throw new IllegalArgumentException();
//            }

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

        @Override
		public SubMap<K,V> subMap(K fromKey,
                                  boolean fromInclusive,
                                  K toKey,
                                  boolean toInclusive) {
            if (fromKey == null || toKey == null)
                throw new NullPointerException();
            return newSubMap(fromKey, fromInclusive, toKey, toInclusive);
        }

        @Override
		public SubMap<K,V> headMap(K toKey,
                                   boolean inclusive) {
            if (toKey == null)
                throw new NullPointerException();
            return newSubMap(null, false, toKey, inclusive);
        }

        @Override
		public SubMap<K,V> tailMap(K fromKey,
                                   boolean inclusive) {
            if (fromKey == null)
                throw new NullPointerException();
            return newSubMap(fromKey, inclusive, null, false);
        }

        @Override
		public SubMap<K,V> subMap(K fromKey, K toKey) {
            return subMap(fromKey, true, toKey, false);
        }

        @Override
		public SubMap<K,V> headMap(K toKey) {
            return headMap(toKey, false);
        }

        @Override
		public SubMap<K,V> tailMap(K fromKey) {
            return tailMap(fromKey, true);
        }

        @Override
		public ConcurrentNavigableMap<K,V> descendingMap() {
            return new DescendingMap(m, lo,loInclusive, hi,hiInclusive);
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
            return new DescendingMap<K,V>(m,lo,loInclusive, hi, hiInclusive).keySet();
        }



        @Override
        public Set<Entry<K, V>> entrySet() {
            return new EntrySet<K, V>(this);
        }



        Iterator<K> keyIterator() {
            return new BTreeKeyIterator(m,lo,loInclusive,hi,hiInclusive);
        }

        Iterator<V> valueIterator() {
            return new BTreeValueIterator(m,lo,loInclusive,hi,hiInclusive);
        }

        Iterator<Map.Entry<K,V>> entryIterator() {
            return new BTreeEntryIterator(m,lo,loInclusive,hi,hiInclusive);
        }

    }


    static protected  class DescendingMap<K,V> extends AbstractMap<K,V> implements  ConcurrentNavigableMap<K,V> {

        protected final BTreeMap<K,V> m;

        protected final K lo;
        protected final boolean loInclusive;

        protected final K hi;
        protected final boolean hiInclusive;

        public DescendingMap(BTreeMap<K,V> m, K lo, boolean loInclusive, K hi, boolean hiInclusive) {
            this.m = m;
            this.lo = lo;
            this.loInclusive = loInclusive;
            this.hi = hi;
            this.hiInclusive = hiInclusive;
            if(lo!=null && hi!=null && m.comparator.compare(lo, hi)>0){
                throw new IllegalArgumentException();
            }


        }


/* ----------------  Map API methods -------------- */

        @Override
        public boolean containsKey(Object key) {
            if (key == null) throw new NullPointerException();
            K k = (K)key;
            return inBounds(k) && m.containsKey(k);
        }

        @Override
        public V get(Object key) {
            if (key == null) throw new NullPointerException();
            K k = (K)key;
            return ((!inBounds(k)) ? null : m.get(k));
        }

        @Override
        public V put(K key, V value) {
            checkKeyBounds(key);
            return m.put(key, value);
        }

        @Override
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

        @Override
        public boolean isEmpty() {
            return !keyIterator().hasNext();
        }

        @Override
        public boolean containsValue(Object value) {
            if(value==null) throw new NullPointerException();
            Iterator<V> i = valueIterator();
            while(i.hasNext()){
                if(value.equals(i.next()))
                    return true;
            }
            return false;
        }

        @Override
        public void clear() {
            Iterator<K> i = keyIterator();
            while(i.hasNext()){
                i.next();
                i.remove();
            }
        }


        /* ----------------  ConcurrentMap API methods -------------- */

        @Override
        public V putIfAbsent(K key, V value) {
            checkKeyBounds(key);
            return m.putIfAbsent(key, value);
        }

        @Override
        public boolean remove(Object key, Object value) {
            K k = (K)key;
            return inBounds(k) && m.remove(k, value);
        }

        @Override
        public boolean replace(K key, V oldValue, V newValue) {
            checkKeyBounds(key);
            return m.replace(key, oldValue, newValue);
        }

        @Override
        public V replace(K key, V value) {
            checkKeyBounds(key);
            return m.replace(key, value);
        }

        /* ----------------  SortedMap API methods -------------- */

        @Override
        public Comparator<? super K> comparator() {
            return m.comparator();
        }

        /* ----------------  Relational methods -------------- */

        @Override
        public Map.Entry<K,V> higherEntry(K key) {
            if(key==null)throw new NullPointerException();
            if(tooLow(key))return null;

            if(tooHigh(key))
                return firstEntry();

            Entry<K,V> r = m.lowerEntry(key);
            return r!=null && !tooLow(r.getKey()) ? r :null;
        }

        @Override
        public K lowerKey(K key) {
            Entry<K,V> n = lowerEntry(key);
            return (n == null)? null : n.getKey();
        }

        @Override
        public Map.Entry<K,V> ceilingEntry(K key) {
            if(key==null) throw new NullPointerException();
            if(tooLow(key)) return null;

            if(tooHigh(key)){
                return firstEntry();
            }

            Entry<K,V> ret = m.floorEntry(key);
            if(ret!=null && tooLow(ret.getKey())) return null;
            return ret;

        }

        @Override
        public K floorKey(K key) {
            Entry<K,V> n = floorEntry(key);
            return (n == null)? null : n.getKey();
        }

        @Override
        public Map.Entry<K,V> floorEntry(K key) {
            if(key==null) throw new NullPointerException();
            if(tooHigh(key)) return null;

            if(tooLow(key)){
                return lastEntry();
            }

            Entry<K,V> ret = m.ceilingEntry(key);
            if(ret!=null && tooHigh(ret.getKey())) return null;
            return ret;
        }

        @Override
        public K ceilingKey(K key) {
            Entry<K,V> k = ceilingEntry(key);
            return k!=null? k.getKey():null;
        }

        @Override
        public Entry<K, V> lowerEntry(K key) {
            Entry<K,V> r = m.higherEntry(key);
            return r!=null && inBounds(r.getKey()) ? r : null;
        }

        @Override
        public K higherKey(K key) {
            Entry<K,V> k = higherEntry(key);
            return k!=null? k.getKey():null;
        }


        @Override
        public K firstKey() {
            Entry<K,V> e = firstEntry();
            if(e==null) throw new NoSuchElementException();
            return e.getKey();
        }

        @Override
        public K lastKey() {
            Entry<K,V> e = lastEntry();
            if(e==null) throw new NoSuchElementException();
            return e.getKey();
        }


        @Override
        public Map.Entry<K,V> lastEntry() {
            Entry<K,V> k =
                    lo==null ?
                            m.firstEntry():
                            m.findLarger(lo, loInclusive);
            return k!=null && inBounds(k.getKey())? k : null;

        }

        @Override
        public Map.Entry<K,V> firstEntry() {
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
        private DescendingMap<K,V> newSubMap(
                                      K toKey,
                                      boolean toInclusive,
                                      K fromKey,
                                      boolean fromInclusive) {

//            if(fromKey!=null && toKey!=null){
//                int comp = m.comparator.compare(fromKey, toKey);
//                if((fromInclusive||!toInclusive) && comp==0)
//                    throw new IllegalArgumentException();
//            }

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
            return new DescendingMap<K,V>(m, fromKey, fromInclusive,
                    toKey, toInclusive);
        }

        @Override
        public DescendingMap<K,V> subMap(K fromKey,
                                  boolean fromInclusive,
                                  K toKey,
                                  boolean toInclusive) {
            if (fromKey == null || toKey == null)
                throw new NullPointerException();
            return newSubMap(fromKey, fromInclusive, toKey, toInclusive);
        }

        @Override
        public DescendingMap<K,V> headMap(K toKey,
                                   boolean inclusive) {
            if (toKey == null)
                throw new NullPointerException();
            return newSubMap(null, false, toKey, inclusive);
        }

        @Override
        public DescendingMap<K,V> tailMap(K fromKey,
                                   boolean inclusive) {
            if (fromKey == null)
                throw new NullPointerException();
            return newSubMap(fromKey, inclusive, null, false);
        }

        @Override
        public DescendingMap<K,V> subMap(K fromKey, K toKey) {
            return subMap(fromKey, true, toKey, false);
        }

        @Override
        public DescendingMap<K,V> headMap(K toKey) {
            return headMap(toKey, false);
        }

        @Override
        public DescendingMap<K,V> tailMap(K fromKey) {
            return tailMap(fromKey, true);
        }

        @Override
        public ConcurrentNavigableMap<K,V> descendingMap() {
            if(lo==null && hi==null) return m;
            return m.subMap(lo,loInclusive,hi,hiInclusive);
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
            return new KeySet<K>((ConcurrentNavigableMap<K,Object>) descendingMap(), m.hasValues);
        }



        @Override
        public Set<Entry<K, V>> entrySet() {
            return new EntrySet<K, V>(this);
        }


        /*
         * ITERATORS
         */

        abstract class Iter<E> implements Iterator<E> {
            Entry<K,V> current = DescendingMap.this.firstEntry();
            Entry<K,V> last = null;


            @Override
            public boolean hasNext() {
                return current!=null;
            }


            public void advance() {
                if(current==null) throw new NoSuchElementException();
                last = current;
                current = DescendingMap.this.higherEntry(current.getKey());
            }

            @Override
            public void remove() {
                if(last==null) throw new IllegalStateException();
                DescendingMap.this.remove(last.getKey());
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


    /**
     * Make readonly snapshot view of current Map. Snapshot is immutable and not affected by modifications made by other threads.
     * Useful if you need consistent view on Map.
     * 
     * Maintaining snapshot have some overhead, underlying Engine is closed after Map view is GCed.
     * Please make sure to release reference to this Map view, so snapshot view can be garbage collected.
     *
     * @return snapshot
     */
    public NavigableMap<K,V> snapshot(){
        Engine snapshot = TxEngine.createSnapshotFor(engine);

        return new BTreeMap<K, V>(snapshot, rootRecidRef, maxNodeSize, valsOutsideNodes,
                counter==null?0L:counter.recid,
                keySerializer, valueSerializer, comparator, numberOfNodeMetas, false);
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
        assert(!(oldValue instanceof ValRef));
        assert(!(newValue instanceof ValRef));

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

    public Engine getEngine(){
        return engine;
    }


    public void printTreeStructure() {
        final long rootRecid = engine.get(rootRecidRef, Serializer.LONG);
        printRecur(this, rootRecid, "");
    }

    private static void printRecur(BTreeMap m, long recid, String s) {
        BTreeMap.BNode n = (BTreeMap.BNode) m.engine.get(recid, m.nodeSerializer);
        System.out.println(s+recid+"-"+n);
        if(!n.isLeaf()){
            for(int i=0;i<n.child().length-1;i++){
                long recid2 = n.child()[i];
                if(recid2!=0)
                    printRecur(m, recid2, s+"  ");
            }
        }
    }


    protected  static long[] arrayLongPut(final long[] array, final int pos, final long value) {
        final long[] ret = Arrays.copyOf(array,array.length+1);
        if(pos<array.length){
            System.arraycopy(array,pos,ret,pos+1,array.length-pos);
        }
        ret[pos] = value;
        return ret;
    }

    /** expand array size by 1, and put value at given position. No items from original array are lost*/
    protected static Object[] arrayPut(final Object[] array, final int pos, final Object value){
        final Object[] ret = Arrays.copyOf(array, array.length+1);
        if(pos<array.length){
            System.arraycopy(array, pos, ret, pos+1, array.length-pos);
        }
        ret[pos] = value;
        return ret;
    }

    protected static void assertNoLocks(LongConcurrentHashMap<Thread> locks){
        LongMap.LongMapIterator<Thread> i = locks.longMapIterator();
        Thread t =null;
        while(i.moveToNext()){
            if(t==null)
                t = Thread.currentThread();
            if(i.value()==t){
                throw new AssertionError("Node "+i.key()+" is still locked");
            }
        }
    }


    protected static void unlock(LongConcurrentHashMap<Thread> locks,final long recid) {
        final Thread t = locks.remove(recid);
        assert(t==Thread.currentThread()):("unlocked wrong thread");
    }

    protected static void unlockAll(LongConcurrentHashMap<Thread> locks) {
        final Thread t = Thread.currentThread();
        LongMap.LongMapIterator<Thread> iter = locks.longMapIterator();
        while(iter.moveToNext())
            if(iter.value()==t)
                iter.remove();
    }


    protected static void lock(LongConcurrentHashMap<Thread> locks, long recid){
        //feel free to rewrite, if you know better (more efficient) way

        final Thread currentThread = Thread.currentThread();
        //check node is not already locked by this thread
        assert(locks.get(recid)!= currentThread):("node already locked by current thread: "+recid);

        while(locks.putIfAbsent(recid, currentThread) != null){
            LockSupport.parkNanos(10);
        }
    }




}
