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

import static org.mapdb.SerializationHeader.*;

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
 * <p>
 * It is possible to obtain <i>consistent</i> iterator by using <code>snapshot()</code>
 * method.
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
 * <p>Theoretical design of BTreeMap is based on <a href="http://www.cs.cornell.edu/courses/cs4411/2009sp/blink.pdf">paper</a>
 * from Philip L. Lehman and S. Bing Yao. More practical aspects of BTreeMap implementation are based on <a href="http://www.doc.ic.ac.uk/~td202/">notes</a>
 * and <a href="http://www.doc.ic.ac.uk/~td202/btree/">demo application</a> from Thomas Dinsdale-Young.
 * B-Linked-Tree used here does not require locking for read. Updates locks only one, two or three nodes.
 * <p/>
 * This B-Linked-Tree structure does not support removal well, entry delete does not collapse tree nodes. Massive
 * deletion causes empty nodes and performance lost. There is workaround in form of compaction process, but it is not
 * implemented yet.
 *
 * @author Jan Kotek
 * @author some parts by Doug Lea
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
//TODO better tests for BTreeMap without values (set)
public class BTreeMap<K,V> extends AbstractMap<K,V>
        implements ConcurrentNavigableMap<K,V>, Bind.MapWithModificationListener<K,V>{




    /** default maximal node size */
    protected static final int DEFAULT_MAX_NODE_SIZE = 32;


    /** recid under which reference to rootRecid is stored */
    protected final long rootRecidRef;

    /** Serializer used to convert keys from/into binary form.
     * TODO delta packing on BTree keys*/
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


    protected final long treeRecid;


    private final KeySet keySet;

    private final EntrySet entrySet = new EntrySet(this);

    private final Values values = new Values(this);
    protected final Serializer defaultSerializer;
    protected final Atomic.Long counter;


    static class BTreeRootSerializer implements  Serializer<BTreeRoot>{
        protected final Serializer defaultSerializer;

        BTreeRootSerializer(Serializer defaultSerializer) {
            this.defaultSerializer = defaultSerializer;
        }

        @Override
        public void serialize(DataOutput out, BTreeRoot value) throws IOException {
            out.writeByte(SerializationHeader.B_TREE_MAP_ROOT_HEADER);
            out.writeLong(value.rootRecidRef);
            out.writeBoolean(value.hasValues);
            out.writeBoolean(value.valsOutsideNodes);
            out.writeInt(value.maxNodeSize);
            out.writeLong(value.counterRecid);
            defaultSerializer.serialize(out, value.keySerializer);
            defaultSerializer.serialize(out, value.valueSerializer);
            defaultSerializer.serialize(out, value.comparator);

        }

        @Override
        public BTreeRoot deserialize(DataInput in, int available) throws IOException {
            BTreeRoot ret = new BTreeRoot();
            if(in.readUnsignedByte()!=SerializationHeader.B_TREE_MAP_ROOT_HEADER) throw new InternalError();
            ret.rootRecidRef = in.readLong();
            ret.hasValues = in.readBoolean();
            ret.valsOutsideNodes = in.readBoolean();
            ret.maxNodeSize = in.readInt();
            ret.counterRecid = in.readLong();
            ret.keySerializer = (BTreeKeySerializer) defaultSerializer.deserialize(in, -1);
            ret.valueSerializer = (Serializer) defaultSerializer.deserialize(in, -1);
            ret.comparator = (Comparator) defaultSerializer.deserialize(in, -1);
            return ret;
        }
    }

    /** data record which holds informations about this BTree. BTreeMap class is not serialized itself. */
    static final class BTreeRoot{
        long rootRecidRef;
        boolean hasValues;
        boolean valsOutsideNodes;
        int maxNodeSize;
        long counterRecid;
        BTreeKeySerializer keySerializer;
        Serializer valueSerializer;
        Comparator comparator;
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
            throw new InternalError();
        }

        @Override
        public int hashCode() {
            throw new InternalError();
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


    protected final Serializer<BNode> nodeSerializer = new Serializer<BNode>() {
        @Override
        public void serialize(DataOutput out, BNode value) throws IOException {
            final boolean isLeaf = value.isLeaf();

            //first byte encodes if is leaf (first bite) and length (last seven bites)
            if(value.keys().length>255) throw new InternalError();
            if(!isLeaf && value.child().length!= value.keys().length) throw new InternalError();
            if(isLeaf && hasValues && value.vals().length!= value.keys().length) throw new InternalError();

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

            if(isLeaf)
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
            else{
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

            //longs go first, so it is possible to reconstruct tree without serializer
            if(isLeaf){
                Utils.packLong(out, ((LeafNode) value).next);
            }else{
                for(long child : ((DirNode)value).child)
                    Utils.packLong(out, child);
            }



            keySerializer.serialize(out,left?1:0,
                    right?value.keys().length-1:value.keys().length,
                    value.keys());

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
            final int header = in.readUnsignedByte();
            final int size = in.readUnsignedByte();
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
                long next = Utils.unpackLong(in);
                Object[] keys = (Object[]) keySerializer.deserialize(in, start,end,size);
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
                Object[] keys = (Object[]) keySerializer.deserialize(in, start,end,size);
                if(keys.length!=size) throw new InternalError();
                return new DirNode(keys, child);
            }
        }
    };


    /** Constructor used to create new BTreeMap without existing record (recid) in Engine.
     *  This constructor creates new record and saves all configuration parameters there.
     *  Constructor args are defining BTreeMap format, are stored in db and can not be changed latter.
     *
     * @param engine used for persistence
     * @param maxNodeSize maximal BTree Node size. Node will split if number of entries is higher
     * @param hasValues is Map or Set? If true only keys will be stored, no values
     * @param valsOutsideNodes Store Values outside of BTree Nodes in separate record?
     * @param defaultSerializer serialier used to serialize/deserialize other serializers. May be null for default value.
     * @param keySerializer Serialzier used for keys. May be null for defualt value. TODO delta packing
     * @param valueSerializer Serializer used for values. May be null for default value
     * @param comparator Comparator to sort keys in this BTree, may be null.
     */
    public BTreeMap(Engine engine, int maxNodeSize, boolean hasValues, boolean valsOutsideNodes, boolean keepCounter,
                    Serializer defaultSerializer,
                    BTreeKeySerializer<K> keySerializer, Serializer<V> valueSerializer, Comparator<K> comparator) {
        if(maxNodeSize%2!=0) throw new IllegalArgumentException("maxNodeSize must be dividable by 2");
        if(maxNodeSize<6) throw new IllegalArgumentException("maxNodeSize too low");
        if(maxNodeSize>126) throw new IllegalArgumentException("maxNodeSize too high");
        SerializerBase.assertSerializable(keySerializer);
        SerializerBase.assertSerializable(valueSerializer);
        SerializerBase.assertSerializable(comparator);


        if(defaultSerializer==null) defaultSerializer = Serializer.BASIC_SERIALIZER;


        this.defaultSerializer = defaultSerializer;
        this.hasValues = hasValues;
        this.valsOutsideNodes = valsOutsideNodes;
        this.engine = engine;
        this.maxNodeSize = maxNodeSize;
        this.comparator = comparator==null? Utils.COMPARABLE_COMPARATOR : comparator;
        //TODO when delta packing implemented, add assertion for COMPARABLE_COMPARATOR
        this.keySerializer = keySerializer==null ?  new BTreeKeySerializer.BasicKeySerializer(defaultSerializer) :  keySerializer;
        this.valueSerializer = valueSerializer==null ? (Serializer<V>) defaultSerializer : valueSerializer;


        this.keySet = new KeySet(this, hasValues);

        LeafNode emptyRoot = new LeafNode(new Object[]{null, null}, new Object[]{null, null}, 0);
        long rootRecidVal = engine.put(emptyRoot, nodeSerializer);
        rootRecidRef = engine.put(rootRecidVal,Serializer.LONG_SERIALIZER);

        long counterRecid = 0;
        if(keepCounter){
            counterRecid = engine.put(0L, Serializer.LONG_SERIALIZER);
            this.counter = new Atomic.Long(engine,counterRecid);
            Bind.size(this,counter);
        }else{
            this.counter = null;
        }

        BTreeRoot r = new BTreeRoot();
        r.hasValues = this.hasValues;
        r.valsOutsideNodes = this.valsOutsideNodes;
        r.rootRecidRef = this.rootRecidRef;
        r.maxNodeSize =  this.maxNodeSize;
        r.keySerializer =  this.keySerializer;
        r.valueSerializer =  this.valueSerializer;
        r.comparator =  this.comparator;
        r.counterRecid = counterRecid;
        this.treeRecid = engine.put(r, new BTreeRootSerializer(this.defaultSerializer));


    }



    /**
     * Constructor used to load existing BTreeMap (with assigned recid).
     * Map was already created and saved to Engine, this constructor just loads it.
     *
     * @param engine used for persistence
     * @param recid under which BTreeMap was stored
     * @param defaultSerializer used to deserialize other serializers and comparator
     */
    public BTreeMap(Engine engine, long recid, Serializer defaultSerializer) {
        this.engine = engine;
        this.treeRecid = recid;
        if(defaultSerializer==null) defaultSerializer = Serializer.BASIC_SERIALIZER;
        this.defaultSerializer = defaultSerializer;

        BTreeRoot r = engine.get(recid, new BTreeRootSerializer(defaultSerializer));
        this.hasValues = r.hasValues;
        this.rootRecidRef = r.rootRecidRef;
        this.maxNodeSize = r.maxNodeSize;
        this.keySerializer = r.keySerializer;
        this.valueSerializer = r.valueSerializer;
        this.comparator = r.comparator;
        this.valsOutsideNodes = r.valsOutsideNodes;


        this.keySet = new KeySet(this, hasValues);

        if(r.counterRecid!=0){
            counter = new Atomic.Long(engine,r.counterRecid);
            Bind.size(this,counter);
        }else{
            this.counter = null;
        }
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
        if(key==null) throw new NullPointerException();
        K v = (K) key;
        final long rootRecid = engine.get(rootRecidRef, Serializer.LONG_SERIALIZER);
        long current = rootRecid;
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
            Object ret = (hasValues? leaf.vals[pos] : Utils.EMPTY_STRING);
            return valExpand(ret);
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

    protected V put2(K v, V value2, final boolean putOnlyIfAbsent){
        if(v == null) throw new IllegalArgumentException("null key");
        if(value2 == null) throw new IllegalArgumentException("null value");
        Utils.checkMapValueIsNotCollecion(value2);

        V value = value2;
        if(valsOutsideNodes){
            long recid = engine.put(value2, valueSerializer);
            value = (V) new ValRef(recid);
        }

        int stackPos = -1;
        long[] stackVals = new long[4];

        final long rootRecid = engine.get(rootRecidRef, Serializer.LONG_SERIALIZER);
        long current = rootRecid;

        BNode A = engine.get(current, nodeSerializer);
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
            A = engine.get(current, nodeSerializer);
        }
        int level = 1;

        long p=0;
        try{
        while(true){
            boolean found;
            do{
                Utils.lock(nodeLocks, current);
                found = true;
                A = engine.get(current, nodeSerializer);
                int pos = findChildren(v, A.keys());
                if(pos<A.keys().length-1 &&  v!=null && A.keys()[pos]!=null &&
                        0==comparator.compare(v,A.keys()[pos])){

                    Object oldVal =   (hasValues? A.vals()[pos] : Utils.EMPTY_STRING);
                    if(putOnlyIfAbsent){
                        //is not absent, so quit
                        Utils.unlock(nodeLocks, current);
                        Utils.assertNoLocks(nodeLocks);
                        V ret =  valExpand(oldVal);
                        notify(v,ret, value2);
                        return ret;
                    }
                    //insert new
                    Object[] vals = null;
                    if(hasValues){
                        vals = Arrays.copyOf(A.vals(), A.vals().length);
                        vals[pos] = value;
                    }

                    A = new LeafNode(Arrays.copyOf(A.keys(), A.keys().length), vals, ((LeafNode)A).next);
                    engine.update(current, A, nodeSerializer);
                    //already in here
                    Utils.unlock(nodeLocks, current);
                    Utils.assertNoLocks(nodeLocks);
                    V ret =  valExpand(oldVal);
                    notify(v,ret, value2);
                    return ret;
                }

                if(A.highKey() != null && comparator.compare(v, A.highKey())>0){
                    //follow link until necessary
                    Utils.unlock(nodeLocks, current);
                    found = false;
                    int pos2 = findChildren(v, A.keys());
                    while(A!=null && pos2 == A.keys().length){
                        //TODO lock?
                        long next = A.next();

                        if(next==0) break;
                        current = next;
                        A = engine.get(current, nodeSerializer);
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
                    engine.update(current, n, nodeSerializer);
                }else{
                    if(p==0)
                        throw new InternalError();
                    long[] child = Utils.arrayLongPut(A.child(), pos, p);
                    DirNode d = new DirNode(keys, child);
                    engine.update(current, d, nodeSerializer);
                }

                Utils.unlock(nodeLocks, current);
                Utils.assertNoLocks(nodeLocks);
                notify(v,  null, value2);
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
                long q = engine.put(B, nodeSerializer);
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
                engine.update(current, A, nodeSerializer);

                if(!isRoot){
                    Utils.unlock(nodeLocks, current);
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


                    long newRootRecid = engine.put(R, nodeSerializer);

                    //TODO tree root locking
                    engine.update(rootRecidRef, newRootRecid, Serializer.LONG_SERIALIZER);


                    //TODO update tree levels
                    Utils.unlock(nodeLocks, current);
                    Utils.assertNoLocks(nodeLocks);
                    notify(v, null, value2);
                    return null;
                }
            }
        }
        }catch(RuntimeException e){
            Utils.unlockAll(nodeLocks);
            throw e;
        }catch(Exception e){
            Utils.unlockAll(nodeLocks);
            throw new RuntimeException(e);
        }
    }


    class BTreeIterator{
        LeafNode currentLeaf;
        K lastReturnedKey;
        int currentPos;

        BTreeIterator(){
            //find left-most leaf
            final long rootRecid = engine.get(rootRecidRef, Serializer.LONG_SERIALIZER);
            BNode node = engine.get(rootRecid, nodeSerializer);
            while(!node.isLeaf()){
                node = engine.get(node.child()[0], nodeSerializer);
            }
            currentLeaf = (LeafNode) node;
            currentPos = 1;

            while(currentLeaf.keys.length==2){
                //follow link until leaf is not empty
                if(currentLeaf.next == 0){
                    currentLeaf = null;
                    return;
                }
                currentLeaf = (LeafNode) engine.get(currentLeaf.next, nodeSerializer);
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
                currentLeaf = (LeafNode) engine.get(currentLeaf.next, nodeSerializer);
                while(currentLeaf.keys.length==2){
                    if(currentLeaf.next ==0){
                        currentLeaf = null;
                        currentPos=-1;
                        return;
                    }
                    currentLeaf = (LeafNode) engine.get(currentLeaf.next, nodeSerializer);
                }
            }
        }
    }

    @Override
	public V remove(Object key) {
        return remove2(key, null);
    }

    private V remove2(Object key, Object value) {
        final long rootRecid = engine.get(rootRecidRef, Serializer.LONG_SERIALIZER);
        long current = rootRecid;
        BNode A = engine.get(current, nodeSerializer);
        while(!A.isLeaf()){
            current = nextDir((DirNode) A, key);
            A = engine.get(current, nodeSerializer);
        }

        try{
        while(true){

            Utils.lock(nodeLocks, current);
            A = engine.get(current, nodeSerializer);
            int pos = findChildren(key, A.keys());
            if(pos<A.keys().length&& key!=null && A.keys()[pos]!=null &&
                    0==comparator.compare(key,A.keys()[pos])){
                //delete from node
                Object oldVal =  hasValues? A.vals()[pos] : Utils.EMPTY_STRING;
                oldVal = valExpand(oldVal);
                if(value!=null && !value.equals(oldVal)){
                    Utils.unlock(nodeLocks, current);
                    return null;
                }
                //check for last node which was already deleted
                if(pos == A.keys().length-1 && value == null){
                    Utils.unlock(nodeLocks, current);
                    return null;
                }

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
                engine.update(current, A, nodeSerializer);
                Utils.unlock(nodeLocks, current);
                notify((K)key, (V)oldVal, null);
                return (V) oldVal;
            }else{
                Utils.unlock(nodeLocks, current);
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
            Utils.unlockAll(nodeLocks);
            throw e;
        }catch(Exception e){
            Utils.unlockAll(nodeLocks);
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
        if(value instanceof ValRef) throw new InternalError();
        return new SimpleImmutableEntry<K, V>((K)key,  (V)value);
    }


    @Override
    public boolean isEmpty() {
        return !keyIterator().hasNext();
    }

    @Override
    public int size(){
        if(counter!=null)
            return (int) counter.get(); //TODO larger then MAX_INT

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
        if(key == null || (value == null)) throw new NullPointerException();
        return remove2(key, value)!=null;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        if(key == null || oldValue == null || newValue == null ) throw new NullPointerException();

        long rootRecid = engine.get(rootRecidRef, Serializer.LONG_SERIALIZER);
        long current = rootRecid;
        BNode node = engine.get(current, nodeSerializer);
        //dive until leaf is found
        while(!node.isLeaf()){
            current = nextDir((DirNode) node, key);
            node = engine.get(current, nodeSerializer);
        }

        Utils.lock(nodeLocks, current);
        LeafNode leaf = (LeafNode) engine.get(current, nodeSerializer);

        int pos = findChildren(key, node.keys());
        try{
        while(pos==leaf.keys.length){
            //follow leaf link until necessary
            Utils.lock(nodeLocks, leaf.next);
            Utils.unlock(nodeLocks, current);
            current = leaf.next;
            leaf = (LeafNode) engine.get(current, nodeSerializer);
            pos = findChildren(key, node.keys());
        }

        boolean ret = false;
        if( key!=null && leaf.keys()[pos]!=null &&
                0==comparator.compare(key,leaf.keys[pos])){
            Object val  = leaf.vals[pos];
            val = valExpand(val);
            if(oldValue.equals(val)){ //TODO use comparator here?
                Object[] vals = Arrays.copyOf(leaf.vals, leaf.vals.length);
                notify(key, oldValue, newValue);
                if(valsOutsideNodes){
                    long recid = engine.put(newValue, valueSerializer);
                    newValue = (V) new ValRef(recid);
                }
                vals[pos] = newValue;
                leaf = new LeafNode(Arrays.copyOf(leaf.keys, leaf.keys.length), vals, leaf.next);

                engine.update(current, leaf, nodeSerializer);

                ret = true;
            }
        }
        Utils.unlock(nodeLocks, current);
        return ret;
        }catch(RuntimeException e){
            Utils.unlockAll(nodeLocks);
            throw e;
        }catch(Exception e){
            Utils.unlockAll(nodeLocks);
            throw new RuntimeException(e);
        }
    }

    @Override
    public V replace(K key, V value) {
        if(key == null || value == null) throw new NullPointerException();
        final long rootRecid = engine.get(rootRecidRef,Serializer.LONG_SERIALIZER);
        long current = rootRecid;
        BNode node = engine.get(current, nodeSerializer);
        //dive until leaf is found
        while(!node.isLeaf()){
            current = nextDir((DirNode) node, key);
            node = engine.get(current, nodeSerializer);
        }

        Utils.lock(nodeLocks, current);
        LeafNode leaf = (LeafNode) engine.get(current, nodeSerializer);

        try{
        int pos = findChildren(key, node.keys());
        while(pos==leaf.keys.length){
            //follow leaf link until necessary
            Utils.lock(nodeLocks, leaf.next);
            Utils.unlock(nodeLocks, current);
            current = leaf.next;
            leaf = (LeafNode) engine.get(current, nodeSerializer);
            pos = findChildren(key, node.keys());
        }

        Object ret = null;
        if( key!=null && leaf.keys()[pos]!=null &&
                0==comparator.compare(key,leaf.keys[pos])){
            Object[] vals = Arrays.copyOf(leaf.vals, leaf.vals.length);
            Object oldVal = vals[pos];
            ret =  valExpand(oldVal);
            notify(key, (V)ret, value);
            if(valsOutsideNodes && value!=null){
                long recid = engine.put(value, valueSerializer);
                value = (V) new ValRef(recid);
            }
            vals[pos] = value;
            leaf = new LeafNode(Arrays.copyOf(leaf.keys, leaf.keys.length), vals, leaf.next);
            engine.update(current, leaf, nodeSerializer);


        }
        Utils.unlock(nodeLocks, current);
        return (V)ret;
        }catch(RuntimeException e){
            Utils.unlockAll(nodeLocks);
            throw e;
        }catch(Exception e){
            Utils.unlockAll(nodeLocks);
            throw new RuntimeException(e);
        }

    }


    @Override
    public Comparator<? super K> comparator() {
        return comparator;
    }


    @Override
    public Map.Entry<K,V> firstEntry() {
        final long rootRecid = engine.get(rootRecidRef, Serializer.LONG_SERIALIZER);
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
        return makeEntry(l.keys[1], hasValues?valExpand(l.vals[1]):Utils.EMPTY_STRING);
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
        final long rootRecid = engine.get(rootRecidRef, Serializer.LONG_SERIALIZER);
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
                            makeEntry(key2, hasValues?valExpand(n.vals()[i]):Utils.EMPTY_STRING);
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
        final long rootRecid = engine.get(rootRecidRef, Serializer.LONG_SERIALIZER);
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
                return lastEntryRecur(n2);
            }

            //iterate over keys to find last non null key
            for(int i=n.keys().length-1; i>=0;i--){
                Object k = n.keys()[i];
                if(k!=null) {
                    return makeEntry(k, hasValues?valExpand(n.vals()[i]):Utils.EMPTY_STRING);
                }
            }
        }else{
            //dir node, dive deeper
            for(int i=n.child().length-1; i>=0;i--){
                long childRecid = n.child()[i];
                if(childRecid==0) continue;
                BNode n2 = engine.get(childRecid, nodeSerializer);
                Entry<K,V> ret = lastEntryRecur(n2);
                if(ret!=null) return ret;
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

    protected Entry<K, V> findLarger(K key, boolean inclusive) {
        if(key==null) return null;
        K v = (K) key;
        final long rootRecid = engine.get(rootRecidRef, Serializer.LONG_SERIALIZER);
        long current = rootRecid;
        BNode A = engine.get(current, nodeSerializer);

        //dive until  leaf
        while(!A.isLeaf()){
            current = nextDir((DirNode) A, v);
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
                    return makeEntry(leaf.keys[i], hasValues?valExpand(leaf.vals[i]):Utils.EMPTY_STRING);
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
        return get(key)!=null;
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
        return new BTreeKeyIterator();
    }

    Iterator<V> valueIterator() {
        return new BTreeValueIterator();
    }

    Iterator<Map.Entry<K,V>> entryIterator() {
        return new BTreeEntryIterator();
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
        throw new UnsupportedOperationException("descending not supported");
    }

    @Override
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
            else
                return ((BTreeMap.SubMap<E,Object>)m).keyIterator();
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
                return m.put(k,  Utils.EMPTY_STRING) == null;
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
            else
                return ((SubMap<K1,V1>)m).entryIterator();
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
		public SubMap<K,V> descendingMap() {
            throw new UnsupportedOperationException("Descending not supported");
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


            @Override
			public boolean hasNext() {
                return current!=null;
            }


            public void advance() {
                if(current==null) throw new NoSuchElementException();
                last = current;
                current = SubMap.this.higherEntry(current.getKey());
            }

            @Override
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


    /**
     * Make readonly snapshot view of current Map. Snapshot is immutable and not affected by modifications made by other threads.
     * Useful if you need consistent view on Map.
     * <p>
     * Maintaining snapshot have some overhead, underlying Engine is closed after Map view is GCed.
     * Please make sure to release reference to this Map view, so snapshot view can be garbage collected.
     *
     * @return snapshot
     */
    public NavigableMap<K,V> snapshot(){
        Engine snapshot = SnapshotEngine.createSnapshotFor(engine);

        return new BTreeMap<K, V>(snapshot,treeRecid, defaultSerializer);
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
        if(oldValue instanceof ValRef) throw new InternalError();
        if(newValue instanceof ValRef) throw new InternalError();

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

    public void printTreeStructure() {
        final long rootRecid = engine.get(rootRecidRef, Serializer.LONG_SERIALIZER);
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



}
