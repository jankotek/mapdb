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
 * <p> 
 * Insertion, removal,
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
 *<p> 
 * All <tt>Map.Entry</tt> pairs returned by methods in this class
 * and its views represent snapshots of mappings at the time they were
 * produced. They do <em>not</em> support the <tt>Entry.setValue</tt>
 * method. (Note however that it is possible to change mappings in the
 * associated map using <tt>put</tt>, <tt>putIfAbsent</tt>, or
 * <tt>replace</tt>, depending on exactly which effect you need.)
 *<p> 
 * This collection has optional size counter. If this is enabled Map size is
 * kept in {@link Atomic.Long} variable. Keeping counter brings considerable
 * overhead on inserts and removals.
 * If the size counter is not enabled the <tt>size</tt> method is <em>not</em> a constant-time operation.
 * Determining the current number of elements requires a traversal of the elements.
 *<p> 
 * Additionally, the bulk operations <tt>putAll</tt>, <tt>equals</tt>, and
 * <tt>clear</tt> are <em>not</em> guaranteed to be performed
 * atomically. For example, an iterator operating concurrently with a
 * <tt>putAll</tt> operation might view only some of the added
 * elements. NOTE: there is an optional 
 *<p> 
 * This class and its views and iterators implement all of the
 * <em>optional</em> methods of the {@link Map} and {@link Iterator}
 * interfaces. Like most other concurrent collections, this class does
 * <em>not</em> permit the use of <tt>null</tt> keys or values because some
 * null return values cannot be reliably distinguished from the absence of
 * elements.
 *<p> 
 * Theoretical design of BTreeMap is based on <a href="http://www.cs.cornell.edu/courses/cs4411/2009sp/blink.pdf">paper</a>
 * from Philip L. Lehman and S. Bing Yao. More practical aspects of BTreeMap implementation are based on <a href="http://www.doc.ic.ac.uk/~td202/">notes</a>
 * and <a href="http://www.doc.ic.ac.uk/~td202/btree/">demo application</a> from Thomas Dinsdale-Young.
 * B-Linked-Tree used here does not require locking for read. Updates and inserts locks only one, two or three nodes.
 <p> 
 * This B-Linked-Tree structure does not support removal well, entry deletion does not collapse tree nodes. Massive
 * deletion causes empty nodes and performance lost. There is workaround in form of compaction process, but it is not
 * implemented yet.
 *
 * @author Jan Kotek
 * @author some parts by Doug Lea and JSR-166 group
 *
 * TODO links to BTree papers are not working anymore.
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class BTreeMap<K,V> extends AbstractMap<K,V>
        implements ConcurrentNavigableMap<K,V>, Bind.MapWithModificationListener<K,V>{


    protected static final Object EMPTY = new Object();


    /** recid under which reference to rootRecid is stored */
    protected final long rootRecidRef;

    /** Serializer used to convert keys from/into binary form. */
    protected final BTreeKeySerializer keySerializer;

    /** Serializer used to convert keys from/into binary form*/
    protected final Serializer<V> valueSerializer;

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

    private final EntrySet entrySet;

    private final Values values = new Values(this);

    private final ConcurrentNavigableMap<K,V> descendingMap = new DescendingMap(this, null,true, null, false);

    protected final Atomic.Long counter;

    protected final int numberOfNodeMetas;

    /** hack used for DB Catalog*/
    protected static SortedMap<String, Object> preinitCatalog(DB db) {

        Long rootRef = db.getEngine().get(Engine.RECID_NAME_CATALOG, Serializer.LONG);

        BTreeKeySerializer keyser = BTreeKeySerializer.STRING;
        //$DELAY$
        if(rootRef==null){
            if(db.getEngine().isReadOnly())
                return Collections.unmodifiableSortedMap(new TreeMap<String, Object>());

            NodeSerializer rootSerializer = new NodeSerializer(false,BTreeKeySerializer.STRING,
                    db.getDefaultSerializer(), 0);
            BNode root = new LeafNode(keyser.emptyKeys(), true,true,false, new Object[]{}, 0);
            rootRef = db.getEngine().put(root, rootSerializer);
            //$DELAY$
            db.getEngine().update(Engine.RECID_NAME_CATALOG,rootRef, Serializer.LONG);
            db.getEngine().commit();
        }
        return new BTreeMap<String, Object>(db.engine,Engine.RECID_NAME_CATALOG,32,false,0,
                keyser,
                db.getDefaultSerializer(),
                0);
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

    protected static final class ValRefSerializer extends Serializer<ValRef>{

        @Override
        public void serialize(DataOutput out, ValRef value) throws IOException {
            DataIO.packLong(out,value.recid);
        }

        @Override
        public ValRef deserialize(DataInput in, int available) throws IOException {
            return new ValRef(DataIO.unpackLong(in));
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public boolean equals(ValRef a1, ValRef a2) {
            throw new IllegalAccessError();
        }

        @Override
        public int hashCode(ValRef valRef) {
            throw new IllegalAccessError();
        }
    }

    /** common interface for BTree node */
    public abstract static class BNode{

        static final int LEFT_MASK = 1;
        static final int RIGHT_MASK = 1<<1;
        static final int TOO_LARGE_MASK = 1<<2;

        final Object keys;
        final byte flags;


        public BNode(Object keys, boolean leftEdge, boolean rightEdge, boolean tooLarge){
            this.keys = keys;

            this.flags = (byte)(
                           (leftEdge?LEFT_MASK:0)|
                           (rightEdge?RIGHT_MASK:0)|
                           (tooLarge?TOO_LARGE_MASK:0)
            );
        }




        final public Object key(BTreeKeySerializer keyser, int pos) {
            if(isLeftEdge()){
                if(pos--==0)
                    return null;
            }

            if(pos==keyser.length(keys) && isRightEdge())
                return null;
            return keyser.getKey(keys,pos);
        }

        final public  int keysLen(BTreeKeySerializer keyser) {
            return keyser.length(keys) + leftEdgeInc() + rightEdgeInc();
        }

        final public boolean isLeftEdge(){
            return (flags&LEFT_MASK)!=0;
        }


        final public boolean isRightEdge(){
            return (flags&RIGHT_MASK)!=0;
        }


        /** @return 1 if is left edge, or 0*/
        final public int leftEdgeInc(){
            return flags&LEFT_MASK;
        }


        /** @return 1 if is right edge, or 0*/
        final public int rightEdgeInc(){
            return (flags&RIGHT_MASK)>>>1;
        }


        final public boolean isTooLarge(){
            return (flags&TOO_LARGE_MASK)!=0;
        }

        public abstract boolean isLeaf();
        public abstract Object[] vals();

        final public Object highKey(BTreeKeySerializer keyser) {
            if(isRightEdge())
                return null;
            return keyser.getKey(keys,keyser.length(keys)-1);
        }


        public abstract long[] child();
        public abstract long next();

        public final int compare(final BTreeKeySerializer keyser, int pos1, int pos2){
            if(pos1==pos2)
                return 0;
            //$DELAY$
            if(isLeftEdge()){
                //first position is negative infinity, so everything else is bigger
                //first keys is missing in array, so adjust positions
                if(pos1--==0)
                    return -1;
                if(pos2--==0)
                    return 1;
            }
            //$DELAY$
            if(isRightEdge()){
                int keysLen = keyser.length(keys);
                //last position is positive infinity, so everything else is smaller
                if(pos1==keysLen)
                    return 1;
                if(pos2==keysLen)
                    return -1;
            }

            return keyser.compare(keys,pos1,pos2);
        }

        public final int compare(final BTreeKeySerializer keyser, int pos, Object second){
            if(isLeftEdge()) {
                //first position is negative infinity, so everything else is bigger
                //first keys is missing in array, so adjust positions
                if (pos-- == 0)
                    return -1;
            }
            //$DELAY$
            if(isRightEdge() && pos==keyser.length(keys)){
                //last position is positive infinity, so everything else is smaller
                return 1;
            }
            return keyser.compare(keys,pos,second);
        }


        public void checkStructure(BTreeKeySerializer keyser){
            //check all keys are sorted;
            if(keyser==null)
                return;

            int keylen = keyser.length(keys);
            int end = keylen-2+rightEdgeInc();
            if(end>1){
                for(int i = 1;i<=end;i++){
                    if(keyser.compare(keys,i-1, i)>=0)
                        throw new AssertionError("keys are not sorted: "+Arrays.toString(keyser.keysToArray(keys)));
                }
            }
            //check last key is sorted or null
            if(!isRightEdge() && keylen>2){
                if(keyser.compare(keys,keylen-2, keylen-1)>0){
                    throw new AssertionError("Last key is not sorted: "+Arrays.toString(keyser.keysToArray(keys)));
                }
            }
        }

        public abstract BNode copyAddKey(BTreeKeySerializer keyser, int pos, Object newKey, long newChild, Object newValue);

        public abstract BNode copySplitRight(BTreeKeySerializer keyser, int splitPos);

        public abstract BNode copySplitLeft(BTreeKeySerializer keyser, int splitPos, long newNext);
    }

    public final static class DirNode extends BNode{
        final long[] child;

        DirNode(Object keys, boolean leftEdge, boolean rightEdge, boolean tooLarge, long[] child) {
            super(keys, leftEdge, rightEdge, tooLarge);
            this.child = child;

            if(CC.PARANOID)
                checkStructure(null);
        }



        @Override public boolean isLeaf() { return false;}

        @Override public Object[] vals() { return null;}

        @Override public long[] child() { return child;}

        @Override public long next() {return child[child.length-1];}

        @Override public String toString(){
            return "Dir("+leftEdgeInc()+"-"+rightEdgeInc()+"-K"+Fun.toString(keys)+", C"+Arrays.toString(child)+")";
        }


        @Override
        public void checkStructure(BTreeKeySerializer keyser) {
            super.checkStructure(keyser);

            if(keyser!=null && child.length!=keysLen(keyser))
                throw new AssertionError();

            if((isRightEdge() != (child[child.length-1]==0)))
                throw new AssertionError();

        }

        @Override
        public DirNode copyAddKey(BTreeKeySerializer keyser, int pos, Object newKey, long newChild, Object newValue) {
            Object keys2 = keyser.putKey(keys, pos-leftEdgeInc(), newKey);

            long[] child2 = BTreeMap.arrayLongPut(child,pos,newChild);
            //$DELAY$
            return new DirNode(keys2, isLeftEdge(),isRightEdge(),false,child2);
        }

        @Override
        public DirNode copySplitRight(BTreeKeySerializer keyser, int splitPos) {
            int keylen = keyser.length(keys);
            Object keys2 = keyser.copyOfRange(keys,splitPos-leftEdgeInc(),keylen);
            //$DELAY$
            long[] child2 = Arrays.copyOfRange(child,splitPos,child.length);
            //$DELAY$
            return new DirNode(keys2,false,isRightEdge(),false,child2);
        }

        @Override
        public DirNode copySplitLeft(BTreeKeySerializer keyser, int splitPos, long newNext) {
            Object keys2 = keyser.copyOfRange(keys,0,splitPos+1 - leftEdgeInc());
            //$DELAY$
            long[] child2 = Arrays.copyOf(child, splitPos+1);
            child2[splitPos] = newNext;
            //$DELAY$
            return new DirNode(keys2,isLeftEdge(),false,false,child2);
        }

    }


    public final static class LeafNode extends BNode{
        final Object[] vals;
        final long next;

        LeafNode(Object keys, boolean leftEdge, boolean rightEdge, boolean tooLarge, Object[] vals, long next) {
            super(keys,leftEdge, rightEdge, tooLarge);
            this.vals = vals;
            this.next = next;

            if(CC.PARANOID)
                checkStructure(null);
        }

        @Override public boolean isLeaf() { return true;}


        @Override public Object[] vals() { return vals;}


        @Override public long[] child() { return null;}
        @Override public long next() {return next;}

        @Override public String toString(){
            return "Leaf("+leftEdgeInc()+"-"+rightEdgeInc()+"-"+"K"+Fun.toString(keys)+", V"+Arrays.toString(vals)+", L="+next+")";
        }

        @Override
        public void checkStructure(BTreeKeySerializer keyser) {
            super.checkStructure(keyser);
            if((next==0)!=isRightEdge()){
                throw new AssertionError("Next link inconsistent: "+this);
            }

            if(keyser!=null && (keysLen(keyser) != vals.length+2)) {
                throw new AssertionError("Inconsistent vals size: " + this);
            }
            //$DELAY$
            for (Object val : vals) {
                if (val == null)
                    throw new AssertionError("Val is null: " + this);
            }

        }

        @Override
        public LeafNode copyAddKey(BTreeKeySerializer keyser, int pos, Object newKey, long newChild, Object newValue) {
            Object keys2 = keyser.putKey(keys, pos - leftEdgeInc(), newKey);
            //$DELAY$
            Object[] vals2 = arrayPut(vals, pos-1, newValue);
            //$DELAY$
            return new LeafNode(keys2, isLeftEdge(), isRightEdge(), false, vals2,next);
        }

        @Override
        public LeafNode copySplitRight(BTreeKeySerializer keyser, int splitPos) {
            int keylen = keyser.length(keys);
            Object keys2 = keyser.copyOfRange(keys, splitPos-leftEdgeInc(), keylen);
            //$DELAY$
            Object[] vals2 = Arrays.copyOfRange(vals, splitPos, vals.length);
            //$DELAY$
            return new LeafNode(keys2,false, isRightEdge(), false, vals2, next);
        }

        @Override
        public LeafNode copySplitLeft(BTreeKeySerializer keyser, int splitPos, long newNext) {
            int keypos =splitPos+1-leftEdgeInc();
            Object keys2 = keyser.copyOfRange(keys,0,keypos);
            //clone end value
            Object endkey = keyser.getKey(keys2,keypos-1);
            keys2 = keyser.putKey(keys2,keypos,endkey);
            //$DELAY$
            Object[] vals2 = Arrays.copyOf(vals, splitPos);
            //$DELAY$
            //TODO check high/low keys overlap
            return new LeafNode(keys2, isLeftEdge(), false, false, vals2, newNext);
        }

        public LeafNode copyChangeValue(int pos, Object value) {
            Object[] vals2 = Arrays.copyOf(vals,vals.length);
            vals2[pos-1] = value;
            //$DELAY$
            return new LeafNode(keys, isLeftEdge(), isRightEdge(), false, vals2, next);
        }

        public LeafNode copyRemoveKey(BTreeKeySerializer keyser, int pos) {
            int keyPos = pos -leftEdgeInc();
            Object keys2 = keyser.deleteKey(keys,keyPos);
            //$DELAY$
            Object[] vals2 = new Object[vals.length-1];
            System.arraycopy(vals,0,vals2, 0, pos-1);
            System.arraycopy(vals, pos, vals2, pos-1, vals2.length-(pos-1));
            //$DELAY$
            return new LeafNode(keys2, isLeftEdge(), isRightEdge(), false, vals2, next);
        }

        public LeafNode copyClear(BTreeKeySerializer keyser) {
            Object[] keys2 = new Object[2-leftEdgeInc()-rightEdgeInc()];
            if(!isLeftEdge())
                keys2[0] = key(keyser,0);
            //$DELAY$
            if(!isRightEdge())
                keys2[1-leftEdgeInc()] = highKey(keyser);
            //$DELAY$
            return new LeafNode (keyser.arrayToKeys(keys2), isLeftEdge(), isRightEdge(), false, new Object[]{}, next);
        }
    }


    protected final Serializer<BNode> nodeSerializer;

    protected static final class NodeSerializer<A,B> extends Serializer<BNode>{

        protected static final int LEAF_MASK = 1<<15;
        protected static final int LEFT_SHIFT = 14;
        protected static final int LEFT_MASK = 1<< LEFT_SHIFT;
        protected static final int RIGHT_SHIFT = 13;
        protected static final int RIGHT_MASK = 1<< RIGHT_SHIFT;
        protected static final int SIZE_MASK = RIGHT_MASK - 1;


        protected final boolean hasValues;
        protected final boolean valsOutsideNodes;
        protected final BTreeKeySerializer keySerializer;
        protected final Serializer<Object> valueSerializer;
        protected final int numberOfNodeMetas;

        public NodeSerializer(boolean valsOutsideNodes, BTreeKeySerializer keySerializer, Serializer valueSerializer,  int numberOfNodeMetas) {
            if(CC.PARANOID && ! (keySerializer!=null))
                throw new AssertionError();
            this.hasValues = valueSerializer!=null;
            this.valsOutsideNodes = valsOutsideNodes;
            this.keySerializer = keySerializer;
            this.valueSerializer = valsOutsideNodes? new ValRefSerializer() : valueSerializer;
            this.numberOfNodeMetas = numberOfNodeMetas;
        }

        @Override
        public void serialize(DataOutput out, BNode value) throws IOException {
            final boolean isLeaf = value.isLeaf();

            //check node integrity in paranoid mode
            if(CC.PARANOID){
                value.checkStructure(keySerializer);
            }
            //$DELAY$

            final int header =
                    (isLeaf ? LEAF_MASK : 0) |
                    (value.isLeftEdge() ? LEFT_MASK : 0) |
                    (value.isRightEdge() ? RIGHT_MASK : 0) |
                    value.keysLen(keySerializer);

            out.writeShort(header);
            //$DELAY$

            //write node metas, right now this is ignored, but in future it could be used for counted btrees or aggregations
            for(int i=0;i<numberOfNodeMetas;i++){
                DataIO.packLong(out,0);
            }
            //$DELAY$
            //longs go first, so it is possible to reconstruct tree without serializer
            if(isLeaf){
                DataIO.packLong(out, ((LeafNode) value).next);
            }else{
                for(long child : ((DirNode)value).child)
                    DataIO.packLong(out, child);
            }


            if(keySerializer.length(value.keys)>0)
                keySerializer.serialize(out,value.keys);
            //$DELAY$
            if(isLeaf){
                if(hasValues){
                    for(Object val:value.vals()){
                       valueSerializer.serialize(out,  val);
                    }
                }else{
                    serializeSetFlags(out, value);
                }
            }
        }

        public void serializeSetFlags(DataOutput out, BNode value) throws IOException {
            //write bits if values are null
            boolean[] bools = new boolean[value.vals().length];
            for(int i=0;i<bools.length;i++){
                bools[i] = value.vals()[i]!=null;
            }
            //$DELAY$
            //pack
            byte[] bb = SerializerBase.booleanToByteArray(bools);
            out.write(bb);
        }

        @Override
        public BNode deserialize(DataInput in, int available) throws IOException {
            final int header = in.readUnsignedShort();
            final int size = header & SIZE_MASK;

            //read node metas, right now this is ignored, but in future it could be used for counted btrees or aggregations
            for(int i=0;i<numberOfNodeMetas;i++){
                DataIO.unpackLong(in);
            }
            //$DELAY$
            //first bite indicates leaf
            final boolean isLeaf = ((header& LEAF_MASK) != 0);
            final int left = (header& LEFT_MASK) >>LEFT_SHIFT;
            final int right = (header& RIGHT_MASK) >>RIGHT_SHIFT;

            BNode node;
            if(isLeaf){
                node = deserializeLeaf(in, size, left, right);
            }else{
                node = deserializeDir(in, size, left, right);
            }
            //$DELAY$
            if(CC.PARANOID){
                node.checkStructure(keySerializer);
            }
            return node;
        }

        private BNode deserializeDir(final DataInput in, final int size, final int left, final int right) throws IOException {
            final long[] child = new long[size];
            for(int i=0;i<size;i++)
                child[i] = DataIO.unpackLong(in);
            int keysize = size - left- right;
            //$DELAY$
            final Object keys = keysize==0?
                    keySerializer.emptyKeys():
                    keySerializer.deserialize(in, keysize);
            //$DELAY$
            return new DirNode(keys, left!=0, right!=0, false ,child);
        }

        private BNode deserializeLeaf(final DataInput in, final int size, final int left, final int right) throws IOException {
            final long next = DataIO.unpackLong(in);
            int keysize = size - left- right;
            //$DELAY$
            final Object keys = keysize==0?
                    keySerializer.emptyKeys():
                    keySerializer.deserialize(in, keysize);
            //$DELAY$
            Object[] vals = new Object[size-2];
            //$DELAY$
            if(hasValues){
                for(int i=0;i<vals.length;i++){
                    vals[i] = valueSerializer.deserialize(in, -1);
                }
            }else{
                deserSetVals(in, vals);
            }
            return new LeafNode(keys,  left!=0, right!=0, false , vals, next);
        }

        private void deserSetVals(DataInput in, Object[] vals) throws IOException {
                //restore values which were deleted
                boolean[] bools = SerializerBase.readBooleanArray(vals.length, in);
            //$DELAY$
                for(int i=0;i<bools.length;i++){
                    if(bools[i])
                        vals[i]=EMPTY;
                }
        }

        @Override
        public boolean isTrusted() {
            return keySerializer.isTrusted() && valueSerializer.isTrusted();
        }
    }


    /** Constructor used to create new BTreeMap.
     *
     * @param engine used for persistence
     * @param rootRecidRef reference to root recid
     * @param maxNodeSize maximal BTree Node size. Node will split if number of entries is higher
     * @param valsOutsideNodes Store Values outside of BTree Nodes in separate record?
     * @param counterRecid recid under which {@code Atomic.Long} is stored, or {@code 0} for no counter
     * @param keySerializer Serializer used for keys. May be null for default value.
     * @param valueSerializer Serializer used for values. May be null for default value
     * @param numberOfNodeMetas number of meta records associated with each BTree node
     */
    public BTreeMap(Engine engine, long rootRecidRef,int maxNodeSize, boolean valsOutsideNodes, long counterRecid,
                    BTreeKeySerializer keySerializer, Serializer<V> valueSerializer,
                    int numberOfNodeMetas) {
        if(maxNodeSize%2!=0) throw new IllegalArgumentException("maxNodeSize must be dividable by 2");
        if(maxNodeSize<6) throw new IllegalArgumentException("maxNodeSize too low");
        if((maxNodeSize& NodeSerializer.SIZE_MASK) !=maxNodeSize)
            throw new IllegalArgumentException("maxNodeSize too high");
        if(rootRecidRef<=0||counterRecid<0 || numberOfNodeMetas<0) throw new IllegalArgumentException();
        if(keySerializer==null) throw new NullPointerException();
//        SerializerBase.assertSerializable(keySerializer); //TODO serializer serialization
//        SerializerBase.assertSerializable(valueSerializer);

        this.rootRecidRef = rootRecidRef;
        this.hasValues = valueSerializer!=null;
        this.valsOutsideNodes = valsOutsideNodes;
        this.engine = engine;
        this.maxNodeSize = maxNodeSize;
        this.numberOfNodeMetas = numberOfNodeMetas;

        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        entrySet = new EntrySet(this, valueSerializer);

        this.nodeSerializer = new NodeSerializer(valsOutsideNodes,keySerializer,valueSerializer,numberOfNodeMetas);

        this.keySet = new KeySet(this, hasValues);
        //$DELAY$

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
            //$DELAY$
            BNode n= engine.get(r,nodeSerializer);
            leftEdges2.add(r);
            if(n.isLeaf()) break;
            r = n.child()[0];
        }
        //$DELAY$
        Collections.reverse(leftEdges2);
        leftEdges = new CopyOnWriteArrayList<Long>(leftEdges2);
    }

    /** creates empty root node and returns recid of its reference*/
    static protected long createRootRef(Engine engine, BTreeKeySerializer keySer, Serializer valueSer, int numberOfNodeMetas){
        final LeafNode emptyRoot = new LeafNode(keySer.emptyKeys(), true,true, false,new Object[]{}, 0);
        //empty root is serializer simpler way, so we can use dummy values
        long rootRecidVal = engine.put(emptyRoot,  new NodeSerializer(false,keySer, valueSer, numberOfNodeMetas));
        return engine.put(rootRecidVal,Serializer.LONG);
    }





    @Override
	public V get(Object key){
    	return (V) get(key, true);
    }

    protected Object get(Object key, boolean expandValue) {
        if(key==null) throw new NullPointerException();
        K v = (K) key;
        long current = engine.get(rootRecidRef, Serializer.LONG); //get root
        //$DELAY$
        BNode A = engine.get(current, nodeSerializer);

        //dive until  leaf
        while(!A.isLeaf()){
            //$DELAY$
            current = nextDir((DirNode) A, v);
            //$DELAY$
            A = engine.get(current, nodeSerializer);
        }

        for(;;) {
            int pos = keySerializer.findChildren2(A, key);
            //$DELAY$
            if (pos > 0 && pos != A.keysLen(keySerializer) - 1) {
                //found
                Object val =  A.vals()[pos - 1];
                //$DELAY$
                if(expandValue)
                    val = valExpand(val);
                return val;
            } else if (pos <= 0 && -pos - 1 != A.keysLen(keySerializer) - 1) {
                //$DELAY$
                //not found
                return null;
            } else {
                //move to next link
                current = A.next();
                //$DELAY$
                if (current == 0) {
                    return null;
                }
                A = engine.get(current, nodeSerializer);
            }
        }

    }

    protected V valExpand(Object ret) {
        if(valsOutsideNodes && ret!=null) {
            long recid = ((ValRef)ret).recid;
            //$DELAY$
            ret = engine.get(recid, valueSerializer);
        }
        return (V) ret;
    }

    protected final long nextDir(DirNode d, Object key) {
        int pos = keySerializer.findChildren(d, key) - 1;
        //$DELAY$
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

        V value = value2;
        if(valsOutsideNodes){
            long recid = engine.put(value2, valueSerializer);
            //$DELAY$
            value = (V) new ValRef(recid);
        }

        int stackPos = -1;
        long[] stackVals = new long[4];

        final long rootRecid = engine.get(rootRecidRef, Serializer.LONG);
        long current = rootRecid;
        //$DELAY$
        BNode A = engine.get(current, nodeSerializer);
        while(!A.isLeaf()){
            //$DELAY$
            long t = current;
            current = nextDir((DirNode) A, v);
            //$DELAY$
            if(CC.PARANOID && ! (current>0) )
                throw new AssertionError(A);
            //if is not link
            if (current != A.next()) {
                //stack push t
                stackPos++;
                if(stackVals.length == stackPos) //grow if needed
                    stackVals = Arrays.copyOf(stackVals, stackVals.length*2);
                //$DELAY$
                stackVals[stackPos] = t;
            }
            //$DELAY$
            A = engine.get(current, nodeSerializer);
        }
        int level = 1;

        long p=0;
        try{
        while(true){
            //$DELAY$
            boolean found;
            do{
                //$DELAY$
                lock(nodeLocks, current);
                //$DELAY$
                found = true;
                A = engine.get(current, nodeSerializer);
                int pos = keySerializer.findChildren(A, v);
                //check if keys is already in tree
                //$DELAY$
                if(pos<A.keysLen(keySerializer)-1 &&  v!=null && A.key(keySerializer,pos)!=null && //TODO A.key(pos]!=null??
                        0==A.compare(keySerializer,pos,v)){
                    //$DELAY$
                    //yes key is already in tree
                    Object oldVal = A.vals()[pos-1];
                    //$DELAY$
                    if(putOnlyIfAbsent){
                        //is not absent, so quit
                        unlock(nodeLocks, current);
                        if(CC.PARANOID) assertNoLocks(nodeLocks);
                        return valExpand(oldVal);
                    }
                    //insert new
                    //$DELAY$
                    A = ((LeafNode)A).copyChangeValue(pos,value);
                    if(CC.PARANOID && ! (nodeLocks.get(current)==Thread.currentThread()))
                        throw new AssertionError();
                    engine.update(current, A, nodeSerializer);
                    //$DELAY$
                    //already in here
                    V ret =  valExpand(oldVal);
                    notify(key,ret, value2);
                    unlock(nodeLocks, current);
                    //$DELAY$
                    if(CC.PARANOID) assertNoLocks(nodeLocks);
                    return ret;
                }

                //if v > highvalue(a)
                if(!A.isRightEdge() && A.compare(keySerializer,A.keysLen(keySerializer)-1,v)<0){
                    //$DELAY$
                    //follow link until necessary
                    unlock(nodeLocks, current);
                    found = false;
                    //$DELAY$
                    int pos2 = keySerializer.findChildren(A, v);
                    while(A!=null && pos2 == A.keysLen(keySerializer)){
                        //TODO lock?
                        long next = A.next();
                        //$DELAY$
                        if(next==0) break;
                        current = next;
                        A = engine.get(current, nodeSerializer);
                        //$DELAY$
                        pos2 = keySerializer.findChildren(A, v);
                    }

                }


            }while(!found);

            int pos = keySerializer.findChildren(A, v);
            //$DELAY$
            A = A.copyAddKey(keySerializer,pos,v,p,value);
            //$DELAY$
            // can be new item inserted into A without splitting it?
            if(A.keysLen(keySerializer) - (A.isLeaf()?1:0)<maxNodeSize){
                //$DELAY$
                if(CC.PARANOID && ! (nodeLocks.get(current)==Thread.currentThread()))
                    throw new AssertionError();
                engine.update(current, A, nodeSerializer);

                notify(key,  null, value2);
                //$DELAY$
                unlock(nodeLocks, current);
                if(CC.PARANOID) assertNoLocks(nodeLocks);
                return null;
            }else{
                //node is not safe, it requires splitting

                final int splitPos = A.keysLen(keySerializer)/2;
                //$DELAY$
                BNode B = A.copySplitRight(keySerializer,splitPos);
                //$DELAY$
                long q = engine.put(B, nodeSerializer);
                A = A.copySplitLeft(keySerializer,splitPos, q);
                //$DELAY$
                if(CC.PARANOID && ! (nodeLocks.get(current)==Thread.currentThread()))
                    throw new AssertionError();
                engine.update(current, A, nodeSerializer);

                if((current != rootRecid)){ //is not root
                    unlock(nodeLocks, current);
                    p = q;
                    v = (K) A.highKey(keySerializer);
                    //$DELAY$
                    level = level+1;
                    if(stackPos!=-1){ //if stack is not empty
                        current = stackVals[stackPos--];
                    }else{
                        //current := the left most node at level
                        current = leftEdges.get(level-1);
                    }
                    //$DELAY$
                    if(CC.PARANOID && ! (current>0))
                        throw new AssertionError();
                }else{
                    BNode R = new DirNode(
                            keySerializer.arrayToKeys(new Object[]{A.highKey(keySerializer)}),
                            true,true,false,
                            new long[]{current,q, 0});
                    //$DELAY$
                    lock(nodeLocks, rootRecidRef);
                    //$DELAY$
                    unlock(nodeLocks, current);
                    //$DELAY$
                    long newRootRecid = engine.put(R, nodeSerializer);
                    //$DELAY$
                    if(CC.PARANOID && ! (nodeLocks.get(rootRecidRef)==Thread.currentThread()))
                        throw new AssertionError();
                    engine.update(rootRecidRef, newRootRecid, Serializer.LONG);
                    //add newRootRecid into leftEdges
                    leftEdges.add(newRootRecid);

                    notify(key, null, value2);
                    //$DELAY$
                    unlock(nodeLocks, rootRecidRef);
                    //$DELAY$
                    if(CC.PARANOID) assertNoLocks(nodeLocks);
                    //$DELAY$
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
                //$DELAY$
                pointToStart();
            }else{
                //$DELAY$
                Fun.Pair<Integer, LeafNode> l = m.findLargerNode(lo, loInclusive);
                currentPos = l!=null? l.a : -1;
                currentLeaf = l!=null ? l.b : null;
            }
            this.hi = hi;
            this.hiInclusive = hiInclusive;
            //$DELAY$
            if(hi!=null && currentLeaf!=null){
                //check in bounds
                int c = currentLeaf.compare(m.keySerializer,currentPos,hi);
                if (c > 0 || (c == 0 && !hiInclusive)){
                    //out of high bound
                    currentLeaf=null;
                    currentPos=-1;
                    //$DELAY$
                }
            }

        }


        private void pointToStart() {
            //find left-most leaf
            final long rootRecid = m.engine.get(m.rootRecidRef, Serializer.LONG);
            BNode node = (BNode) m.engine.get(rootRecid, m.nodeSerializer);
            //$DELAY$
            while(!node.isLeaf()){
                //$DELAY$
                node = (BNode) m.engine.get(node.child()[0], m.nodeSerializer);
            }
            currentLeaf = (LeafNode) node;
            currentPos = 1;
            //$DELAY$
            while(currentLeaf.keysLen(m.keySerializer)==2){
                //follow link until leaf is not empty
                if(currentLeaf.next == 0){
                    //$DELAY$
                    currentLeaf = null;
                    return;
                }
                //$DELAY$
                currentLeaf = (LeafNode) m.engine.get(currentLeaf.next, m.nodeSerializer);
            }
        }


        public boolean hasNext(){
            return currentLeaf!=null;
        }

        public void remove(){
            if(lastReturnedKey==null) throw new IllegalStateException();
            m.remove(lastReturnedKey);
            //$DELAY$
            lastReturnedKey = null;
        }

        protected void advance(){
            if(currentLeaf==null) return;
            lastReturnedKey =  currentLeaf.key(m.keySerializer,currentPos);
            currentPos++;
            //$DELAY$
            if(currentPos == currentLeaf.keysLen(m.keySerializer)-1){
                //move to next leaf
                if(currentLeaf.next==0){
                    currentLeaf = null;
                    currentPos=-1;
                    return;
                }
                //$DELAY$
                currentPos = 1;
                currentLeaf = (LeafNode) m.engine.get(currentLeaf.next, m.nodeSerializer);
                while(currentLeaf.keysLen(m.keySerializer)==2){
                    if(currentLeaf.next ==0){
                        currentLeaf = null;
                        currentPos=-1;
                        return;
                    }
                    currentLeaf = (LeafNode) m.engine.get(currentLeaf.next, m.nodeSerializer);
                    //$DELAY$
                }
            }
            if(hi!=null && currentLeaf!=null){
                //check in bounds
                int c = currentLeaf.compare(m.keySerializer,currentPos,hi);
                if (c > 0 || (c == 0 && !hiInclusive)){
                    //$DELAY$
                    //out of high bound
                    currentLeaf=null;
                    currentPos=-1;
                }
            }
        }
    }

    @Override
	public V remove(Object key) {
        return removeOrReplace(key, null, null);
    }

    private V removeOrReplace(final Object key, final Object value, final  Object putNewValue) {
        if(key==null)
            throw new NullPointerException("null key");
        long current = engine.get(rootRecidRef, Serializer.LONG);

        BNode A = engine.get(current, nodeSerializer);
        //$DELAY$
        while(!A.isLeaf()){
            //$DELAY$
            current = nextDir((DirNode) A, key);
            A = engine.get(current, nodeSerializer);
        }

        long old =0;
        try{for(;;){
            //$DELAY$
            lock(nodeLocks, current);
            //$DELAY$
            if(old!=0) {
                //$DELAY$
                unlock(nodeLocks, old);
            }
            A = engine.get(current, nodeSerializer);
            //$DELAY$
            int pos = keySerializer.findChildren2(A, key);
//            System.out.println(key+" - "+pos+" - "+A);
            if(pos>0 && pos!=A.keysLen(keySerializer)-1){
                //found, delete from node
                //$DELAY$
                Object oldVal =   A.vals()[pos-1];
                oldVal = valExpand(oldVal);
                if(value!=null && valueSerializer!=null && !valueSerializer.equals((V)value,(V)oldVal)){
                    unlock(nodeLocks, current);
                    //$DELAY$
                    return null;
                }

                Object putNewValueOutside = putNewValue;
                if(putNewValue!=null && valsOutsideNodes){
                    //$DELAY$
                    long recid = engine.put((V)putNewValue,valueSerializer);
                    //$DELAY$
                    putNewValueOutside = new ValRef(recid);
                }

                A = putNewValue!=null?
                        ((LeafNode)A).copyChangeValue(pos,putNewValueOutside):
                        ((LeafNode)A).copyRemoveKey(keySerializer,pos);
                if(CC.PARANOID && ! (nodeLocks.get(current)==Thread.currentThread()))
                    throw new AssertionError();
                //$DELAY$
                engine.update(current, A, nodeSerializer);
                notify((K)key, (V)oldVal, (V)putNewValue);
                unlock(nodeLocks, current);
                return (V) oldVal;
            }else if(pos<=0 && -pos-1!=A.keysLen(keySerializer)-1){
                //not found
                unlock(nodeLocks, current);
                //$DELAY$
                return null;
            }else{
                //move to next link
                old = current;
                current = A.next();
                //$DELAY$
                if(current==0){
                    //end reached
                    unlock(nodeLocks,old);
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
        boolean hasListeners = modListeners.length>0;
        long current = engine.get(rootRecidRef, Serializer.LONG);

        BNode A = engine.get(current, nodeSerializer);
        //$DELAY$
        while(!A.isLeaf()){
            current = A.child()[0];
            //$DELAY$
            A = engine.get(current, nodeSerializer);
        }

        long old =0;
        try{for(;;) {
            //$DELAY$
            //lock nodes
            lock(nodeLocks, current);
            if (old != 0) {
                //$DELAY$
                unlock(nodeLocks, old);
            }
            //$DELAY$
            //notify about deletion
            int size = A.keysLen(keySerializer)-1;
            if(hasListeners) {
                //$DELAY$
                for (int i = 1; i < size; i++) {
                    Object val = (V) A.vals()[i - 1];
                    val = valExpand(val);
                    //$DELAY$
                    notify((K) A.key(keySerializer,i),(V) val, null);
                }
            }

            //remove all node content
            A = ((LeafNode) A).copyClear(keySerializer);
            //$DELAY$
            engine.update(current, A, nodeSerializer);

            //move to next link
            old = current;
            //$DELAY$
            current = A.next();
            if (current == 0) {
                //end reached
                //$DELAY$
                unlock(nodeLocks, old);
                //$DELAY$
                return;
            }
            //$DELAY$
            A = engine.get(current, nodeSerializer);
        }
        }catch(RuntimeException e){
            unlockAll(nodeLocks);
            throw e;
        }catch(Exception e){
            unlockAll(nodeLocks);
            throw new RuntimeException(e);
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
            K ret = (K) currentLeaf.key(m.keySerializer,currentPos);
            //$DELAY$
            advance();
            //$DELAY$
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
            //$DELAY$
            advance();
            //$DELAY$
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
            K ret = (K) currentLeaf.key(m.keySerializer,currentPos);
            Object val = currentLeaf.vals[currentPos-1];
            //$DELAY$
            advance();
            //$DELAY$
            return m.makeEntry(ret, m.valExpand(val));
        }
    }






    protected Entry<K, V> makeEntry(Object key, Object value) {
        if(CC.PARANOID && ! (!(value instanceof ValRef)))
            throw new AssertionError();
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
        //$DELAY$
        while(iter.hasNext()){
            //$DELAY$
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
        return value != null && removeOrReplace(key, value, null) != null;
    }

    @Override
    public boolean replace(final K key, final V oldValue, final V newValue) {
        if(key == null || oldValue == null || newValue == null ) throw new NullPointerException();

        return removeOrReplace(key,oldValue,newValue)!=null;
    }

    @Override
    public V replace(final K key, final V value) {
        if(key == null || value == null) throw new NullPointerException();

        return removeOrReplace(key, null, value);
    }


    @Override
    public Comparator<? super K> comparator() {
        return keySerializer.comparator();
    }


    @Override
    public Map.Entry<K,V> firstEntry() {
        final long rootRecid = engine.get(rootRecidRef, Serializer.LONG);
        BNode n = engine.get(rootRecid, nodeSerializer);
        //$DELAY$
        while(!n.isLeaf()){
            //$DELAY$
            n = engine.get(n.child()[0], nodeSerializer);
        }
        LeafNode l = (LeafNode) n;
        //follow link until necessary
        while(l.keysLen(keySerializer)==2){
            if(l.next==0) return null;
            //$DELAY$
            l = (LeafNode) engine.get(l.next, nodeSerializer);
        }
        //$DELAY$
        return makeEntry(l.key(keySerializer,1), valExpand(l.vals[0]));
    }


    @Override
    public Entry<K, V> pollFirstEntry() {
        //$DELAY$
        while(true){
            //$DELAY$
            Entry<K, V> e = firstEntry();
            //$DELAY$
            if(e==null || remove(e.getKey(),e.getValue())){
                return e;
            }
        }
    }

    @Override
    public Entry<K, V> pollLastEntry() {
        //$DELAY$
        while(true){
            Entry<K, V> e = lastEntry();
            //$DELAY$
            if(e==null || remove(e.getKey(),e.getValue())){
                return e;
            }
        }
    }


    protected Entry<K,V> findSmaller(K key,boolean inclusive){
        if(key==null) throw new NullPointerException();
        final long rootRecid = engine.get(rootRecidRef, Serializer.LONG);
        //$DELAY$
        BNode n = engine.get(rootRecid, nodeSerializer);
        //$DELAY$
        Entry<K,V> k = findSmallerRecur(n, key, inclusive);
        //$DELAY$
        if(k==null || (k.getValue()==null)) return null;
        return k;
    }

    private Entry<K, V> findSmallerRecur(BNode n, K key, boolean inclusive) {
        //TODO optimize comparation in this method
        final boolean leaf = n.isLeaf();
        final int start = leaf ? n.keysLen(keySerializer)-2 : n.keysLen(keySerializer)-1;
        final int end = leaf?1:0;
        final int res = inclusive? 1 : 0;
        //$DELAY$
        for(int i=start;i>=end; i--){
            //$DELAY$
            final Object key2 = n.key(keySerializer,i);
            int comp = (key2==null)? -1 : keySerializer.comparator().compare(key2, key);
            if(comp<res){
                if(leaf){
                    //$DELAY$
                    return key2==null ? null :
                            makeEntry(key2, valExpand(n.vals()[i-1]));
                }else{
                    final long recid = n.child()[i];
                    if(recid==0) continue;
                    BNode n2 = engine.get(recid, nodeSerializer);
                    //$DELAY$
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
        //$DELAY$
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
                //$DELAY$
                if(ret!=null)
                    return ret;
            }

            //iterate over keys to find last non null key
            for(int i=n.keysLen(keySerializer)-2; i>0;i--){
                Object k = n.key(keySerializer,i);
                if(k!=null && n.vals().length>0) {
                    Object val = valExpand(n.vals()[i-1]);
                    //$DELAY$
                    if(val!=null){
                        //$DELAY$
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
                //$DELAY$
                Entry<K,V> ret = lastEntryRecur(n2);
                //$DELAY$
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
        //$DELAY$
        while(!A.isLeaf()){
            current = nextDir((DirNode) A, key);
            //$DELAY$
            A = engine.get(current, nodeSerializer);
        }

        //now at leaf level
        LeafNode leaf = (LeafNode) A;
        //follow link until first matching node is found
        final int comp = inclusive?1:0;
        //$DELAY$
        while(true){
            //$DELAY$
            for(int i=1;i<leaf.keysLen(keySerializer)-1;i++){
                //$DELAY$
                if(leaf.key(keySerializer,i)==null) continue;
                //$DELAY$
                if(-leaf.compare(keySerializer, i, key)<comp){
                    //$DELAY$
                    return makeEntry(leaf.key(keySerializer,i), valExpand(leaf.vals[i-1]));
                }


            }
            if(leaf.next==0) return null; //reached end
            //$DELAY$
            leaf = (LeafNode) engine.get(leaf.next, nodeSerializer);
        }

    }

    protected Fun.Pair<Integer,LeafNode> findLargerNode(final K key, boolean inclusive) {
        if(key==null) return null;

        long current = engine.get(rootRecidRef, Serializer.LONG);
        //$DELAY$
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
            //$DELAY$
            for(int i=1;i<leaf.keysLen(keySerializer)-1;i++){
                if(leaf.key(keySerializer,i)==null) continue;
                //$DELAY$
                if(-leaf.compare(keySerializer,i, key)<comp){
                    return new Fun.Pair(i, leaf);
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
        //$DELAY$
        while(valueIter.hasNext()){
            //$DELAY$
            if(valueSerializer.equals((V)value,valueIter.next()))
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
        private final Serializer valueSerializer;
        EntrySet(ConcurrentNavigableMap<K1, V1> map, Serializer valueSerializer) {
            m = map;
            this.valueSerializer = valueSerializer;
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
            //$DELAY$
            return v != null && valueSerializer.equals(v,e.getValue());
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
            if(lo!=null && hi!=null && m.keySerializer.comparator().compare(lo, hi)>0){
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
            if(key==null)
                throw new NullPointerException("key null");
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
                if(m.valueSerializer.equals((V)value,i.next()))
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
                    int c = m.keySerializer.comparator().compare(fromKey, lo);
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
                    int c = m.keySerializer.comparator().compare(toKey, hi);
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
                int c = m.keySerializer.comparator().compare(key, lo);
                if (c < 0 || (c == 0 && !loInclusive))
                    return true;
            }
            return false;
        }

        private boolean tooHigh(K key) {
            if (hi != null) {
                int c = m.keySerializer.comparator().compare(key, hi);
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
            return new EntrySet<K, V>(this,m.valueSerializer);
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
            if(lo!=null && hi!=null && m.keySerializer.comparator().compare(lo, hi)>0){
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
                if(m.valueSerializer.equals((V) value,i.next()))
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
                    int c = m.keySerializer.comparator().compare(fromKey, lo);
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
                    int c = m.keySerializer.comparator().compare(toKey, hi);
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
                int c = m.keySerializer.comparator().compare(key, lo);
                if (c < 0 || (c == 0 && !loInclusive))
                    return true;
            }
            return false;
        }

        private boolean tooHigh(K key) {
            if (hi != null) {
                int c = m.keySerializer.comparator().compare(key, hi);
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
            return new EntrySet<K, V>(this,m.valueSerializer);
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
                keySerializer, valueSerializer, numberOfNodeMetas);
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

    //TODO check  references to notify
    protected void notify(K key, V oldValue, V newValue) {
        if(CC.PARANOID && ! (!(oldValue instanceof ValRef)))
            throw new AssertionError();
        if(CC.PARANOID && ! (!(newValue instanceof ValRef)))
            throw new AssertionError();

        Bind.MapListener<K,V>[] modListeners2  = modListeners;
        for(Bind.MapListener<K,V> listener:modListeners2){
            if(listener!=null)
                listener.update(key, oldValue, newValue);
        }
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
        if(CC.PARANOID && ! (t==Thread.currentThread()))
            throw new AssertionError("unlocked wrong thread");
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
        if(CC.PARANOID && ! (locks.get(recid)!= currentThread))
            throw new AssertionError("node already locked by current thread: "+recid);

        while(locks.putIfAbsent(recid, currentThread) != null){
            LockSupport.parkNanos(10);
        }
    }


    public void checkStructure(){
        LongHashMap recids = new LongHashMap();
        final long recid = engine.get(rootRecidRef, Serializer.LONG);

        checkNodeRecur(recid,recids);

    }

    private void checkNodeRecur(long rootRecid, LongHashMap recids) {
        BNode n = engine.get(rootRecid, nodeSerializer);
        n.checkStructure(keySerializer);

        if(recids.get(rootRecid)!=null){
            throw new AssertionError("Duplicate recid: "+rootRecid);
        }
        recids.put(rootRecid,this);

        if(n.next()!=0L && recids.get(n.next())==null){
            throw new AssertionError("Next link was not found: "+n);
        }
        if(n.next()==rootRecid){
            throw new AssertionError("Recursive next: "+n);
        }
        if(!n.isLeaf()){
            long[] child = n.child();
            for(int i=child.length-1; i>=0; i--){
                long recid = child[i];
                if(recid==rootRecid){
                    throw new AssertionError("Recursive recid: "+n);
                }

                if(recid==0 || recid==n.next()){
                    continue;
                }
                checkNodeRecur(recid, recids);;

            }
        }

    }


}
