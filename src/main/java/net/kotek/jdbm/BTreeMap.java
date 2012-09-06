package net.kotek.jdbm;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 * Concurrent B-linked-tree.
 */
public class BTreeMap<K,V> extends  AbstractMap<K,V> implements ConcurrentSortedMap<K,V>, ConcurrentMap<K,V> {

    protected static final Serializer SERIALIZER = Serializer.BASIC_SERIALIZER;

    //TODO infinity objects can be replaced with nulls? but what if key was deleted?
    protected static final Object NEG_INFINITY = new Object(){
        @Override public String toString() { return "neg_infinity"; }
    };
    protected static final Object POS_INFINITY = new Object(){
        @Override public String toString() { return "pos_infinity"; }
    };


    protected long rootRecid;

    protected final Comparator comparator =  JdbmUtil.COMPARABLE_COMPARATOR;

    protected final LongConcurrentHashMap<Thread> nodeWriteLocks = new LongConcurrentHashMap<Thread>();

    protected final int k;

    protected final RecordManager recman;

    protected final long treeRecid;



    static class Root{
        long rootRecid;
    }


    interface BNode{
        boolean isLeaf();
        Object[] keys();
        Object[] vals();
        Object highKey();
        long[] child();
    }

    final static class DirNode implements BNode{
        final Object[] v;
        final long[] child;

        DirNode(Object[] v, long[] child) {
            this.v = v;
            this.child = child;
        }

        @Override public boolean isLeaf() { return false;}

        @Override public Object[] keys() { return v;}
        @Override public Object[] vals() { return null;}

        @Override public Object highKey() {return v[v.length-1];}

        @Override public long[] child() { return child;}

        @Override public String toString(){
            return "Dir(K"+Arrays.toString(v)+", C"+Arrays.toString(child)+")";
        }

    }

    final static class LeafNode implements BNode{
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

        @Override public String toString(){
            return "Leaf(K"+Arrays.toString(keys)+", V"+Arrays.toString(vals)+", L="+next+")";
        }


    }



    final Serializer<BNode> NODE_SERIALIZER = new Serializer<BNode>() {
        @Override
        public void serialize(DataOutput out, BNode value) throws IOException {
            final boolean isLeaf = value.isLeaf();


            //first byte encodes if is leaf (first bite) and length (last seven bites)
            if(CC.ASSERT && value.keys().length>127) throw new InternalError();
            if(CC.ASSERT && !isLeaf && value.child().length!= value.keys().length) throw new InternalError();
            if(CC.ASSERT && isLeaf && value.vals().length!= value.keys().length) throw new InternalError();

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
            for(Object key:value.keys()){
                SERIALIZER.serialize(out, key);
            }

            if(isLeaf)
                for(Object val:value.vals()){
                    SERIALIZER.serialize(out,val);
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
                Object[] keys = new  Object[size];
                for(int i=0;i<size;i++)
                    keys[i] = SERIALIZER.deserialize(in,-1);
                Object[] vals = new Object[size];
                for(int i=0;i<size;i++){
                    vals[i] = SERIALIZER.deserialize(in, -1);
                }
                return new LeafNode(keys, vals, next);
            }else{
                long[] child = new long[size];
                for(int i=0;i<size;i++)
                    child[i] = JdbmUtil.unpackLong(in);
                Object[] keys = new  Object[size];
                for(int i=0;i<size;i++)
                    keys[i] = SERIALIZER.deserialize(in,-1);
                return new DirNode(keys, child);
            }
        }
    };


    /** constructor used to create new tree*/
    public BTreeMap(RecordManager recman, int maxNodeSize) {
        this.recman = recman;
        LeafNode emptyRoot = new LeafNode(new Object[]{NEG_INFINITY, POS_INFINITY}, new Object[]{null, null}, 0);
        this.rootRecid = recman.recordPut(emptyRoot, NODE_SERIALIZER);
        this.treeRecid = 0;
        this.k = maxNodeSize/2;
    }


    protected void unlockNode(final long nodeRecid) {
        final Thread t = nodeWriteLocks.remove(nodeRecid);
        if(t!=Thread.currentThread())
            throw new InternalError("unlocked wrong thread");
    }

    protected void lockNode(final long nodeRecid) {
        //feel free to rewrite, if you know better (more efficient) way
        while(nodeWriteLocks.putIfAbsent(nodeRecid, Thread.currentThread()) != null){
            Thread.yield();
        }
    }

    /**
     * Find the first children node with a key equal or greater than the given key.
     * If all items are smaller it returns `keys.length`
     */
    protected final int findChildren(final Object key, final Object[] keys) {

        int i = 0;
        if(keys[0] == NEG_INFINITY) i++;
        final int max = keys[keys.length-1] == POS_INFINITY ? keys.length-1 :  keys.length;
        //TODO binary search here
        while(i!=max && comparator.compare(key, keys[i])>0){
            i++;
        }
        return i;
    }

    public V get(Object key){
        if(key==null) return null;
        K v = (K) key;
        long current = rootRecid;
        BNode A = recman.recordGet(current, NODE_SERIALIZER);

        //dive until  leaf
        while(!A.isLeaf()){
            current = nextDir((DirNode) A, v);
            A = recman.recordGet(current, NODE_SERIALIZER);
        }

        //now at leaf level
        LeafNode leaf = (LeafNode) A;
        int pos = findChildren(v, leaf.keys);
        while(pos == leaf.keys.length){
            //follow next link on leaf until necessary
            leaf = (LeafNode) recman.recordGet(leaf.next, NODE_SERIALIZER);
            pos = findChildren(v, leaf.keys);
        }

        //finish search
        if(v.equals(leaf.keys[pos]))
            return (V) leaf.vals[pos];
        else
            return null;
    }

    protected long nextDir(DirNode d, K key) {
        int pos = findChildren(key, d.v) - 1;
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

        Deque<Long> stack = new LinkedList<Long>();
        long current = rootRecid;

        BNode A = recman.recordGet(current, NODE_SERIALIZER);
        while(!A.isLeaf()){
            long t = current;
            current = nextDir((DirNode) A, v);
            if(current == A.child()[A.child().length-1]){
                //is link, do nothing
            }else{
                stack.push(t);
            }
            A = recman.recordGet(current, NODE_SERIALIZER);
        }
        int level = 1;

        long p=0;
        while(true){
            boolean found = true;
            do{
                lockNode(current);
                A = recman.recordGet(current, NODE_SERIALIZER);
                int pos = findChildren(v, A.keys());
                if(pos<A.keys().length-1 && v.equals(A.keys()[pos])){

                    V oldVal = (V) A.vals()[pos];
                    if(putOnlyIfAbsent){
                        //is not absent, so quit
                        unlockNode(current);
                        return oldVal;
                    }
                    //insert new
                    Object[] vals = Arrays.copyOf(A.vals(), A.vals().length);
                    vals[pos] = value;
                    A = new LeafNode(Arrays.copyOf(A.keys(), A.keys().length), vals, ((LeafNode)A).next);
                    recman.recordUpdate(current, A, NODE_SERIALIZER);
                    //already in here
                    unlockNode(current);
                    return oldVal;
                }


                if(A.highKey() != POS_INFINITY && comparator.compare(v, A.highKey())>0){
                    //follow link until necessary
                    unlockNode(current);
                    found = false;
                    int pos2 = findChildren(v, A.keys());
                    while(pos2 == A.keys().length){
                        //TODO lock?
                        current = ((LeafNode)A).next;
                        A = recman.recordGet(current, NODE_SERIALIZER);
                    }
                }


            }while(!found);


            // can be new item inserted into A without splitting it?
            if(A.keys().length<k*2){
                int pos = findChildren(v, A.keys());
                Object[] keys = JdbmUtil.arrayPut(A.keys(), pos, v);

                if(A.isLeaf()){
                    Object[] vals = JdbmUtil.arrayPut(A.vals(), pos, value);
                    LeafNode n = new LeafNode(keys, vals, ((LeafNode)A).next);
                    recman.recordUpdate(current, n, NODE_SERIALIZER);
                }else{
                    if(CC.ASSERT && p==0)
                        throw new InternalError();
                    long[] child = JdbmUtil.arrayLongPut(A.child(), pos, p);
                    DirNode d = new DirNode(keys, child);
                    recman.recordUpdate(current, d, NODE_SERIALIZER);
                }

                unlockNode(current);
                return null;
            }else{
                //node is not safe, it requires splitting
                final boolean isRoot = (current == rootRecid);

                final int pos = findChildren(v, A.keys());
                final Object[] keys = JdbmUtil.arrayPut(A.keys(), pos, v);
                final Object[] vals = A.isLeaf()? JdbmUtil.arrayPut(A.vals(), pos, value) : null;
                final long[] child = A.isLeaf()? null : JdbmUtil.arrayLongPut(A.child(), pos, p);
                final int splitPos = keys.length/2;
                BNode B = A.isLeaf()?
                        new LeafNode(
                                Arrays.copyOfRange(keys, splitPos, keys.length),
                                Arrays.copyOfRange(vals, splitPos, vals.length),
                                ((LeafNode)A).next):
                        new DirNode(Arrays.copyOfRange(keys, splitPos, keys.length),
                                Arrays.copyOfRange(child, splitPos, keys.length))
                        ;
                long q = recman.recordPut(B, NODE_SERIALIZER);
                if(A.isLeaf()){  //  splitPos+1 is there so A gets new high  value (key)
                    Object[] vals2 = Arrays.copyOf(vals, splitPos+1);
                    //TODO check high/low keys overlap
                    A = new LeafNode(Arrays.copyOf(keys, splitPos+1), vals2, q);
                }else{
                    long[] child2 = Arrays.copyOf(child, splitPos+1);
                    child2[splitPos] = q;
                    A = new DirNode(Arrays.copyOf(keys, splitPos+1), child2);
                }
                recman.recordUpdate(current, A, NODE_SERIALIZER);

                if(!isRoot){
                    unlockNode(current);
                    p = q;
                    v = (K) A.highKey();
                    level = level+1;
                    if(!stack.isEmpty()){
                        current = stack.pop();
                    }else{
                        current = -1; //TODO pointer to left most node at level level
                        throw new InternalError();
                    }
                }else{
                    BNode R = new DirNode(
                            new Object[]{A.keys()[0], A.highKey(), B.highKey()},
                            new long[]{current,q, 0});
                    rootRecid = recman.recordPut(R, NODE_SERIALIZER);
                    //TODO update tree levels
                    unlockNode(current);
                    return null;
                }
            }
        }
    }


    abstract class BTreeIterator{
        LeafNode currentLeaf;
        K lastReturnedKey;
        int currentPos;

        BTreeIterator(){
            //find left-most leaf
            BNode node = recman.recordGet(rootRecid, NODE_SERIALIZER);
            while(!node.isLeaf()){
                node = recman.recordGet(node.child()[0],NODE_SERIALIZER);
            }
            currentLeaf = (LeafNode) node;
            //handle empty map
            if(currentLeaf.keys.length==2 && currentLeaf.keys[0] == NEG_INFINITY && currentLeaf.keys[1]==POS_INFINITY)
                currentLeaf = null;
            else
                //handle negative infinity
                currentPos = currentLeaf.keys[0] == NEG_INFINITY? 1: 0;
        }

        public boolean hasNext(){
            return currentLeaf!=null;
        }

        public void remove(){
            BTreeMap.this.remove(lastReturnedKey);
        }

        protected void moveToNext(){
            if(currentLeaf==null) return;
            lastReturnedKey = (K) currentLeaf.keys[currentPos];
            currentPos++;
            if(currentPos == currentLeaf.keys.length-1 ){
                //move to next leaf
                if(currentLeaf.next==0){
                    //end was reached
                    currentPos = 0;
                    currentLeaf = null;
                }else{
                    currentLeaf = (LeafNode) recman.recordGet(currentLeaf.next, NODE_SERIALIZER);
                    currentPos = 0;
                }
            }
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
            V ret = (V) currentLeaf.vals[currentPos];
            moveToNext();
            return ret;
        }
    }

    class BTreeEntryIterator extends BTreeIterator implements  Iterator<Entry<K, V>>{

        @Override
        public Entry<K, V> next() {
            if(currentLeaf == null) throw new NoSuchElementException();
            K ret = (K) currentLeaf.keys[currentPos];
            moveToNext();
            return new BTreeEntry(ret);

        }


    };

    class BTreeEntry implements Entry<K,V>{

        final K key;

        BTreeEntry(K key) {
            this.key = key;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return BTreeMap.this.get(key);
        }

        @Override
        public V setValue(V value) {
            return BTreeMap.this.put(key, value);
        }

        @Override
        public boolean equals(Object o) {
            return (o instanceof Entry) && key.equals(((Entry) o).getKey());
        }

        @Override
        public int hashCode() {
            final V value = BTreeMap.this.get(key);
            return (key == null ? 0 : key.hashCode()) ^
                    (value == null ? 0 : value.hashCode());
        }

    }

    @Override
    public boolean containsKey(Object key) {
        return get(key)!=null;
    }

    @Override
    public boolean containsValue(Object value){
        return values.contains(value);
    }

    public V remove(Object key) {
        //TODO btree remove
        throw new UnsupportedOperationException("not implemented yet");
    }


    @Override
    public void clear() {
        //TODO clear
    }

    final private Set<K> keySet = new AbstractSet<K>(){

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
        public Iterator<K> iterator() {
            return new BTreeKeyIterator();
        }

        @Override
        public boolean add(K k) {
            return false;
        }

        @Override
        public boolean remove(Object o) {
            return BTreeMap.this.remove(o)!=null;
        }

        @Override
        public void clear() {
            BTreeMap.this.clear();
        }
    };

    @Override
    public Set<K> keySet() {
        return keySet;
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
        public boolean remove(Object o) {
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

    private final Set<Entry<K, V>> entrySet = new AbstractSet<Entry<K, V>>(){

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
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }


        @Override
        public void clear() {
            BTreeMap.this.clear();
        }
    };

    @Override
    public Set<Entry<K, V>> entrySet() {
        return entrySet;
    }

    @Override
    public boolean isEmpty() {
        return !keySet.iterator().hasNext();
    }

    @Override
    public int size(){
        long size = 0;
        Iterator iter = keySet.iterator();
        while(iter.hasNext()){
            iter.next();
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
        return false;   //TODO concurrent stuff
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        if(key == null || oldValue == null || newValue == null ) throw new NullPointerException();

        long current = rootRecid;
        BNode node = recman.recordGet(current, NODE_SERIALIZER);
        //dive until leaf is found
        while(!node.isLeaf()){
            current = nextDir((DirNode) node, key);
            node = recman.recordGet(current, NODE_SERIALIZER);
        }

        lockNode(current);
        LeafNode leaf = (LeafNode) recman.recordGet(current, NODE_SERIALIZER);

        int pos = findChildren(key, node.keys());
        while(pos==leaf.keys.length){
            //follow leaf link until necessary
            lockNode(leaf.next);
            unlockNode(current);
            current = leaf.next;
            leaf = (LeafNode) recman.recordGet(current, NODE_SERIALIZER);
            pos = findChildren(key, node.keys());
        }

        if(key.equals(leaf.keys[pos]) && oldValue.equals(leaf.vals[pos])){
            Object[] vals = Arrays.copyOf(leaf.vals, leaf.vals.length);
            vals[pos] = newValue;
            leaf = new LeafNode(Arrays.copyOf(leaf.keys, leaf.keys.length), vals, leaf.next);
            recman.recordUpdate(current, leaf, NODE_SERIALIZER);
            unlockNode(current);
            return true;
        }else{
            unlockNode(current);
            return false;
        }
    }

    @Override
    public V replace(K key, V value) {
        if(key == null || value == null) throw new NullPointerException();

        long current = rootRecid;
        BNode node = recman.recordGet(current, NODE_SERIALIZER);
        //dive until leaf is found
        while(!node.isLeaf()){
            current = nextDir((DirNode) node, key);
            node = recman.recordGet(current, NODE_SERIALIZER);
        }

        lockNode(current);
        LeafNode leaf = (LeafNode) recman.recordGet(current, NODE_SERIALIZER);

        int pos = findChildren(key, node.keys());
        while(pos==leaf.keys.length){
            //follow leaf link until necessary
            lockNode(leaf.next);
            unlockNode(current);
            current = leaf.next;
            leaf = (LeafNode) recman.recordGet(current, NODE_SERIALIZER);
            pos = findChildren(key, node.keys());
        }

        if(key.equals(leaf.keys[pos])){
            Object[] vals = Arrays.copyOf(leaf.vals, leaf.vals.length);
            V oldVal = (V) vals[pos];
            vals[pos] = value;
            leaf = new LeafNode(Arrays.copyOf(leaf.keys, leaf.keys.length), vals, leaf.next);
            recman.recordUpdate(current, leaf, NODE_SERIALIZER);
            unlockNode(current);
            return oldVal;
        }else{
            unlockNode(current);
            return null;
        }
    }

}
