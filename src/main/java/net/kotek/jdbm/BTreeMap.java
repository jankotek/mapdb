package net.kotek.jdbm;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 * Concurrent B-linked-tree.
 */
public class BTreeMap<K,V> extends  AbstractMap<K,V> implements
        ConcurrentSortedMap<K,V>, ConcurrentMap<K,V>, SortedMap<K,V> {

    protected final Serializer SERIALIZER = Serializer.BASIC_SERIALIZER;

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

    protected final int maxNodeSize;

    protected final RecordManager recman;

    protected final boolean hasValues;


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

        @Override public String toString(){
            return "Dir(K"+Arrays.toString(keys)+", C"+Arrays.toString(child)+")";
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
            for(Object key:value.keys()){
                SERIALIZER.serialize(out, key);
            }

            if(isLeaf && hasValues)
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
                Object[] vals  = null;
                if(hasValues){
                    vals = new Object[size];
                    for(int i=0;i<size;i++){
                        vals[i] = SERIALIZER.deserialize(in, -1);
                    }
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
    public BTreeMap(RecordManager recman, int maxNodeSize, boolean hasValues) {
        if(maxNodeSize%2!=0) throw new IllegalArgumentException("maxNodeSize must be dividable by 2");
        if(maxNodeSize<6) throw new IllegalArgumentException("maxNodeSize too low");
        if(maxNodeSize>126) throw new IllegalArgumentException("maxNodeSize too high");
        this.hasValues = hasValues;
        this.recman = recman;
        this.maxNodeSize = maxNodeSize;
        LeafNode emptyRoot = new LeafNode(new Object[]{NEG_INFINITY, POS_INFINITY}, new Object[]{null, null}, 0);
        this.rootRecid = recman.recordPut(emptyRoot, nodeSerializer);
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
        BNode A = recman.recordGet(current, nodeSerializer);

        //dive until  leaf
        while(!A.isLeaf()){
            current = nextDir((DirNode) A, v);
            A = recman.recordGet(current, nodeSerializer);
        }

        //now at leaf level
        LeafNode leaf = (LeafNode) A;
        int pos = findChildren(v, leaf.keys);
        while(pos == leaf.keys.length){
            //follow next link on leaf until necessary
            leaf = (LeafNode) recman.recordGet(leaf.next, nodeSerializer);
            pos = findChildren(v, leaf.keys);
        }

        if(pos == 1 && leaf.keys.length==2){
            return null; //empty node
        }
        //finish search
        if(v.equals(leaf.keys[pos]))
            return (V) (hasValues? leaf.vals[pos] : JdbmUtil.EMPTY_STRING);
        else
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

        Deque<Long> stack = new LinkedList<Long>();
        long current = rootRecid;

        BNode A = recman.recordGet(current, nodeSerializer);
        while(!A.isLeaf()){
            long t = current;
            current = nextDir((DirNode) A, v);
            if(current == A.child()[A.child().length-1]){
                //is link, do nothing
            }else{
                stack.push(t);
            }
            A = recman.recordGet(current, nodeSerializer);
        }
        int level = 1;

        long p=0;
        while(true){
            boolean found = true;
            do{
                lockNode(current);
                A = recman.recordGet(current, nodeSerializer);
                int pos = findChildren(v, A.keys());
                if(pos<A.keys().length-1 && v.equals(A.keys()[pos])){

                    V oldVal = (V) (hasValues? A.vals()[pos] : JdbmUtil.EMPTY_STRING);
                    if(putOnlyIfAbsent){
                        //is not absent, so quit
                        unlockNode(current);
                        return oldVal;
                    }
                    //insert new
                    Object[] vals = null;
                    if(hasValues){
                        vals = Arrays.copyOf(A.vals(), A.vals().length);
                        vals[pos] = value;
                    }

                    A = new LeafNode(Arrays.copyOf(A.keys(), A.keys().length), vals, ((LeafNode)A).next);
                    recman.recordUpdate(current, A, nodeSerializer);
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
                        A = recman.recordGet(current, nodeSerializer);
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
                    recman.recordUpdate(current, n, nodeSerializer);
                }else{
                    if(CC.ASSERT && p==0)
                        throw new InternalError();
                    long[] child = JdbmUtil.arrayLongPut(A.child(), pos, p);
                    DirNode d = new DirNode(keys, child);
                    recman.recordUpdate(current, d, nodeSerializer);
                }

                unlockNode(current);
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
                long q = recman.recordPut(B, nodeSerializer);
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
                recman.recordUpdate(current, A, nodeSerializer);

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
                    rootRecid = recman.recordPut(R, nodeSerializer);
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
            BNode node = recman.recordGet(rootRecid, nodeSerializer);
            while(!node.isLeaf()){
                node = recman.recordGet(node.child()[0], nodeSerializer);
            }
            currentLeaf = (LeafNode) node;
            currentPos = 1;

            while(currentLeaf.keys.length==2){
                //follow link until leaf is not empty
                if(currentLeaf.next == 0){
                    currentLeaf = null;
                    return;
                }
                currentLeaf = (LeafNode) recman.recordGet(currentLeaf.next, nodeSerializer);
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
                currentLeaf = (LeafNode) recman.recordGet(currentLeaf.next, nodeSerializer);
                while(currentLeaf.keys.length==2){
                    if(currentLeaf.next ==0){
                        currentLeaf = null;
                        currentPos=-1;
                        return;
                    }
                    currentLeaf = (LeafNode) recman.recordGet(currentLeaf.next, nodeSerializer);
                }
            }
        }
    }

    public V remove(Object key) {
        return remove2(key, null);
    }

    private V remove2(Object key, Object value) {
        long current = rootRecid;
        BNode A = recman.recordGet(current, nodeSerializer);
        while(!A.isLeaf()){
            current = nextDir((DirNode) A, key);
            A = recman.recordGet(current, nodeSerializer);
        }

        while(true){

            lockNode(current);
            A = recman.recordGet(current, nodeSerializer);
            int pos = findChildren(key, A.keys());
            if(pos<A.keys().length&& key.equals(A.keys()[pos])){
                //delete from node
                Object oldVal = hasValues? A.vals()[pos] : JdbmUtil.EMPTY_STRING;
                if(value!=null && !value.equals(oldVal)) return null;

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
                recman.recordUpdate(current, A, nodeSerializer);
                unlockNode(current);
                return (V) oldVal;
            }else{
                unlockNode(current);
                //follow link until necessary
                if(A.highKey() != POS_INFINITY && comparator.compare(key, A.highKey())>0){
                    int pos2 = findChildren(key, A.keys());
                    while(pos2 == A.keys().length){
                        //TODO lock?
                        current = ((LeafNode)A).next;
                        A = recman.recordGet(current, nodeSerializer);
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
            if(o instanceof Entry){
                Entry e = (Entry) o;
                Object key = e.getKey();
                if(key == null) return false;
                return BTreeMap.this.remove(key, e.getValue());
            }
            return false;
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
        return remove2(key, value)!=null;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        if(key == null || oldValue == null || newValue == null ) throw new NullPointerException();

        long current = rootRecid;
        BNode node = recman.recordGet(current, nodeSerializer);
        //dive until leaf is found
        while(!node.isLeaf()){
            current = nextDir((DirNode) node, key);
            node = recman.recordGet(current, nodeSerializer);
        }

        lockNode(current);
        LeafNode leaf = (LeafNode) recman.recordGet(current, nodeSerializer);

        int pos = findChildren(key, node.keys());
        while(pos==leaf.keys.length){
            //follow leaf link until necessary
            lockNode(leaf.next);
            unlockNode(current);
            current = leaf.next;
            leaf = (LeafNode) recman.recordGet(current, nodeSerializer);
            pos = findChildren(key, node.keys());
        }

        if(key.equals(leaf.keys[pos]) && oldValue.equals(leaf.vals[pos])){
            Object[] vals = Arrays.copyOf(leaf.vals, leaf.vals.length);
            vals[pos] = newValue;
            leaf = new LeafNode(Arrays.copyOf(leaf.keys, leaf.keys.length), vals, leaf.next);
            recman.recordUpdate(current, leaf, nodeSerializer);
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
        BNode node = recman.recordGet(current, nodeSerializer);
        //dive until leaf is found
        while(!node.isLeaf()){
            current = nextDir((DirNode) node, key);
            node = recman.recordGet(current, nodeSerializer);
        }

        lockNode(current);
        LeafNode leaf = (LeafNode) recman.recordGet(current, nodeSerializer);

        int pos = findChildren(key, node.keys());
        while(pos==leaf.keys.length){
            //follow leaf link until necessary
            lockNode(leaf.next);
            unlockNode(current);
            current = leaf.next;
            leaf = (LeafNode) recman.recordGet(current, nodeSerializer);
            pos = findChildren(key, node.keys());
        }

        if(key.equals(leaf.keys[pos])){
            Object[] vals = Arrays.copyOf(leaf.vals, leaf.vals.length);
            V oldVal = (V) vals[pos];
            vals[pos] = value;
            leaf = new LeafNode(Arrays.copyOf(leaf.keys, leaf.keys.length), vals, leaf.next);
            recman.recordUpdate(current, leaf, nodeSerializer);
            unlockNode(current);
            return oldVal;
        }else{
            unlockNode(current);
            return null;
        }
    }


    @Override
    public Comparator<? super K> comparator() {
        return comparator;
    }

    @Override
    public SortedMap<K, V> subMap(K fromKey, K toKey) {
        throw new InternalError("not yet implemented");
    }

    @Override
    public SortedMap<K, V> headMap(K toKey) {
        throw new InternalError("not yet implemented");
    }

    @Override
    public SortedMap<K, V> tailMap(K fromKey) {
        throw new InternalError("not yet implemented");
    }

    @Override
    public K firstKey() {
        BNode n = recman.recordGet(rootRecid, nodeSerializer);
        while(!n.isLeaf()){
            n = recman.recordGet(n.child()[0], nodeSerializer);
        }
        LeafNode l = (LeafNode) n;
        //follow link until necessary
        while(l.keys.length==2){
            if(l.next==0) return null;
            l = (LeafNode) recman.recordGet(l.next, nodeSerializer);
        }
        return (K) l.keys[1];
    }

    @Override
    public K lastKey() {
        throw new InternalError("not yet implemented");
        //TODO last key, not so simple with empty leaf nodes
    }
}
