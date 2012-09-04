package net.kotek.jdbm;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Concurrent B-linked-tree.
 */
public class BTreeMap<K,V> {

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

    public V get(K v){
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


    public V put(K v, V value){

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

}
