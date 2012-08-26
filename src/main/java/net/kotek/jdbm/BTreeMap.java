package net.kotek.jdbm;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Concurrent B-linked-tree.
 */
public class BTreeMap<K,V> {

    static final Serializer SERIALIZER = Serializer.BASIC_SERIALIZER;

    protected final long rootRecid;

    protected Comparator comparator =  JdbmUtil.COMPARABLE_COMPARATOR;

    protected LongConcurrentHashMap<Thread> nodeWriteLocks = new LongConcurrentHashMap<Thread>();

    protected final int maxNodeSize = 32;


    static class Root{
        long rootRecid;
    }

    static class BNode {
        long[] refs;
        Object[] keys;
        Object[] values;
    }

    final Serializer<BNode> NODE_SERIALIZER = new Serializer<BNode>() {
        @Override
        public void serialize(DataOutput out, BNode value) throws IOException {
            final boolean isLeaf = value.values!=null;
            //first byte encodes if is leaf (first bite) and length (last seven bites)
            if(CC.ASSERT && value.keys.length>127) throw new InternalError();
            final int header = (isLeaf?0x80:0)  | value.keys.length;
            out.write(header);

            if(!isLeaf){
                //write references
                for(long ref:value.refs){
                    JdbmUtil.packLong(out,ref);
                }
            }

            //write keys
            for(Object key:value.keys){
                SERIALIZER.serialize(out, key);
            }

            if(isLeaf){
                //write values
                for(Object v:value.values){
                    SERIALIZER.serialize(out,v);
                }
            }

        }

        @Override
        public BNode deserialize(DataInput in, int available) throws IOException {
            BNode ret = new BNode();
            int size = in.readUnsignedByte();
            //first bite indicates leaf
            final boolean isLeaf = (size & 0x80) != 0;
            //rest is for node size
            size = size & 0x7f;

            if(!isLeaf){
                ret.refs = new long[size];
                for(int i=0;i<ret.refs.length;i++){
                    ret.refs[i] = JdbmUtil.unpackLong(in);
                }
            }


            ret.keys =  new Object[size];
            for(int i=0;i<ret.keys.length;i++){
                ret.keys[i] = SERIALIZER.deserialize(in, -1);
            }


            if(isLeaf){
                ret.values =  new Object[size];
                for(int i=0;i<ret.values.length;i++){
                    ret.values[i] = SERIALIZER.deserialize(in, -1);
                }
            }

            return ret;
        }
    };

    final RecordManager recman;
    final long treeRecid;

    /** constructor used to create new tree*/
    public BTreeMap(RecordManager recman) {
        this.recman = recman;
        this.rootRecid = recman.recordPut(null, Serializer.NULL_SERIALIZER);
        this.treeRecid = 0;

    }

    /** constructor used to load existing tree*/
    public BTreeMap(RecordManager recman, long treeRecid) {
        this.recman = recman;
        this.treeRecid = treeRecid;
        this.rootRecid = 0L;
    }




    public V put(K key, V value) {
        BNode root = recman.recordGet(rootRecid, NODE_SERIALIZER);
        if(root == null){
            //create new root
            root = new BNode();
            root.keys = new Object[]{key};
            root.values = new Object[]{value};
            recman.recordUpdate(rootRecid, root, NODE_SERIALIZER);
            return null;
        }
        //insert at position
        int pos = findChildren(key, root.keys);
        //binary search does not take this case into account
        if(pos == root.keys.length-1){
            if(comparator.compare(key, root.keys[pos])==1)
                pos++;
        }

        boolean containsKey = pos<root.keys.length && root.keys[pos].equals(key);
        V ret = containsKey? (V) root.values[pos] : null;

        if(root.keys.length>=maxNodeSize){
            //need to split node
            long recid1 = putValueIntoNode(key, value,root, containsKey, pos, 0, maxNodeSize/2, 0);
            long recid2 = putValueIntoNode(key, value,root, containsKey, pos, maxNodeSize/2,  root.keys.length, 0);

            BNode root2 = new BNode();
            root2.keys = new Object[]{root.keys[0], root.keys[maxNodeSize/2]};
            root2.refs = new long[]{recid1, recid2};
            recman.recordUpdate(rootRecid, root2, NODE_SERIALIZER);
        }else{
            //just update node, no spliting
            putValueIntoNode(key, value, root, containsKey, pos,  0,  root.keys.length, rootRecid);
        }
        return ret;
    }

    private void unlockNode(final long nodeRecid) {
        final Thread t = nodeWriteLocks.remove(nodeRecid);
        if(t!=Thread.currentThread())
            throw new InternalError("unlocked wrong thread");
    }

    private void lockNode(final long nodeRecid) {
        //feel free to rewrite, if you know better (more efficient) way
        while(nodeWriteLocks.putIfAbsent(nodeRecid, Thread.currentThread()) != null){
            Thread.yield();
        }
    }

    protected long putValueIntoNode(final K key, final V value, final BNode node, final boolean containsKey, final int pos,
                               final int start,final  int end, final long nodeRecid) {
        BNode node2 = new BNode();
        if(pos<start ||pos>end){
            //pos outside of range, so just copy node
            node2.keys = Arrays.copyOfRange(node.keys,  start, end);
            node2.values = Arrays.copyOfRange(node.values, start, end);
        }else if(containsKey){
            //key equality, so just update value
            node2.keys = Arrays.copyOfRange(node.keys,  start, end);
            node2.values = Arrays.copyOfRange(node.values, start, end);
            node2.values[pos-start] = value;
        }else{
            //key not equal, needs to expand array

            node2.keys = Arrays.copyOfRange(node.keys, start, end + 1);
            node2.values = Arrays.copyOfRange(node.values, start, end + 1);

            //make space by moving to new pos
            if(pos!=node.keys.length){
                System.arraycopy(node.keys, pos, node2.keys, pos+1-start, end-pos);
                System.arraycopy(node.values, pos, node2.values, pos+1-start, end-pos);
            }
            node2.keys[pos-start] = key;
            node2.values[pos-start] = value;

        }
        if(nodeRecid!=0){
            recman.recordUpdate(nodeRecid, node2, NODE_SERIALIZER);
            return rootRecid;
        }else{
            return recman.recordPut(node2, NODE_SERIALIZER);
        }
    }

    public V get(K key) {
        BNode root = recman.recordGet(rootRecid, NODE_SERIALIZER);
        final int index = findChildren(key, root.keys);
        return (V) root.values[index];
    }


    /**
     * Find the first children node with a key equal or greater than the given
     * key.
     *
     * @return index of first children with equal or greater key.
     */
    private int findChildren(final K key, final Object[] keys) {
        int left = 0;
        int right = keys.length-1;
        int middle;

        // binary search
        while (true) {
            middle = (left + right) / 2;
            if (comparator.compare(keys[middle], key) == -1) {
                left = middle + 1;
            } else {
                right = middle;
            }
            if (left >= right) {
                return right;
            }
        }
    }

}
