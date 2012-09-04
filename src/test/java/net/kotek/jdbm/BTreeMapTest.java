package net.kotek.jdbm;

import org.junit.*;
import static org.junit.Assert.*;
import java.io.*;
import java.util.*;

public class BTreeMapTest extends JdbmTestCase{

    class TNode{
        List nodes;
        TNode(Object... args){
            this.nodes = Arrays.asList(args);
        }

        public String toString(){
            String ret = "new TNode(";
            Iterator i = nodes.iterator();
            while(i.hasNext()){
                Object next = i.next();
                if(next instanceof TNode){
                    ret+="  "+next.toString()+", ";
                }else{
                    ret+="  "+next.toString()+", ";
                }
            }
            ret+=")\n ";
            return ret;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TNode tNode = (TNode) o;

            if (nodes != null ? !nodes.equals(tNode.nodes) : tNode.nodes != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return nodes != null ? nodes.hashCode() : 0;
        }
    }

    void print(BTreeMap m){
        System.out.println(TNode(m));
    }

    TNode TNode(BTreeMap m){
        return TNodeRecur(m, m.rootRecid);
    }

    private TNode TNodeRecur(BTreeMap m, long recid) {
        TNode ret = new TNode();
        ret.nodes = new ArrayList();
        BTreeMap.BNode n = (BTreeMap.BNode) m.recman.recordGet(recid, m.NODE_SERIALIZER);
        for(int i=0;i<n.keys().length;i++){
            ret.nodes.add(n.keys()[i]);
            if(!n.isLeaf() && n.child()[i]!=0)
                ret.nodes.add(TNodeRecur(m,n.child()[i]));
        }
        return ret;
    }


    @Test public void test_leaf_node_serialization() throws IOException {
        BTreeMap m = new BTreeMap(recman, 32);

        BTreeMap.LeafNode n = new BTreeMap.LeafNode(new Object[]{1,2,3, BTreeMap.POS_INFINITY}, new Object[]{1,2,3,null}, 111);
        BTreeMap.LeafNode n2 = (BTreeMap.LeafNode) JdbmUtil.clone(n, m.NODE_SERIALIZER);
        assertArrayEquals(n.keys(), n2.keys());
        assertEquals(n.next, n2.next);
    }

    @Test public void test_dir_node_serialization() throws IOException {
        BTreeMap m = new BTreeMap(recman,32);

        BTreeMap.DirNode n = new BTreeMap.DirNode(new Object[]{1,2,3, BTreeMap.POS_INFINITY}, new long[]{4,5,6,7});
        BTreeMap.DirNode n2 = (BTreeMap.DirNode) JdbmUtil.clone(n, m.NODE_SERIALIZER);

        assertArrayEquals(n.keys(), n2.keys());
        assertArrayEquals(n.child, n2.child);
    }

    @Test public void test_find_children(){
        BTreeMap m = new BTreeMap(recman,32);
        assertEquals(8,m.findChildren(11, new Integer[]{1,2,3,4,5,6,7,8}));
        assertEquals(0,m.findChildren(1, new Integer[]{1,2,3,4,5,6,7,8}));
        assertEquals(0,m.findChildren(0, new Integer[]{1,2,3,4,5,6,7,8}));
        assertEquals(7,m.findChildren(8, new Integer[]{1,2,3,4,5,6,7,8}));
        assertEquals(4,m.findChildren(49, new Integer[]{10,20,30,40,50}));
        assertEquals(4,m.findChildren(50, new Integer[]{10,20,30,40,50}));
        assertEquals(3,m.findChildren(40, new Integer[]{10,20,30,40,50}));
        assertEquals(3,m.findChildren(39, new Integer[]{10,20,30,40,50}));
    }


    @Test public void test_next_dir(){
        BTreeMap m = new BTreeMap(recman,32);
        BTreeMap.DirNode d = new BTreeMap.DirNode(new Integer[]{44,62,68, 71}, new long[]{10,20,30,40});

        assertEquals(10, m.nextDir(d, 62));
        assertEquals(10, m.nextDir(d, 44));
        assertEquals(10, m.nextDir(d, 48));

        assertEquals(20, m.nextDir(d, 63));
        assertEquals(20, m.nextDir(d, 64));
        assertEquals(20, m.nextDir(d, 68));

        assertEquals(30, m.nextDir(d, 69));
        assertEquals(30, m.nextDir(d, 70));
        assertEquals(30, m.nextDir(d, 71));

        assertEquals(40, m.nextDir(d, 72));
        assertEquals(40, m.nextDir(d, 73));
    }

    @Test public void test_next_dir_infinity(){
        BTreeMap m = new BTreeMap(recman,32);
        BTreeMap.DirNode d = new BTreeMap.DirNode(
                new Object[]{BTreeMap.NEG_INFINITY,62,68, 71},
                new long[]{10,20,30,40});
        assertEquals(10, m.nextDir(d, 33));
        assertEquals(10, m.nextDir(d, 62));
        assertEquals(20, m.nextDir(d, 63));

        d = new BTreeMap.DirNode(
                new Object[]{44,62,68, BTreeMap.POS_INFINITY},
                new long[]{10,20,30,40});

        assertEquals(10, m.nextDir(d, 62));
        assertEquals(10, m.nextDir(d, 44));
        assertEquals(10, m.nextDir(d, 48));

        assertEquals(20, m.nextDir(d, 63));
        assertEquals(20, m.nextDir(d, 64));
        assertEquals(20, m.nextDir(d, 68));

        assertEquals(30, m.nextDir(d, 69));
        assertEquals(30, m.nextDir(d, 70));
        assertEquals(30, m.nextDir(d, 71));

        assertEquals(30, m.nextDir(d, 72));
        assertEquals(30, m.nextDir(d, 73));

    }

    @Test public void simple_root_get(){
        BTreeMap m = new BTreeMap(recman,32);
        BTreeMap.LeafNode l = new BTreeMap.LeafNode(
                new Object[]{BTreeMap.NEG_INFINITY, 10,20,30, BTreeMap.POS_INFINITY},
                new Object[]{null, 10,20,30, null},
                0);
        m.rootRecid = recman.recordPut(l, m.NODE_SERIALIZER);

        assertEquals(null, m.get(1));
        assertEquals(null, m.get(9));
        assertEquals(10, m.get(10));
        assertEquals(null, m.get(11));
        assertEquals(null, m.get(19));
        assertEquals(20, m.get(20));
        assertEquals(null, m.get(21));
        assertEquals(null, m.get(29));
        assertEquals(30, m.get(30));
        assertEquals(null, m.get(31));
    }

    @Test public void get_dive_link(){
        BTreeMap m = new BTreeMap(recman,32);

        BTreeMap.LeafNode n3 = new BTreeMap.LeafNode(new Object[]{60,70,BTreeMap.POS_INFINITY}, new Object[]{60,70, null}, 0);
        long r3 = recman.recordPut(n3, m.NODE_SERIALIZER);

        BTreeMap.LeafNode n2 = new BTreeMap.LeafNode(new Object[]{40,50}, new Object[]{40,50}, r3);
        long r2 = recman.recordPut(n2, m.NODE_SERIALIZER);

        BTreeMap.LeafNode n1 = new BTreeMap.LeafNode(new Object[]{BTreeMap.NEG_INFINITY, 10,20,30}, new Object[]{null, 10,20,30}, r2);
        long r1 = recman.recordPut(n1, m.NODE_SERIALIZER);

        BTreeMap.DirNode d = new BTreeMap.DirNode(new Object[]{BTreeMap.NEG_INFINITY, 60, BTreeMap.POS_INFINITY},
                new long[]{r1,r3,0});

        m.rootRecid = recman.recordPut(d, m.NODE_SERIALIZER);

        for(int i=1;i<79;i++){
            Integer expected = i%10==0 ? i : null;
            assertEquals(expected, m.get(i));
        }
    }

    @Test public void root_leaf_insert(){
        BTreeMap m = new BTreeMap(recman,6);
        m.put(11,12);
        BTreeMap.LeafNode n = (BTreeMap.LeafNode) recman.recordGet(m.rootRecid, m.NODE_SERIALIZER);
        assertArrayEquals(new Object[]{BTreeMap.NEG_INFINITY, 11, BTreeMap.POS_INFINITY}, n.keys);
        assertArrayEquals(new Object[]{null, 12, null}, n.vals);
        assertEquals(0, n.next);
    }

    @Test public void batch_insert(){
        BTreeMap m = new BTreeMap(recman,6);

        for(int i=0;i<1000;i++){
            m.put(i*10,i*10+1);
        }


        for(int i=0;i<10000;i++){
            assertEquals(i%10==0?i+1:null, m.get(i));
        }

    }

}

