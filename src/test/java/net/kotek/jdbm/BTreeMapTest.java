package net.kotek.jdbm;

import org.junit.*;
import static org.junit.Assert.*;
import java.io.*;
import java.util.*;

public class BTreeMapTest extends JdbmTestCase{


    public static void print(BTreeMap m) {
        printRecur(m, m.rootRecid, "");
    }

    private static void printRecur(BTreeMap m, long recid, String s) {
        if(s.length()>100) throw new InternalError();
        BTreeMap.BNode n = (BTreeMap.BNode) m.recman.recordGet(recid, m.nodeSerializer);
        System.out.println(s+recid+"-"+n);
        if(!n.isLeaf()){
            for(int i=0;i<n.child().length-1;i++){
                long recid2 = n.child()[i];
                if(recid2!=0)
                    printRecur(m, recid2, s+"  ");
            }
        }
    }


    @Test public void test_leaf_node_serialization() throws IOException {
        BTreeMap m = new BTreeMap(recman, 32,true);

        BTreeMap.LeafNode n = new BTreeMap.LeafNode(new Object[]{1,2,3, BTreeMap.POS_INFINITY}, new Object[]{1,2,3,null}, 111);
        BTreeMap.LeafNode n2 = (BTreeMap.LeafNode) JdbmUtil.clone(n, m.nodeSerializer);
        assertArrayEquals(n.keys(), n2.keys());
        assertEquals(n.next, n2.next);
    }

    @Test public void test_dir_node_serialization() throws IOException {
        BTreeMap m = new BTreeMap(recman,32,true);

        BTreeMap.DirNode n = new BTreeMap.DirNode(new Object[]{1,2,3, BTreeMap.POS_INFINITY}, new long[]{4,5,6,7});
        BTreeMap.DirNode n2 = (BTreeMap.DirNode) JdbmUtil.clone(n, m.nodeSerializer);

        assertArrayEquals(n.keys(), n2.keys());
        assertArrayEquals(n.child, n2.child);
    }

    @Test public void test_find_children(){
        BTreeMap m = new BTreeMap(recman,32,true);
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
        BTreeMap m = new BTreeMap(recman,32,true);
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
        BTreeMap m = new BTreeMap(recman,32,true);
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
        BTreeMap m = new BTreeMap(recman,32,true);
        BTreeMap.LeafNode l = new BTreeMap.LeafNode(
                new Object[]{BTreeMap.NEG_INFINITY, 10,20,30, BTreeMap.POS_INFINITY},
                new Object[]{null, 10,20,30, null},
                0);
        m.rootRecid = recman.recordPut(l, m.nodeSerializer);

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
        BTreeMap m = new BTreeMap(recman,32,true);

        BTreeMap.LeafNode n3 = new BTreeMap.LeafNode(new Object[]{60,70,BTreeMap.POS_INFINITY}, new Object[]{60,70, null}, 0);
        long r3 = recman.recordPut(n3, m.nodeSerializer);

        BTreeMap.LeafNode n2 = new BTreeMap.LeafNode(new Object[]{40,50}, new Object[]{40,50}, r3);
        long r2 = recman.recordPut(n2, m.nodeSerializer);

        BTreeMap.LeafNode n1 = new BTreeMap.LeafNode(new Object[]{BTreeMap.NEG_INFINITY, 10,20,30}, new Object[]{null, 10,20,30}, r2);
        long r1 = recman.recordPut(n1, m.nodeSerializer);

        BTreeMap.DirNode d = new BTreeMap.DirNode(new Object[]{BTreeMap.NEG_INFINITY, 60, BTreeMap.POS_INFINITY},
                new long[]{r1,r3,0});

        m.rootRecid = recman.recordPut(d, m.nodeSerializer);

        for(int i=1;i<79;i++){
            Integer expected = i%10==0 ? i : null;
            assertEquals(expected, m.get(i));
        }
    }

    @Test public void root_leaf_insert(){
        BTreeMap m = new BTreeMap(recman,6,true);
        m.put(11,12);
        BTreeMap.LeafNode n = (BTreeMap.LeafNode) recman.recordGet(m.rootRecid, m.nodeSerializer);
        assertArrayEquals(new Object[]{BTreeMap.NEG_INFINITY, 11, BTreeMap.POS_INFINITY}, n.keys);
        assertArrayEquals(new Object[]{null, 12, null}, n.vals);
        assertEquals(0, n.next);
    }

    @Test public void batch_insert(){
        BTreeMap m = new BTreeMap(recman,6,true);

        for(int i=0;i<1000;i++){
            m.put(i*10,i*10+1);
        }

        for(int i=0;i<10000;i++){
            assertEquals(i%10==0?i+1:null, m.get(i));
        }
    }

    @Test public void test_empty_iterator(){
        BTreeMap m = new BTreeMap(recman,6,true);
        assertFalse(m.keySet().iterator().hasNext());
        assertFalse(m.values().iterator().hasNext());
    }

    @Test public void test_key_iterator(){
        BTreeMap m = new BTreeMap(recman,6,true);
        for(int i = 0;i<20;i++){
            m.put(i,i*10);
        }

        Iterator iter = m.keySet().iterator();

        for(int i = 0;i<20;i++){
            assertTrue(iter.hasNext());
            assertEquals(i,iter.next());
        }
        assertFalse(iter.hasNext());
    }

    @Test public void test_size(){
        BTreeMap m = new BTreeMap(recman,6,true);
        assertTrue(m.isEmpty());
        assertEquals(0,m.size());
        for(int i = 1;i<30;i++){
            m.put(i,i);
            assertEquals(i,m.size());
            assertFalse(m.isEmpty());
        }
    }

    @Test public void delete(){
        BTreeMap m = new BTreeMap(recman,6,true);
        for(int i:new int[]{
                10, 50, 20, 42,
                //44, 68, 20, 93, 85, 71, 62, 77, 4, 37, 66
        }){
            m.put(i,i);
        }
        assertEquals(10, m.remove(10));
        assertEquals(20, m.remove(20));
        assertEquals(42, m.remove(42));
    }


}


