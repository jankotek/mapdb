package net.kotek.jdbm;

import org.junit.*;

import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

import java.io.*;
import java.util.Arrays;
import java.util.List;

public class BTreeMapTest extends JdbmTestCase{

    @Test public void test_leaf_node_serialization() throws IOException {
        BTreeMap m = new BTreeMap(recman);
        BTreeMap.BNode n = new BTreeMap.BNode();
        n.keys = new Object[]{Long.valueOf(1), Long.valueOf(2)};
        n.values = new Object[]{Long.valueOf(4), Long.valueOf(5)};

        n = (BTreeMap.BNode) JdbmUtil.clone(n, m.NODE_SERIALIZER);

        assertEquals(n.keys[0], 1L);
        assertEquals(n.keys[1], 2L);
        assertEquals(n.values[0], 4L);
        assertEquals(n.values[1], 5L);
        assertNull(n.refs);
    }

    @Test public void test_dir_node_serialization() throws IOException {
        BTreeMap m = new BTreeMap(recman);
        BTreeMap.BNode n = new BTreeMap.BNode();
        n.keys = new Object[]{Long.valueOf(1), Long.valueOf(2)};
        n.refs = new long[]{4, 5};

        n = (BTreeMap.BNode) JdbmUtil.clone(n, m.NODE_SERIALIZER);

        assertEquals(n.keys[0], 1L);
        assertEquals(n.keys[1], 2L);
        assertEquals(n.refs[0], 4L);
        assertEquals(n.refs[1], 5L);
        assertNull(n.values);
    }

    @Test public void test_single_level_insert(){
        BTreeMap m = new BTreeMap(recman);
        m.put(1L,2L);
        assertThat(2L, is(m.get(1L)));
    }

    @Test public void single_level__unordered_insert(){
        BTreeMap<Long,Long> m = new BTreeMap(recman);
        long[] d = new long[]{5L, 1L, 10L, 3L, 7L, 11L};
        for(long k:d) m.put(k,k+100);

        for(long k:d)
            assertThat(m.get(k), is(k+100));

        //check that value is updated when it equals
        m.put(5L, -1L);
        for(long k:d)
            assertThat(m.get(k), is(k==5L ? -1L : k+100));

    }


    @Test public void  put_value_into_node(){
        BTreeMap<Integer,Integer> m = new BTreeMap<Integer,Integer>(recman);

        BTreeMap.BNode n = new BTreeMap.BNode();
        n.keys = new Integer[]{10,20,30,40,50,60};
        n.values = new Integer[]{11,21,31,41,51,61};

        //test node splitting without expansion
        long recid = m.putValueIntoNode(20,22, n, true, 1, 0, 3, 0L );
        BTreeMap.BNode n2 = recman.recordGet(recid,m.NODE_SERIALIZER);
        assertEquals(asList(n2.keys), asList(10, 20, 30));
        assertEquals(asList(n2.values), asList(11, 22, 31));

        recid = m.putValueIntoNode(50,52, n, true, 4, 3, 6, 0L );
        n2 = recman.recordGet(recid,m.NODE_SERIALIZER);
        assertEquals(asList(n2.keys), asList(40, 50, 60));
        assertEquals(asList(n2.values), asList(41, 52, 61));

        //test node splitting with expansion
        recid = m.putValueIntoNode(25,27, n, false, 2, 0, 3, 0L );
        n2 = recman.recordGet(recid,m.NODE_SERIALIZER);
        assertEquals( asList(n2.keys), asList(10, 20, 25,30));
        assertEquals( asList(n2.values), asList(11, 21, 27, 31));

        recid = m.putValueIntoNode(55,57, n, false, 5, 3, 6, 0L );
        n2 = recman.recordGet(recid,m.NODE_SERIALIZER);
        assertEquals(asList(n2.keys), asList(40, 50, 55,  60));
        assertEquals( asList(n2.values), asList(41, 51, 57, 61));

        //test node splitting without changing value
        recid = m.putValueIntoNode(55,57,n,true,5,0,3,0L);
        n2 = recman.recordGet(recid,m.NODE_SERIALIZER);
        assertEquals( asList(n2.keys), asList(10, 20, 30));
        assertEquals( asList(n2.values), asList(11, 21, 31));
    }

    @Test public void root_basic_split(){
        BTreeMap<Integer,Integer> m = new BTreeMap<Integer,Integer>(recman);
        //first fill root node
        List keys = arrayList();
        List vals = arrayList();

        for(int i = 0;i<m.maxNodeSize;i++){
            m.put(i*10, i*10+1);
            keys.add(i*10);
            vals.add(i*10+1);
        }
        //check root before split
        BTreeMap.BNode root = recman.recordGet(m.rootRecid, m.NODE_SERIALIZER);
        assertEquals(keys, asList(root.keys));
        assertEquals(vals, asList(root.values));
        assertNull(root.refs);

        //add value into second half, it should trigger split
        m.put(1000,1001);
        keys.add(1000);
        vals.add(1001);

        root = recman.recordGet(m.rootRecid, m.NODE_SERIALIZER);
        assertEquals(2,root.keys.length);
        assertEquals(2,root.refs.length);
        assertNull(root.values);

        //check children
        BTreeMap.BNode n1 = recman.recordGet(root.refs[0], m.NODE_SERIALIZER);
        assertEquals( asList(n1.keys), keys.subList(0,m.maxNodeSize/2));
        assertEquals(asList(n1.values), vals.subList(0, m.maxNodeSize / 2));

        BTreeMap.BNode n2 = recman.recordGet(root.refs[1], m.NODE_SERIALIZER);
        assertEquals(asList(n2.keys), keys.subList(m.maxNodeSize / 2, keys.size()));
        assertEquals( asList(n2.values), vals.subList(m.maxNodeSize/2, keys.size()));
    }

    @Test public void root_triple_split(){
        BTreeMap<Integer,Integer> m = new BTreeMap<Integer,Integer>(recman);
        //first fill root node
        List keys = arrayList();
        List vals = arrayList();

        for(int i = 0;i<m.maxNodeSize*3;i++){
            m.put(i*10, i*10+1);
            keys.add(i*10);
            vals.add(i*10+1);
        }

        BTreeMap.BNode root = recman.recordGet(m.rootRecid, m.NODE_SERIALIZER);
        assertEquals(asList(0,m.maxNodeSize*10,m.maxNodeSize*20), asList(root.keys));
        assertEquals(3,root.refs.length);
        assertNull(root.values);

        for(int i=0;i<3;i++){
            long recid = root.refs[i];
            BTreeMap.BNode n = recman.recordGet(recid, m.NODE_SERIALIZER);
            //for(int j=0;j<m.maxNodeSize;j++){
        }

    }

}
