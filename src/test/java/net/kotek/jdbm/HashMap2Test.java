package net.kotek.jdbm;

import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class HashMap2Test extends JdbmTestCase {


    void printMap(HashMap2 m){
        System.out.println(toString(m.segmentRecids, recman));
    }

    static String toString(long[] rootRecids, RecordStore recman){
        String s = "Arrays.asList(\n";
        for(long r:rootRecids){
            s+= (r==0)?null:recursiveToString(r,"", recman);
        }
        //s=s.substring(0,s.length()-2);
        s+=");";
        return s;
    }

    static private String recursiveToString(long r, String prefix, RecordStore recman) {
        prefix+="  ";
        String s="";
        long[][] nn = recman.recordGet(r, HashMap2.DIR_SERIALIZER);
        if(nn==null){
            s+=prefix+"null,\n";
        }else{
            s+= prefix+"Arrays.asList(\n";
        for(long[] n:nn){
            if(n==null){
                s+=prefix+"  null,\n";
            }else{
                s+=prefix+"  Arrays.asList(\n";
                for(long r2:n){
                    if(r2==0){
                        s+=prefix+"    "+"null,\n";
                    }else{
                        if((r2&1)==0){
                            s+=recursiveToString(r2>>>1, prefix+"    ", recman);
                        }else{
                            s+=prefix+"    "+"Array.asList(";
                            TreeMap m = new TreeMap();
                            HashMap2.LinkedNode node =
                                    (HashMap2.LinkedNode) recman.recordGet
                                            (r2>>>1,new HashMap2(recman,true).LN_SERIALIZER );
                            while(node!=null){
                                m.put(node.key, node.value);
                                node = (HashMap2.LinkedNode) recman.recordGet (node.next,new HashMap2(recman,true).LN_SERIALIZER );
                            }
                            for(Object k:m.keySet()){
                                s+= k+","+m.get(k)+",";
                            }
                            //s=s.substring(0,s.length()-1);
                            s+="),\n";
                        }
                    }
                }
                s+=prefix+"  ),\n";
            }
        }
//            s=s.substring(0,s.length()-2);
            s+=prefix+"),\n";
        }
        return s;
    }


    @Test public void testDirSerializer() throws IOException {

        long[][] l = new long[16][];
        l[3] = new long[] {0,0,12,13,14,0,Long.MAX_VALUE,0};
        l[6] = new long[] {1,2,3,4,5,6,7,8};

        DataOutput2 out = new DataOutput2();
        HashMap2.DIR_SERIALIZER.serialize(out,l);

        DataInput2 in = swap(out);

        long[][] b = HashMap2.DIR_SERIALIZER.deserialize(in, -1);

        assertEquals(null, b[0]);
        assertEquals(null, b[1]);
        assertEquals(null, b[2]);
        assertEquals(Arrays.toString(new long[] {0,0,12,13,14,0,Long.MAX_VALUE,0}), Arrays.toString(b[3]));
        assertEquals(null, b[4]);
        assertEquals(null, b[5]);
        assertEquals(Arrays.toString(new long[] {1,2,3,4,5,6,7,8}), Arrays.toString(b[6]));
        assertEquals(null, b[7]);

    }

    @Test public void ln_serialization() throws IOException {
        HashMap2.LinkedNode n = new HashMap2.LinkedNode(123456, 123L, 456L);

        DataOutput2 out = new DataOutput2();

        Serializer ln_serializer = new HashMap2(recman, true).LN_SERIALIZER;
        ln_serializer.serialize(out, n);

        DataInput2 in = swap(out);

        HashMap2.LinkedNode n2  = (HashMap2.LinkedNode) ln_serializer.deserialize(in, -1);

        assertEquals(123456, n2.next);
        assertEquals(123L,n2.key);
        assertEquals(456L,n2.value);
    }

    @Test public void test_simple_put(){

        HashMap2  m = new HashMap2(recman,true);

        m.put(111L, 222L);
        m.put(333L, 444L);
        assertTrue(m.containsKey(111L));
        assertTrue(!m.containsKey(222L));
        assertTrue(m.containsKey(333L));
        assertTrue(!m.containsKey(444L));

        assertEquals(222L, m.get(111L));
        assertEquals(null, m.get(222L));
        assertEquals(444l, m.get(333L));
    }

    @Test public void test_hash_collision(){
        HashMap2 m = new HashMap2(recman,true){
            @Override
            protected int hash(Object key) {
                return 0;
            }
        };

        for(long i = 0;i<20;i++){
            m.put(i,i+100);
        }

        for(long i = 0;i<20;i++){
            assertTrue(m.containsKey(i));
            assertEquals(i+100, m.get(i));
        }

        m.put(11L, 1111L);
        assertEquals(1111L,m.get(11L) );
    }

    @Test public void test_hash_dir_expand(){
        HashMap2 m = new HashMap2(recman,true){
            @Override
            protected int hash(Object key) {
                return 0;
            }
        };

        for(long i = 0;i<HashMap2.BUCKET_OVERFLOW;i++){
            m.put(i,i);
        }

        //segment should not be expanded
        long[][] l = recman.recordGet(m.segmentRecids[0],HashMap2.DIR_SERIALIZER);
        assertNotNull(l[0]);
        assertEquals(1, l[0][0]&1);  //last bite indicates leaf
        for(int j=1;j<8;j++){ //all others should be null
            assertEquals(0, l[0][j]);
        }
        long recid = l[0][0]>>>1;

        for(long i = HashMap2.BUCKET_OVERFLOW -1; i>=0; i--){
            assertTrue(recid!=0);
            HashMap2.LinkedNode  n = (HashMap2.LinkedNode) recman.recordGet(recid, m.LN_SERIALIZER);
            assertEquals(i, n.key);
            assertEquals(i, n.value);
            recid = n.next;
        }

        //adding one more item should trigger dir expansion to next level
        m.put((long)HashMap2.BUCKET_OVERFLOW, (long)HashMap2.BUCKET_OVERFLOW);

        recid = m.segmentRecids[0];

        l = recman.recordGet(recid, HashMap2.DIR_SERIALIZER);
        assertNotNull(l[0]);
        for(int j=1;j<8;j++){ //all others should be null
           assertEquals(null, l[j]);
        }

        assertEquals(0, l[0][0]&1); //last bite indicates leaf
        for(int j=1;j<8;j++){ //all others should be zero
          assertEquals(0, l[0][j]);
        }

        recid = l[0][0]>>>1;


        l = recman.recordGet(recid, HashMap2.DIR_SERIALIZER);
        assertNotNull(l[0]);
        for(int j=1;j<8;j++){ //all others should be null
            assertEquals(null, l[j]);
        }

        assertEquals(1, l[0][0]&1); //last bite indicates leaf
        for(int j=1;j<8;j++){ //all others should be zero
            assertEquals(0, l[0][j]);
        }

        recid = l[0][0]>>>1;

        for(long i = 0; i<=HashMap2.BUCKET_OVERFLOW; i++){
            assertTrue(recid!=0);
            HashMap2.LinkedNode n = (HashMap2.LinkedNode) recman.recordGet(recid, m.LN_SERIALIZER);

            assertNotNull(n);
            assertEquals(i, n.key);
            assertEquals(i, n.value);
            recid = n.next;
        }

    }


    @Test public void test_delete(){
        HashMap2 m = new HashMap2(recman,true){
            @Override
            protected int hash(Object key) {
                return 0;
            }
        };

        for(long i = 0;i<20;i++){
            m.put(i,i+100);
        }

        for(long i = 0;i<20;i++){
            assertTrue(m.containsKey(i));
            assertEquals(i+100, m.get(i));
        }


        for(long i = 0;i<20;i++){
            m.remove(i);
        }

        for(long i = 0;i<20;i++){
            assertTrue(!m.containsKey(i));
            assertEquals(null, m.get(i));
        }
    }

    @Test public void clear(){
        HashMap2 m = new HashMap2(recman,true);
        for(Integer i=0;i<1e5;i++){
            m.put(i,i);
        }
        m.clear();
        assertTrue(m.isEmpty());
        assertEquals(0, m.size());
    }

    @Test //(timeout = 10000)
     public void testIteration(){
        HashMap2 m = new HashMap2<Integer, Integer>(recman, true){
            @Override
            protected int hash(Object key) {
                return ((Integer)key).intValue();
            }
        };

        final int max = 140;
        final int inc = 111111;

        for(Integer i=0;i<max;i++){
            m.put(i,i+inc);
        }

        Iterator<Integer> keys = m.keySet().iterator();
        for(Integer i=0;i<max;i++){
            assertTrue(keys.hasNext());
            assertEquals(i, keys.next());
        }
        assertTrue(!keys.hasNext());

        Iterator<Integer> vals = m.values().iterator();
        for(Integer i=inc;i<max+inc;i++){
            assertTrue(vals.hasNext());
            assertEquals(i, vals.next());
        }
        assertTrue(!vals.hasNext());


        //great it worked, test stuff spread across segments
        m.clear();
        assertTrue(m.isEmpty());

        for(int i = 0;i<max;i++){
            m.put((1<<30)+i, i+inc);
            m.put((2<<30)+i, i+inc);
            m.put((3<<30)+i, i+inc);
        }

        assertEquals(max*3,m.size());

        int countSegments = 0;
        for(long segmentRecid:m.segmentRecids){
            if(recman.recordGet(segmentRecid, HashMap2.DIR_SERIALIZER)!=null)
                countSegments++;
        }

        assertEquals(3, countSegments);

        keys = m.keySet().iterator();
        for(int i=1;i<=3;i++){
            for(int j=0;j<max;j++){
                assertTrue(keys.hasNext());
                assertEquals(Integer.valueOf((i<<30)+j), keys.next());
            }
        }
        assertTrue(!keys.hasNext());

    }

}

