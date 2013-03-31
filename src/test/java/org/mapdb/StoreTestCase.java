package org.mapdb;


import org.junit.After;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mapdb.Serializer.BYTE_ARRAY_SERIALIZER;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * JUnit test case which provides JDBM specific staff
 */
abstract public class StoreTestCase extends TestFile{




    Engine engine = openEngine();

    @After
    public void tearDown() throws Exception {
        if(engine!=null && !engine.isClosed())
            engine.close();
    }


    protected Engine openEngine() {
        return new StoreDirect(fac);
    }


    void reopenStore() {
        engine.close();
        engine = openEngine();
    }


    DataInput2 swap(DataOutput2 d){
        byte[] b = d.copyBytes();
        return new DataInput2(ByteBuffer.wrap(b),0);
    }



    @Test public void testSetGet(){
        long recid  = engine.put((long) 10000, Serializer.LONG_SERIALIZER);
        Long  s2 = engine.get(recid, Serializer.LONG_SERIALIZER);
        assertEquals(s2, Long.valueOf(10000));
    }


    final List<Long> arrayList(long... vals){
        ArrayList<Long> ret = new ArrayList<Long>();
        for(Long l:vals){
            ret.add(l);
        }
        return ret;
    }



    @Test
    public void large_record(){
        byte[] b = new byte[100000];
        Arrays.fill(b, (byte) 111);
        long recid = engine.put(b, BYTE_ARRAY_SERIALIZER);
        byte[] b2 = engine.get(recid, BYTE_ARRAY_SERIALIZER);
        assertArrayEquals(b,b2);
    }

    @Test public void large_record_update(){
        byte[] b = new byte[100000];
        Arrays.fill(b, (byte) 111);
        long recid = engine.put(b, BYTE_ARRAY_SERIALIZER);
        Arrays.fill(b, (byte)222);
        engine.update(recid, b, BYTE_ARRAY_SERIALIZER);
        byte[] b2 = engine.get(recid, BYTE_ARRAY_SERIALIZER);
        assertArrayEquals(b,b2);
        engine.commit();
        reopenStore();
        b2 = engine.get(recid, BYTE_ARRAY_SERIALIZER);
        assertArrayEquals(b,b2);
    }

    @Test public void large_record_delete(){
        byte[] b = new byte[100000];
        Arrays.fill(b, (byte) 111);
        long recid = engine.put(b, BYTE_ARRAY_SERIALIZER);
        engine.delete(recid,BYTE_ARRAY_SERIALIZER);
    }


    @Test public void large_record_larger(){
        byte[] b = new byte[10000000];
        Arrays.fill(b, (byte) 111);
        long recid = engine.put(b, BYTE_ARRAY_SERIALIZER);
        byte[] b2 = engine.get(recid, BYTE_ARRAY_SERIALIZER);
        assertArrayEquals(b,b2);
        engine.commit();
        reopenStore();
        b2 = engine.get(recid, BYTE_ARRAY_SERIALIZER);
        assertArrayEquals(b,b2);

    }


    @Test public void test_store_reopen(){
        long recid = engine.put("aaa", Serializer.STRING_SERIALIZER);
        engine.commit();
        reopenStore();

        String aaa = engine.get(recid, Serializer.STRING_SERIALIZER);
        assertEquals("aaa",aaa);
    }


}
