package org.mapdb;


import junit.framework.AssertionFailedError;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.*;
import static org.mapdb.Serializer.BYTE_ARRAY_NOSIZE;

/**
 * Tests contract of various implementations of Engine interface
 */
public abstract class EngineTest<ENGINE extends Engine>{

    protected abstract ENGINE openEngine();

    void reopen(){
        if(!canReopen()) return;
        e.close();
        e=openEngine();
    }

    boolean canReopen(){return true;}
    boolean canRollback(){return true;}

    ENGINE e;
    @Before public void init(){
        e = openEngine();
    }

    @Test public void put_get(){
        Long l = 11231203099090L;
        long recid = e.put(l, Serializer.LONG);
        assertEquals(l, e.get(recid, Serializer.LONG));
    }

    @Test public void put_reopen_get(){
        if(!canReopen()) return;
        Long l = 11231203099090L;
        long recid = e.put(l, Serializer.LONG);
        e.commit();
        reopen();
        assertEquals(l, e.get(recid, Serializer.LONG));
    }

    @Test public void put_get_large(){
        byte[] b = new byte[(int) 1e6];
        new Random().nextBytes(b);
        long recid = e.put(b, Serializer.BYTE_ARRAY_NOSIZE);
        assertArrayEquals(b, e.get(recid, Serializer.BYTE_ARRAY_NOSIZE));
    }

    @Test public void put_reopen_get_large(){
        if(!canReopen()) return;
        byte[] b = new byte[(int) 1e6];
        new Random().nextBytes(b);
        long recid = e.put(b, Serializer.BYTE_ARRAY_NOSIZE);
        e.commit();
        reopen();
        assertArrayEquals(b, e.get(recid, Serializer.BYTE_ARRAY_NOSIZE));
    }


    @Test public void first_recid(){
        assertEquals(Store.RECID_LAST_RESERVED+1, e.put(1,Serializer.INTEGER));
    }


    @Test public void compact0(){
        Long v1 = 129031920390121423L;
        Long v2 = 909090901290129990L;
        Long v3 = 998898989L;
        long recid1 = e.put(v1, Serializer.LONG);
        long recid2 = e.put(v2, Serializer.LONG);

        e.commit();
        e.compact();

        assertEquals(v1, e.get(recid1,Serializer.LONG));
        assertEquals(v2, e.get(recid2,Serializer.LONG));
        long recid3 = e.put(v3, Serializer.LONG);
        assertEquals(v1, e.get(recid1,Serializer.LONG));
        assertEquals(v2, e.get(recid2,Serializer.LONG));
        assertEquals(v3, e.get(recid3,Serializer.LONG));
        e.commit();
        assertEquals(v1, e.get(recid1,Serializer.LONG));
        assertEquals(v2, e.get(recid2,Serializer.LONG));
        assertEquals(v3, e.get(recid3,Serializer.LONG));

    }


    @Test public void compact(){
        Map<Long,Long> recids = new HashMap<Long, Long>();
        for(Long l=0L;l<1000;l++){
            recids.put(l,
                    e.put(l, Serializer.LONG));
        }

        e.commit();
        e.compact();

        for(Map.Entry<Long,Long> m:recids.entrySet()){
            Long recid= m.getValue();
            Long value = m.getKey();
            assertEquals(value, e.get(recid, Serializer.LONG));
        }


    }


    @Test public void compact2(){
        Map<Long,Long> recids = new HashMap<Long, Long>();
        for(Long l=0L;l<1000;l++){
            recids.put(l,
                    e.put(l, Serializer.LONG));
        }

        e.commit();
        e.compact();
        for(Long l=1000L;l<2000;l++){
            recids.put(l, e.put(l, Serializer.LONG));
        }

        for(Map.Entry<Long,Long> m:recids.entrySet()){
            Long recid= m.getValue();
            Long value = m.getKey();
            assertEquals(value, e.get(recid, Serializer.LONG));
        }
    }


    @Test public void compact_large_record(){
        byte[] b = new byte[100000];
        long recid = e.put(b, Serializer.BYTE_ARRAY_NOSIZE);
        e.commit();
        e.compact();
        assertArrayEquals(b, e.get(recid, Serializer.BYTE_ARRAY_NOSIZE));
    }


    @Test public void testSetGet(){
        long recid  = e.put((long) 10000, Serializer.LONG);
        Long  s2 = e.get(recid, Serializer.LONG);
        assertEquals(s2, Long.valueOf(10000));
    }



    @Test
    public void large_record(){
        byte[] b = new byte[100000];
        Arrays.fill(b, (byte) 111);
        long recid = e.put(b, BYTE_ARRAY_NOSIZE);
        byte[] b2 = e.get(recid, BYTE_ARRAY_NOSIZE);
        assertArrayEquals(b,b2);
    }

    @Test public void large_record_update(){
        byte[] b = new byte[100000];
        Arrays.fill(b, (byte) 111);
        long recid = e.put(b, BYTE_ARRAY_NOSIZE);
        Arrays.fill(b, (byte)222);
        e.update(recid, b, BYTE_ARRAY_NOSIZE);
        byte[] b2 = e.get(recid, BYTE_ARRAY_NOSIZE);
        assertArrayEquals(b,b2);
        e.commit();
        reopen();
        b2 = e.get(recid, BYTE_ARRAY_NOSIZE);
        assertArrayEquals(b,b2);
    }

    @Test public void large_record_delete(){
        byte[] b = new byte[100000];
        Arrays.fill(b, (byte) 111);
        long recid = e.put(b, BYTE_ARRAY_NOSIZE);
        e.delete(recid, BYTE_ARRAY_NOSIZE);
    }


    @Test public void large_record_larger(){
        byte[] b = new byte[10000000];
        Arrays.fill(b, (byte) 111);
        long recid = e.put(b, BYTE_ARRAY_NOSIZE);
        byte[] b2 = e.get(recid, BYTE_ARRAY_NOSIZE);
        assertArrayEquals(b,b2);
        e.commit();
        reopen();
        b2 = e.get(recid, BYTE_ARRAY_NOSIZE);
        assertArrayEquals(b,b2);

    }


    @Test public void test_store_reopen(){
        long recid = e.put("aaa", Serializer.STRING_NOSIZE);
        e.commit();
        reopen();

        String aaa = e.get(recid, Serializer.STRING_NOSIZE);
        assertEquals("aaa",aaa);
    }

    @Test public void test_store_reopen_nocommit(){
        long recid = e.put("aaa", Serializer.STRING_NOSIZE);
        e.commit();
        e.update(recid,"bbb",Serializer.STRING_NOSIZE);
        reopen();

        String expected = canRollback()&&canReopen()?"aaa":"bbb";
        assertEquals(expected, e.get(recid, Serializer.STRING_NOSIZE));
    }


    @Test public void rollback(){
        long recid = e.put("aaa", Serializer.STRING_NOSIZE);
        e.commit();
        e.update(recid, "bbb", Serializer.STRING_NOSIZE);

        if(!canRollback())return;
        e.rollback();

        assertEquals("aaa",e.get(recid, Serializer.STRING_NOSIZE));

    }

    @Test public void rollback_reopen(){
        long recid = e.put("aaa", Serializer.STRING_NOSIZE);
        e.commit();
        e.update(recid, "bbb", Serializer.STRING_NOSIZE);

        if(!canRollback())return;
        e.rollback();

        assertEquals("aaa",e.get(recid, Serializer.STRING_NOSIZE));
        reopen();
        assertEquals("aaa",e.get(recid, Serializer.STRING_NOSIZE));
    }

    /** after deletion it enters preallocated state */
    @Test public void delete_and_get(){
        long recid = e.put("aaa", Serializer.STRING);
        e.delete(recid,Serializer.STRING);
        assertNull(e.get(recid,Serializer.ILLEGAL_ACCESS));
        long recid2 = e.put("bbb", Serializer.STRING);
        assertNotEquals(recid,recid2);
    }

    @Test public void get_non_existent(){
        long recid = Engine.RECID_FIRST;
        try{
            e.get(recid,Serializer.ILLEGAL_ACCESS);
            fail();
        }catch(DBException e){
            assertEquals(DBException.Code.ENGINE_GET_VOID, e.getCode());
        }
    }

    @Test public void get_non_existent_after_delete_and_compact(){
        long recid = e.put(1L,Serializer.LONG);
        e.delete(recid,Serializer.LONG);
        assertNull(e.get(recid,Serializer.ILLEGAL_ACCESS));
        e.commit();
        e.compact();
        try{
            e.get(recid,Serializer.STRING);
            if(!(e instanceof StoreAppend)) //TODO remove after compact on StoreAppend
                fail();
        }catch(DBException e){
            assertEquals(DBException.Code.ENGINE_GET_VOID, e.getCode());
        }
    }

    @Test public void preallocate_cas(){
        long recid = e.preallocate();
        assertFalse(e.compareAndSwap(recid,1L,2L,Serializer.ILLEGAL_ACCESS));
        assertTrue(e.compareAndSwap(recid,null,2L,Serializer.LONG));
        assertEquals((Long)2L, e.get(recid,Serializer.LONG));
    }


    @Test public void preallocate_get_update_delete_update_get(){
        long recid = e.preallocate();
        assertNull(e.get(recid,Serializer.ILLEGAL_ACCESS));
        e.update(recid,1L, Serializer.LONG);
        assertEquals((Long)1L, e.get(recid,Serializer.LONG));
        e.delete(recid,Serializer.LONG);
        assertNull(e.get(recid,Serializer.ILLEGAL_ACCESS));
        e.update(recid,1L, Serializer.LONG);
        assertEquals((Long)1L, e.get(recid,Serializer.LONG));
    }

    @Test public void cas_delete(){
        long recid = e.put(1L,Serializer.LONG);
        assertTrue(e.compareAndSwap(recid,1L,null,Serializer.LONG));
        assertNull(e.get(recid,Serializer.ILLEGAL_ACCESS));
    }

    @Test public void reserved_recid_exists(){
        for(long recid=1;recid<Engine.RECID_FIRST;recid++){
            assertNull(e.get(recid,Serializer.ILLEGAL_ACCESS));
        }
        try{
            e.get(Engine.RECID_FIRST,Serializer.ILLEGAL_ACCESS);
            fail();
        }catch(DBException e){
            assertEquals(DBException.Code.ENGINE_GET_VOID, e.getCode());
        }

    }



    @Test(expected = NullPointerException.class)
    public void NPE_get(){
        e.get(1,null);
    }

    @Test(expected = NullPointerException.class)
    public void NPE_put(){
        e.put(1L,null);
    }

    @Test(expected = NullPointerException.class)
    public void NPE_update(){
        e.update(1,1L, null);
    }

    @Test(expected = NullPointerException.class)
    public void NPE_cas(){
        e.compareAndSwap(1,1L, 1L,  null);
    }

    @Test(expected = NullPointerException.class)
    public void NPE_delete(){
        e.delete(1L, null);
    }

    @Test public void putGetUpdateDelete(){
        Engine st = openEngine();
        String s = "aaaad9009";
        long recid = st.put(s,Serializer.STRING);

        assertEquals(s,st.get(recid,Serializer.STRING));

        s = "da8898fe89w98fw98f9";
        st.update(recid,s,Serializer.STRING);
        assertEquals(s,st.get(recid,Serializer.STRING));

        st.delete(recid,Serializer.STRING);
        assertNull(st.get(recid, Serializer.STRING));
    }


    @Test public void zero_size_serializer(){
        Serializer s = new Serializer<String>() {

            @Override
            public void serialize(DataOutput out, String value) throws IOException {
                if("".equals(value))
                    return;
                Serializer.STRING.serialize(out,value);
            }

            @Override
            public String deserialize(DataInput in, int available) throws IOException {
                if(available==0)
                    return "";
                return Serializer.STRING.deserialize(in,available);
            }
        };

        Engine e = openEngine();
        long recid = e.put("", s);
        assertEquals("",e.get(recid,s));

        e.update(recid, "a", s);
        assertEquals("a",e.get(recid,s));

        e.compareAndSwap(recid,"a","", s);
        assertEquals("",e.get(recid,s));


        e.update(recid, "a", s);
        assertEquals("a",e.get(recid,s));

        e.update(recid,"", s);
        assertEquals("",e.get(recid,s));

    }
}
