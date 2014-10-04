package org.mapdb;


import org.junit.Before;
import org.junit.Test;

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
}
