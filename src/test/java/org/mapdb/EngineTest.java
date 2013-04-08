package org.mapdb;


import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Tests contract of various implementations of Engine interface
 */
public abstract class EngineTest<ENGINE extends Engine>{

    protected abstract ENGINE openEngine();

    void reopen(){
        e.close();
        e=openEngine();
    }

    ENGINE e;
    @Before public void init(){
        e = openEngine();
    }

    @Test public void put_get(){
        Long l = 11231203099090L;
        long recid = e.put(l, Serializer.LONG_SERIALIZER);
        assertEquals(l, e.get(recid, Serializer.LONG_SERIALIZER));
    }

    @Test public void put_reopen_get(){
        Long l = 11231203099090L;
        long recid = e.put(l, Serializer.LONG_SERIALIZER);
        e.commit();
        reopen();
        assertEquals(l, e.get(recid, Serializer.LONG_SERIALIZER));
    }

    @Test public void put_get_large(){
        byte[] b = new byte[(int) 1e6];
        Utils.RANDOM.nextBytes(b);
        long recid = e.put(b, Serializer.BYTE_ARRAY_SERIALIZER);
        assertArrayEquals(b, e.get(recid, Serializer.BYTE_ARRAY_SERIALIZER));
    }

    @Test public void put_reopen_get_large(){
        byte[] b = new byte[(int) 1e6];
        Utils.RANDOM.nextBytes(b);
        long recid = e.put(b, Serializer.BYTE_ARRAY_SERIALIZER);
        e.commit();
        reopen();
        assertArrayEquals(b, e.get(recid, Serializer.BYTE_ARRAY_SERIALIZER));
    }


    @Test public void first_recid(){
        assertEquals(Engine.LAST_RESERVED_RECID+1, e.put(1,Serializer.INTEGER_SERIALIZER));
    }


    @Test public void compact0(){
        Long v1 = 129031920390121423L;
        Long v2 = 909090901290129990L;
        Long v3 = 998898989L;
        long recid1 = e.put(v1, Serializer.LONG_SERIALIZER);
        long recid2 = e.put(v2, Serializer.LONG_SERIALIZER);

        e.commit();
        e.compact();

        assertEquals(v1, e.get(recid1,Serializer.LONG_SERIALIZER));
        assertEquals(v2, e.get(recid2,Serializer.LONG_SERIALIZER));
        long recid3 = e.put(v3, Serializer.LONG_SERIALIZER);
        assertEquals(v1, e.get(recid1,Serializer.LONG_SERIALIZER));
        assertEquals(v2, e.get(recid2,Serializer.LONG_SERIALIZER));
        assertEquals(v3, e.get(recid3,Serializer.LONG_SERIALIZER));
        e.commit();
        assertEquals(v1, e.get(recid1,Serializer.LONG_SERIALIZER));
        assertEquals(v2, e.get(recid2,Serializer.LONG_SERIALIZER));
        assertEquals(v3, e.get(recid3,Serializer.LONG_SERIALIZER));

    }


    @Test public void compact(){
        Map<Long,Long> recids = new HashMap<Long, Long>();
        for(Long l=0L;l<1000;l++){
            recids.put(l,
                    e.put(l, Serializer.LONG_SERIALIZER));
        }

        e.commit();
        e.compact();

        for(Map.Entry<Long,Long> m:recids.entrySet()){
            Long recid= m.getValue();
            Long value = m.getKey();
            assertEquals(value, e.get(recid, Serializer.LONG_SERIALIZER));
        }


    }


    @Test public void compact2(){
        Map<Long,Long> recids = new HashMap<Long, Long>();
        for(Long l=0L;l<1000;l++){
            recids.put(l,
                    e.put(l, Serializer.LONG_SERIALIZER));
        }

        e.commit();
        e.compact();
        for(Long l=1000L;l<2000;l++){
            recids.put(l, e.put(l, Serializer.LONG_SERIALIZER));
        }

        for(Map.Entry<Long,Long> m:recids.entrySet()){
            Long recid= m.getValue();
            Long value = m.getKey();
            assertEquals(value, e.get(recid, Serializer.LONG_SERIALIZER));
        }
    }


    @Test public void compact_large_record(){
        byte[] b = new byte[100000];
        long recid = e.put(b, Serializer.BYTE_ARRAY_SERIALIZER);
        e.commit();
        e.compact();
        assertArrayEquals(b, e.get(recid, Serializer.BYTE_ARRAY_SERIALIZER));
    }

}
