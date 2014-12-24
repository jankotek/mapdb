package org.mapdb;


import org.junit.Test;

import java.io.File;
import java.util.Arrays;

import static org.junit.Assert.*;

@SuppressWarnings({"rawtypes","unchecked"})
public class StoreWALTest<E extends StoreWAL> extends StoreCachedTest<E>{

    @Override boolean canRollback(){return true;}

    File f = UtilsTest.tempDbFile();


    @Override protected E openEngine() {
        StoreWAL e =new StoreWAL(f.getPath());
        e.init();
        return (E)e;
    }



    @Test
    public void WAL_created(){
        File wal0 = new File(f.getPath()+".0.wal");
        File wal1 = new File(f.getPath()+".1.wal");
        File wal2 = new File(f.getPath()+".2.wal");

        StoreWAL w = openEngine();

        assertTrue(wal0.exists());
        assertTrue(wal0.length()>16);
        assertFalse(wal1.exists());

        w.put("aa",Serializer.STRING);
        w.commit();
        assertTrue(wal0.exists());
        assertTrue(wal0.length()>16);
        assertTrue(wal1.exists());
        assertTrue(wal1.length()>16);
        assertFalse(wal2.exists());

        w.put("aa",Serializer.STRING);
        w.commit();
        assertTrue(wal0.exists());
        assertTrue(wal0.length() > 16);
        assertTrue(wal1.exists());
        assertTrue(wal1.length() > 16);
        assertTrue(wal2.exists());
    }

    @Test public void WAL_replay_long(){
        StoreWAL e = openEngine();
        long v = e.composeIndexVal(1000, e.round16Up(10000), true, true, true);
        long offset = 0xF0000;
        e.walPutLong(offset,v);
        e.commit();
        e.structuralLock.lock();
        e.commitLock.lock();
        e.replayWAL();
        assertEquals(v,e.vol.getLong(offset));
    }

    @Test public void WAL_replay_mixed(){
        StoreWAL e = openEngine();
        e.structuralLock.lock();

        for(int i=0;i<3;i++) {
            long v = e.composeIndexVal(100+i, e.round16Up(10000)+i*16, true, true, true);
            e.walPutLong(0xF0000+i*8, v);
            byte[] d  = new byte[9];
            Arrays.fill(d, (byte) i);
            e.putDataSingleWithoutLink(-1,e.round16Up(100000)+64+i*16,d,0,d.length);
        }
        e.commit();
        e.structuralLock.lock();
        e.commitLock.lock();
        e.replayWAL();

        for(int i=0;i<3;i++) {
            long v = e.composeIndexVal(100+i, e.round16Up(10000)+i*16, true, true, true);
            assertEquals(v, e.vol.getLong(0xF0000+i*8));

            byte[] d  = new byte[9];
            Arrays.fill(d, (byte) i);
            byte[] d2  = new byte[9];

            e.vol.getData(e.round16Up(100000)+64+i*16,d2,0,d2.length);
            assertArrayEquals(d,d2);
        }

    }

}
