package org.mapdb;


import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

@SuppressWarnings({"rawtypes","unchecked"})
public class StoreWALTest<E extends StoreWAL> extends StoreCachedTest<E>{

    @Override boolean canRollback(){return true;}

    File f = UtilsTest.tempDbFile();


    @Override protected E openEngine() {
        return (E) new StoreWAL(f.getPath());
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

}
