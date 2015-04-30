package org.mapdb;

import org.junit.Test;
import sun.misc.Unsafe;

import static org.junit.Assert.*;


/** delete this class if it fails to compile due to missign 'sun.misc.Unsafe' */
public class UnsafeStuffTest {

    Unsafe unsafe = null; //just add compilation time dependency

    @Test
    public void dbmaker(){
        DB db = DBMaker.memoryUnsafeDB().transactionDisable().make();

        StoreDirect s = (StoreDirect) Store.forDB(db);
        assertEquals(Volume.UNSAFE_VOL_FACTORY, s.volumeFactory);
        assertEquals(UnsafeStuff.UnsafeVolume.class, s.vol.getClass());
    }


    @Test
    public void factory(){
        Volume vol = Volume.UNSAFE_VOL_FACTORY.makeVolume(null, false);
        assertEquals(UnsafeStuff.UnsafeVolume.class, vol.getClass());
    }
}