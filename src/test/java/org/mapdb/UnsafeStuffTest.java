package org.mapdb;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;


/** delete this class if it fails to compile due to missign 'sun.misc.Unsafe' */
public class UnsafeStuffTest {

    sun.misc.Unsafe unsafe = null; //just add compilation time dependency

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


    @Test public void byteArrayHashMatches(){
        Random r = new Random();

        for(int i=0;i<1000;i++){
            int len = r.nextInt(10000);
            byte[] b = new byte[len];
            r.nextBytes(b);
            assertEquals(
                    DataIO.hash(b, 0, len, len),
                    UnsafeStuff.hash(b, 0, len, len)
            );
        }
    }

    @Test public void charArrayHashMatches(){
        Random r = new Random();

        for(int i=0;i<1000;i++){
            int len = r.nextInt(10000);
            char[] b = new char[len];
            for(int j=0;j<len;j++){
                b[j] = (char) r.nextInt(Character.MAX_VALUE);
            }
            assertEquals(
                    DataIO.hash(b, 0, len, len),
                    UnsafeStuff.hash(b, 0, len, len)
            );
        }
    }

}