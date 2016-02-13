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
    
	@Test public void testUnsafeVolume_GetLong() {
		Random random = new Random();
		Volume volume = Volume.UNSAFE_VOL_FACTORY.makeVolume(null, false);
		volume.ensureAvailable(20);
		for (long valueToPut = 0; valueToPut < Long.MAX_VALUE
				&& valueToPut >= 0; valueToPut = random.nextInt(2) + valueToPut * 2) {
			volume.putLong(10, valueToPut);
			long returnedValue = volume.getLong(10);
			assertEquals("value read from the UnsafeVolume is not equal to the value that was put", valueToPut, returnedValue);
			volume.putLong(10, -valueToPut);
			returnedValue = volume.getLong(10);
			assertEquals("value read from the UnsafeVolume is not equal to the value that was put", -valueToPut, returnedValue);
		}
	}
	
	@Test public void testUnsafeVolume_GetInt() {
		Random random = new Random();
		Volume volume = Volume.UNSAFE_VOL_FACTORY.makeVolume(null, false);
		volume.ensureAvailable(20);
		for (int intToPut = 0; intToPut < Integer.MAX_VALUE
				&& intToPut >= 0; intToPut = random.nextInt(2) + intToPut * 2) {
			volume.putInt(10, intToPut);
			int returnedValue = volume.getInt(10);
			assertEquals("int read from the UnsafeVolume is not equal to the int that was put", intToPut,
					returnedValue);
			volume.putInt(10, -intToPut);
			returnedValue = volume.getInt(10);
			assertEquals("int read from the UnsafeVolume is not equal to the int that was put", -intToPut,
					returnedValue);
		}
	}
	
	@Test
	public void testUnsafeVolume_GetByte() {
		Volume volume = Volume.UNSAFE_VOL_FACTORY.makeVolume(null, false);
		volume.ensureAvailable(20);
		for (byte byteToPut = 0; byteToPut < Byte.MAX_VALUE; byteToPut++) {
			volume.putByte(10, byteToPut);
			int returnedValue = volume.getByte(10);
			assertEquals("byte read from the UnsafeVolume is not equal to the byte that was put", byteToPut,
					returnedValue);
			volume.putByte(10, (byte) -byteToPut);
			returnedValue = volume.getByte(10);
			assertEquals("byte read from the UnsafeVolume is not equal to the byte that was put", -byteToPut,
					returnedValue);
		}
	}
	
}