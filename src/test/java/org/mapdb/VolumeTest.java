package org.mapdb;


import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class VolumeTest {


    final int beyondInc = (int) (1e7);

    Volume b = new Volume.MemoryVolume(false);
    {
        b.ensureAvailable(Volume.INITIAL_SIZE);
    }


    @Test
    public void testEnsureAvailable() throws Exception {
        try{
            b.putLong(beyondInc,111L);
            assertTrue(false);
        }catch(Exception e){
            //ignore
        }
        b.ensureAvailable(beyondInc+8);
        b.putLong(beyondInc,111L);
        assertEquals(111L, b.getLong(beyondInc));

    }

    @Test
    public void testPutLong() throws Exception {
        b.putLong(1000, 111L);
        assertEquals(111L, b.getLong(1000));
    }

    @Test
    public void testPutUnsignedByte() throws Exception {
        b.putUnsignedByte(1000, (byte) 11);
        assertEquals(11, b.getUnsignedByte(1000));
        b.putUnsignedByte(1000, (byte) 126);
        assertEquals(126, b.getUnsignedByte(1000));
        b.putUnsignedByte(1000, (byte) 130);
        assertEquals(130, b.getUnsignedByte(1000));
        b.putUnsignedByte(1000, (byte) 255);
        assertEquals(255, b.getUnsignedByte(1000));
        b.putUnsignedByte(1000, (byte) 0);
        assertEquals(0, b.getUnsignedByte(1000));
    }

    @Test
    public void testPutData() throws Exception {
        DataOutput2 out = new DataOutput2();
        out.writeInt(11);
        out.writeLong(1111L);
        b.putData(111L, out.buf, out.pos);

        DataInput2 in = b.getDataInput(111L, out.pos);
        assertEquals(11, in.readInt());
        assertEquals(1111L, in.readLong());
    }

    @Test public void unsignedShort() throws IOException {
        b.putUnsignedShort(1000, 0);
        assertEquals(0, b.getUnsignedShort(1000));
        b.putUnsignedShort(1000, 100);
        assertEquals(100, b.getUnsignedShort(1000));
        b.putUnsignedShort(1000, 32000);
        assertEquals(32000, b.getUnsignedShort(1000));

        b.putUnsignedShort(1000, 35000);
        assertEquals(35000, b.getUnsignedShort(1000));

        b.putUnsignedShort(1000, 65000);
        assertEquals(65000, b.getUnsignedShort(1000));

    }


    @Test public void testConstants(){
        assertEquals(0, Volume.BUF_SIZE%Volume.MappedFileVolume.BUF_SIZE_INC);
        assertEquals(0, Volume.BUF_SIZE%8);
        assertEquals(0, Volume.MappedFileVolume.BUF_SIZE_INC%8);
        assertTrue(Storage.INDEX_OFFSET_START*8< Volume.INITIAL_SIZE);
        assertTrue(Volume.MappedFileVolume.BUF_SIZE_INC> StorageDirect.MAX_RECORD_SIZE);
    }


}
