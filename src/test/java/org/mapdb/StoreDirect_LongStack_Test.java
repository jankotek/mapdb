package org.mapdb;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.mapdb.DataIO.*;

public class StoreDirect_LongStack_Test {

    @Test public void longStack_put_take_single(){
        StoreDirect2 s = new StoreDirect2(null);
        s.structuralLock.lock();

        Volume vol = new Volume.SingleByteArrayVol(1000);
        s.vol = vol;

        vol.clear(0,1000);
        // longStack page is at offset 100 with size 200
        vol.putLong(100, parity3Set(200L<<48));

        long stackTail = 8;

        long val = 10000;
        long newTail = s.longStackPutIntoPage(val,100,stackTail);
        assertEquals(stackTail + packLongSize(val << 1), newTail);

        //verify no other data were modified
        TT.assertZeroes(vol, 0, 100);
        TT.assertZeroes(vol, 100+newTail, 1000);
        assertEquals(vol.getLong(100), parity3Set(200L << 48));
        assertEquals(parity1Set(val << 1), vol.getPackedLong(108)&PACK_LONG_RESULT_MASK);

        //perform take and verify it
        assertEquals((8L<<48)+val, s.longStackTakeFromPage(100,newTail));
        TT.assertZeroes(vol, 0, 100);
        TT.assertZeroes(vol, 108, 1000);
        assertEquals(vol.getLong(100), parity3Set(200L<< 48));
    }

    @Test public void long_stack_page_fill(){
        Random r = new Random();

        StoreDirect2 s = new StoreDirect2(null);
        s.structuralLock.lock();

        Volume vol = new Volume.SingleByteArrayVol(1000);
        s.vol = vol;
        // longStack page is at offset 100 with size 200
        vol.putLong(100, parity3Set(200L<<48));
        List<Long> vals = new ArrayList<Long>();

        long bytesWritten = 8;
        long tail = 8;
        while(true){
            long val = r.nextLong()&0xFFFFFFFFFFFFL; //6 bytes;
            bytesWritten+=packLongSize(val << 1);
            long newTail = s.longStackPutIntoPage(val, 100,tail);
            if(newTail==-1) {
                assertTrue(bytesWritten>200);
                break;
            }
            tail = newTail;
            assertEquals(tail, bytesWritten);
            vals.add(val);
        }
        assertEquals(vol.getLong(100), parity3Set(200L << 48));
        TT.assertZeroes(vol, 0, 100);
        TT.assertZeroes(vol, 100 + tail, 1000);

        //now take values
        Collections.reverse(vals);
        for(long val:vals){
            long val2 = s.longStackTakeFromPage(100, tail);
            long newTail = val2>>>48;
            val2 &= 0xFFFFFFFFFFFFL; //6 bytes
            assertEquals(val, val2);
            assertEquals(tail- packLongSize(val2<<1), newTail);
            tail = (int) newTail;
        }
        assertEquals(8, tail);
        assertEquals(vol.getLong(100), parity3Set(200L << 48));
        TT.assertZeroes(vol, 0, 100);
        TT.assertZeroes(vol, 108, 1000);
    }

    @Test public void long_stack_take(){
        StoreDirect2 s = new StoreDirect2(null);
        s.structuralLock.lock();
        s.vol = new Volume.SingleByteArrayVol(1000);
        s.headVol = new Volume.SingleByteArrayVol(1000);

        // longStack page is at offset 160 with size 200
        s.vol.putLong(160, parity4Set(200L << 48));
        // master link offset is at 16
        long packedSize = packLongSize(10000L<<1);
        long tail = 8;
        s.vol.putPackedLong(160 + tail, parity1Set(10000L << 1));
        s.vol.putPackedLong(160 + tail + packedSize, parity1Set(10000L << 1));
        tail = tail+packedSize*2;

        s.headVol.putLong(16, parity4Set((tail << 48) + 160));

        long ret = s.longStackTake(16);
        assertEquals(10000L, ret);
        assertEquals(parity4Set(((8 + packedSize) << 48) + 160), s.headVol.getLong(16));
    }

    @Test public void long_stack_put(){
        StoreDirect2 s = new StoreDirect2(null);
        s.structuralLock.lock();
        s.vol = new Volume.SingleByteArrayVol(1000);
        s.headVol = new Volume.SingleByteArrayVol(1000);

        // longStack page is at offset 160 with size 200
        s.vol.putLong(160, parity4Set(200L << 48));
        // master link offset is at 16
        long tail = 8;
        s.headVol.putLong(16, parity4Set((tail << 48) + 160));

        s.longStackPut(16L, 10000L);
        long packedSize = packLongSize(10000L<<1);
        assertEquals(parity4Set(((8 + packedSize) << 48) + 160), s.headVol.getLong(16));
    }

    @Test public void long_stack_page_create(){
        StoreDirect2 s = new StoreDirect2(null);
        s.structuralLock.lock();
        s.vol = new Volume.SingleByteArrayVol(1000);
        s.headVol = new Volume.SingleByteArrayVol(1000);
        s.storeSize = 176;

        long retVal = s.longStackPageCreate(10000L, 16, 320);

        assertEquals((160L << 48) + 176, retVal);

        long packedSize = packLongSize(10000L<<1);

        //store size
        assertEquals(s.storeSize, 176 + 160);
        //verify Master Link Value
        assertEquals(parity4Set(((8 + packedSize) << 48) + 176), s.headVol.getLong(16));
        //verify Page Header
        assertEquals(parity4Set((160L << 48) + 320L), s.vol.getLong(176));
        //get value in long stack
        assertEquals(parity1Set(10000L << 1), s.vol.getPackedLong(176+8)&PACK_LONG_RESULT_MASK);
    }

    @Test public void long_stack_put_creates_new_page(){
        StoreDirect2 s = new StoreDirect2(null);
        s.structuralLock.lock();
        s.vol = new Volume.SingleByteArrayVol(1000);
        s.headVol = new Volume.SingleByteArrayVol(1000);
        s.headVol.putLong(16, parity4Set(0));
        s.storeSize = 176;

        long packedSize = packLongSize(10000L<<1);

        s.longStackPut(16,10000L);
        //store size
        assertEquals(s.storeSize, 176 + 160);
        //verify Master Link Value
        assertEquals(parity4Set(((8 + packedSize) << 48) + 176), s.headVol.getLong(16));
        //verify Page Header
        assertEquals(parity4Set(160L << 48), s.vol.getLong(176));
        //get value in long stack
        assertEquals(parity1Set(10000L<<1), s.vol.getPackedLong(176+8)&PACK_LONG_RESULT_MASK);
    }

    @Test public void long_stack_put_creates_new_page_after_page_full(){
        StoreDirect2 s = new StoreDirect2(null);
        s.structuralLock.lock();
        s.vol = new Volume.SingleByteArrayVol(1000);

        //create fake page with size 9 at offset 32
        s.vol.putLong(32, parity4Set(9L<<48));

        //set Master Pointer to point at this fake page
        s.headVol = new Volume.SingleByteArrayVol(1000);
        s.headVol.putLong(16, parity4Set((8L<<48)+32));
        s.storeSize = 176;

        long packedSize = packLongSize(10000L<<1);

        s.longStackPut(16,10000L);
        //store size
        assertEquals(s.storeSize, 176 + 160);
        //verify Master Link Value
        assertEquals(parity4Set(((8 + packedSize) << 48) + 176), s.headVol.getLong(16));
        //verify Page Header
        assertEquals(parity4Set((160L << 48)+32), s.vol.getLong(176));
        //get value in long stack
        assertEquals(parity1Set(10000L<<1), s.vol.getPackedLong(176+8)&PACK_LONG_RESULT_MASK);
    }

    @Test public void long_stack_page_removed_single(){
        StoreDirect2 s = new StoreDirect2(null);
        s.structuralLock.lock();
        s.vol = new Volume.SingleByteArrayVol(1000);

        //create fake page with size 160 at offset 32
        s.vol.putLong(32, parity4Set(160L<<48));
        //put value 2 into this page
        s.vol.putPackedLong(32 + 8, parity1Set(2 << 1));

        //set Master Pointer to point to this fake page
        s.headVol = new Volume.SingleByteArrayVol(1000);
        s.headVol.putLong(16, parity4Set((9L << 48) + 32));

        //take value from this page, it should be deleted and zeroed out
        assertEquals(2L, s.longStackTake(16));

        //Master Link Value is empty
        assertEquals(parity4Set(0L), s.headVol.getLong(16));

        //and all zero
        TT.assertZeroes(s.vol, 24, 1000);
    }

    @Test public void long_stack_page_removed_prev_link(){
        StoreDirect2 s = new StoreDirect2(null);
        s.structuralLock.lock();
        s.vol = new Volume.SingleByteArrayVol(1000);

        //create fake page with size 16 at offset 800
        s.vol.putLong(800, parity4Set(16L << 48));
        //put value 2 into this page
        s.vol.putPackedLong(800 + 8, parity1Set(2 << 1));

        //create fake page with size 160 at offset 32,
        //set its previous link to page 800
        s.vol.putLong(32, parity4Set((160L<<48)+800));
        //put value 2 into this page
        s.vol.putPackedLong(32+8, parity1Set(2<<1));

        //set Master Pointer to point to this fake page
        s.headVol = new Volume.SingleByteArrayVol(1000);
        s.headVol.putLong(16, parity4Set((9L<<48)+32));

        //take value from this page, it should be deleted and zeroed out
        assertEquals(2L, s.longStackTake(16));

        //Master Link Value points to previous page with unknown tail
        assertEquals(parity4Set((0L << 48) + 800L), s.headVol.getLong(16));

        //and all zero
        TT.assertZeroes(s.vol, 24, 800);

        //and previous page was not modified
        assertEquals(parity4Set(16L<<48), s.vol.getLong(800));
        assertEquals(parity1Set(2 << 1), s.vol.getPackedLong(800 + 8) & PACK_LONG_RESULT_MASK);
    }

    @Test public void large_set(){
        int cycles = 2 + TT.scale()*1000;
        StoreDirect2 s = new StoreDirect2(null);
        s.structuralLock.lock();
        s.vol = s.headVol = new Volume.ByteArrayVol();
        s.headVol.ensureAvailable(32);
        s.headVol.putLong(16, parity4Set(0L));

        s.storeSize = 32;
        int max =  1000000;
        Random r = new Random(1);

        for(int i=0;i<cycles;i++){
            long[] vals = new long[max];
            for(int j=0;j<max;j++){
                long val = r.nextLong()&StoreDirect.MOFFSET;
                vals[j] = val;
                s.longStackPut(16,val);
            }
            //take from long stack in reverse order
            for(int j=max-1; j>=0; j--){
                assertEquals(vals[j], s.longStackTake(16));
            }
            assertEquals(0L, s.longStackTake(16));
            TT.assertZeroes(s.vol, 24, s.vol.length());

            assertEquals(32,s.storeSize);
        }
    }

    @Test public void find_tail(){
        StoreDirect2 s = new StoreDirect2(null);
        s.structuralLock.lock();
        s.vol = s.headVol = new Volume.ByteArrayVol();
        s.headVol.ensureAvailable(32+160);
        //create page and fill it with values
        s.headVol.putLong(16, parity4Set((0L<<48)|32)); //zero tail indicates that tail is not known

        //page size is 160, next offset is 0
        s.vol.putLong(32, parity4Set((160L<<48)|0));
        //put 100 single byte packed longs
        long pos = 32+8;
        for(int i=0;i<100;i++){
            pos+=s.vol.putPackedLong(pos, parity1Set(10L<<1));
        }

        assertEquals(pos-32, s.longStackPageFindTail(32));

    }

}