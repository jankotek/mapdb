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

    final long masterLinkOffset = StoreDirect2.O_STACK_FREE_RECID;
    final long M = StoreDirect2.HEADER_SIZE;

    @Test public void longStack_put_take_single(){
        StoreDirect2 s = new StoreDirect2(null);
        s.init();
        s.structuralLock.lock();

        // longStack page is at offset M+100 with size 200
        s.vol.ensureAvailable(M *2);
        s.vol.putLong(M +100, parity3Set(200L<<48));

        long stackTail = 8;

        long val = 10000;
        long newTail = s.longStackPutIntoPage(val, M +100,stackTail);
        assertEquals(stackTail + packLongSize(val << 1), newTail);

        //verify no other data were modified
        TT.assertZeroes(s.vol, StoreDirect2.HEADER_SIZE, M +100);
        TT.assertZeroes(s.vol, M +100+newTail, s.vol.length());
        assertEquals(s.vol.getLong(M +100), parity3Set(200L << 48));
        assertEquals(parity1Set(val << 1), s.vol.getPackedLongReverse(M +108)&PACK_LONG_RESULT_MASK);

        //perform take and verify it
        assertEquals((8L<<48)+val, s.longStackTakeFromPage(M+100,newTail));
        TT.assertZeroes(s.vol, StoreDirect2.HEADER_SIZE, M +100);
        TT.assertZeroes(s.vol, M +108, s.vol.length());
        assertEquals(s.vol.getLong(M +100), parity3Set(200L<< 48));
    }

    @Test public void long_stack_page_fill(){
        Random r = new Random();

        StoreDirect2 s = new StoreDirect2(null);s.init();
        s.init();
        s.structuralLock.lock();

        // longStack page is at offset 100 with size 200
        s.vol.ensureAvailable(M *2);
        s.vol.putLong(M +100, parity3Set(200L<<48));
        List<Long> vals = new ArrayList<Long>();

        long bytesWritten = 8;
        long tail = 8;
        while(true){
            long val = r.nextLong()&0xFFFFFFFFFFFFL; //6 bytes;
            bytesWritten+=packLongSize(val << 1);
            long newTail = s.longStackPutIntoPage(val, M +100,tail);
            if(newTail==-1) {
                assertTrue(bytesWritten>200);
                break;
            }
            tail = newTail;
            assertEquals(tail, bytesWritten);
            vals.add(val);
        }
        assertEquals(s.vol.getLong(M + 100), parity3Set(200L << 48));
        TT.assertZeroes(s.vol, StoreDirect2.HEADER_SIZE, M +100);
        TT.assertZeroes(s.vol, M + 100 + tail, s.vol.length());

        //now take values
        Collections.reverse(vals);
        for(long val:vals){
            long val2 = s.longStackTakeFromPage(M +100, tail);
            long newTail = val2>>>48;
            val2 &= 0xFFFFFFFFFFFFL; //6 bytes
            assertEquals(val, val2);
            assertEquals(tail- packLongSize(val2<<1), newTail);
            tail = (int) newTail;
        }
        assertEquals(8, tail);
        assertEquals(s.vol.getLong(M + 100), parity3Set(200L << 48));
        TT.assertZeroes(s.vol, StoreDirect2.HEADER_SIZE, M +100);
        TT.assertZeroes(s.vol, M + 108, s.vol.length());
    }

    @Test public void long_stack_take(){
        StoreDirect2 s = new StoreDirect2(null);
        s.init();
        s.structuralLock.lock();
        s.vol.ensureAvailable(M * 2);
        // longStack page is at offset 160 with size 200
        s.vol.putLong(M +160, parity4Set(200L << 48));
        // master link offset is at 16
        long packedSize = packLongSize(10000L<<1);
        long tail = 8;
        s.vol.putPackedLongReverse(M + 160 + tail, parity1Set(10000L << 1));
        s.vol.putPackedLongReverse(M + 160 + tail + packedSize, parity1Set(10000L << 1));
        tail = tail+packedSize*2;

        s.headVol.putLong(masterLinkOffset, parity4Set((tail << 48) + M + 160));

        long ret = s.longStackTake(masterLinkOffset);
        assertEquals(10000L, ret);
        assertEquals(parity4Set(((8 + packedSize) << 48) + M + 160), s.headVol.getLong(masterLinkOffset));
    }

    @Test public void long_stack_put(){
        StoreDirect2 s = new StoreDirect2(null);
        s.init();
        s.structuralLock.lock();
        s.vol.ensureAvailable(M * 2);

        // longStack page is at offset 160 with size 200
        s.vol.putLong(M +160, parity4Set(200L << 48));

        long tail = 8;
        s.headVol.putLong(masterLinkOffset, parity4Set((tail << 48) + M +160));

        s.longStackPut(masterLinkOffset, 10000L);
        long packedSize = packLongSize(10000L<<1);
        assertEquals(parity4Set(((8 + packedSize) << 48) + M +160), s.headVol.getLong(masterLinkOffset));
    }

    @Test public void long_stack_page_create(){
        StoreDirect2 s = new StoreDirect2(null);
        s.init();
        s.structuralLock.lock();
        s.vol.ensureAvailable(M * 2);
        s.storeSizeSet(M +176); //page should be created at end of file, by expanding file

        long retVal = s.longStackPageCreate(10000L, masterLinkOffset, M +320);

        assertEquals((160L << 48) + M +176, retVal);

        long packedSize = packLongSize(10000L<<1);

        //store size
        assertEquals(s.storeSizeGet(), M + 176 + 160);
        //verify Master Link Value
        assertEquals(parity4Set(((8 + packedSize) << 48) + M + 176), s.headVol.getLong(masterLinkOffset));
        //verify Page Header
        assertEquals(parity4Set((160L << 48) + M + 320L), s.vol.getLong(M + 176));
        //get value in long stack
        assertEquals(parity1Set(10000L << 1), s.vol.getPackedLongReverse(M +176+8)&PACK_LONG_RESULT_MASK);
    }

    @Test public void long_stack_put_creates_new_page(){
        StoreDirect2 s = new StoreDirect2(null);
        s.init();
        s.structuralLock.lock();
        s.vol.ensureAvailable(M *2);
        s.storeSizeSet(M +176); //page should be created at end of file, by expanding file


        long packedSize = packLongSize(10000L<<1);

        s.longStackPut(masterLinkOffset,10000L);
        //store size
        assertEquals(s.storeSizeGet(), M + 176 + 160);
        //verify Master Link Value
        assertEquals(parity4Set(((8 + packedSize) << 48) + M + 176), s.headVol.getLong(masterLinkOffset));
        //verify Page Header
        assertEquals(parity4Set(160L << 48), s.vol.getLong(M +176));
        //get value in long stack
        assertEquals(parity1Set(10000L<<1), s.vol.getPackedLongReverse(M +176+8)&PACK_LONG_RESULT_MASK);
    }

    @Test public void long_stack_put_creates_new_page_after_page_full(){
        StoreDirect2 s = new StoreDirect2(null);
        s.init();
        s.structuralLock.lock();
        s.vol.ensureAvailable(M*2);

        //create fake page with size 9 at offset M+32
        s.vol.putLong(M + 32, parity4Set(9L << 48));

        //set Master Pointer to point at this fake page
        s.headVol = new Volume.SingleByteArrayVol(1000);
        s.headVol.putLong(masterLinkOffset, parity4Set((8L << 48) + M + 32));
        s.storeSizeSet( M+176);

        long packedSize = packLongSize(10000L<<1);

        s.longStackPut(masterLinkOffset,10000L);
        //store size
        assertEquals(s.storeSizeGet(), M+176 + 160);
        //verify Master Link Value
        assertEquals(parity4Set(((8 + packedSize) << 48) + M+176), s.headVol.getLong(masterLinkOffset));
        //verify Page Header
        assertEquals(parity4Set((160L << 48)+M+32), s.vol.getLong(M+176));
        //get value in long stack
        assertEquals(parity1Set(10000L<<1), s.vol.getPackedLongReverse(M+176+8)&PACK_LONG_RESULT_MASK);
    }

    @Test public void long_stack_page_removed_single(){
        StoreDirect2 s = new StoreDirect2(null);
        s.init();
        s.structuralLock.lock();
        s.vol.ensureAvailable(M * 2);


        //create fake page with size 160 at offset M+32
        s.vol.putLong(M + 32, parity4Set(160L << 48));
        //put value 2 into this page
        s.vol.putPackedLongReverse(M + 32 + 8, parity1Set(2 << 1));

        //set Master Pointer to point to this fake page
        s.headVol.putLong(masterLinkOffset, parity4Set((9L << 48) + M+32));
        s.storeSizeSet(M+32+160);

        //take value from this page, it should be deleted and zeroed out
        assertEquals(2L, s.longStackTake(masterLinkOffset));

        //Master Link Value is empty
        assertEquals(parity4Set(0L), s.headVol.getLong(masterLinkOffset));

        //and all zero
        TT.assertZeroes(s.vol, StoreDirect2.HEADER_SIZE, s.vol.length());
    }

    @Test public void long_stack_page_removed_prev_link(){
        StoreDirect2 s = new StoreDirect2(null);
        s.init();
        s.structuralLock.lock();
        s.vol.ensureAvailable(M*2);


        //create fake page with size 16 at offset 800
        s.vol.putLong(M + 800, parity4Set(16L << 48));
        //put value 2 into this page
        s.vol.putPackedLongReverse(M + 800 + 8, parity1Set(2 << 1));
        s.storeSizeSet(M+800+16);

        //create fake page with size 160 at offset 32,
        //set its previous link to page 800
        s.vol.putLong(M+32, parity4Set((160L<<48)+M+800));
        //put value 2 into this page
        s.vol.putPackedLongReverse(M+32+8, parity1Set(2<<1));

        //set Master Pointer to point to this fake page
        s.headVol.putLong(masterLinkOffset, parity4Set((9L<<48)+M+32));

        //take value from this page, it should be deleted and zeroed out
        assertEquals(2L, s.longStackTake(masterLinkOffset));

        //Master Link Value points to previous page with unknown tail
        assertEquals(parity4Set((0L << 48) + M+800L), s.headVol.getLong(masterLinkOffset));

        //and all zero
        TT.assertZeroes(s.vol, M, M + 800);

        //and previous page was not modified
        assertEquals(parity4Set(16L<<48), s.vol.getLong(M+800));
        assertEquals(parity1Set(2 << 1), s.vol.getPackedLongReverse(M + 800 + 8) & PACK_LONG_RESULT_MASK);
    }

    @Test public void large_set(){
        int cycles = 2 + TT.scale()*1000;
        StoreDirect2 s = new StoreDirect2(null);
        s.init();
        s.structuralLock.lock();

        int max =  1000000;
        Random r = new Random(1);

        for(int i=0;i<cycles;i++){
            long[] vals = new long[max];
            for(int j=0;j<max;j++){
                long val = r.nextLong()&StoreDirect.MOFFSET;
                vals[j] = val;
                s.longStackPut(masterLinkOffset,val);
            }
            //take from long stack in reverse order
            for(int j=max-1; j>=0; j--){
                assertEquals(vals[j], s.longStackTake(masterLinkOffset));
            }
            assertEquals(0L, s.longStackTake(masterLinkOffset));
            TT.assertZeroes(s.vol, StoreDirect2.HEADER_SIZE, s.vol.length());

            assertEquals(StoreDirect2.HEADER_SIZE,s.storeSizeGet());
        }
    }

    @Test public void find_tail(){
        StoreDirect2 s = new StoreDirect2(null);
        s.structuralLock.lock();
        s.vol = s.headVol = new Volume.ByteArrayVol();
        s.headVol.ensureAvailable(32+160);
        //create page and fill it with values
        s.headVol.putLong(16, parity4Set((0L << 48) | 32)); //zero tail indicates that tail is not known

        //page size is 160, next offset is 0
        s.vol.putLong(32, parity4Set((160L << 48) | 0));
        //put 100 single byte packed longs
        long pos = 32+8;
        for(int i=0;i<100;i++){
            pos+=s.vol.putPackedLongReverse(pos, parity1Set(10L<<1));
        }

        assertEquals(pos-32, s.longStackPageFindTail(32));
    }

    @Test public void reverse_flag_packed_long(){
        StoreDirect2 s = new StoreDirect2(null);
        s.init();
        s.structuralLock.lock();
        s.vol.putLong(1024, parity4Set(0));
        s.longStackPut(1024, 1000);
        s.longStackPut(1024, 1000);
        s.longStackPut(1024, 1000);

        long masterLinkVal = parity4Get(s.vol.getLong(1024));
        long pageOffset = masterLinkVal&StoreDirect.MOFFSET;
        long tail = masterLinkVal >>> 48;

        assertTrue((s.vol.getUnsignedByte(pageOffset + tail - 1) & 0x7F) != 0);
    }


}