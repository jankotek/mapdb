package org.mapdb;


import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.mapdb.StoreDirect.*;

public class StoreDirectTest <E extends StoreDirect> extends EngineTest<E>{

    @Override boolean canRollback(){return false;}

    Volume.Factory fac = Volume.fileFactory(false,0,Utils.tempDbFile(), 0L);

    static final long IO_RECID = StoreDirect.IO_FREE_RECID+32;

    @Override protected E openEngine() {
        return (E) new StoreDirect(fac);
    }

    int countIndexRecords(){
        int ret = 0;
        for(int pos = StoreDirect.IO_USER_START; pos<e.indexSize; pos+=8){
            long val = e.index.getLong(pos);
            if(val!=0 && val != StoreDirect.MASK_ARCHIVE) ret++;
        }
        return ret;
    }

    List<Long> getLongStack(long ioRecid){

        ArrayList<Long> ret =new ArrayList<Long>();

        long pagePhysid = e.index.getLong(ioRecid) & StoreDirect.MASK_OFFSET;
        long pageOffset = e.index.getLong(ioRecid) >>>48;


        while(pagePhysid!=0){

            while(pageOffset>=8){
                //System.out.println(pagePhysid + " - "+pageOffset);
                final Long l = e.phys.getSixLong(pagePhysid + pageOffset);
                pageOffset-=6;
                ret.add(l);
            }
            //System.out.println(ret);
            //read location of previous page
            pagePhysid = e.phys.getLong(pagePhysid) & StoreDirect.MASK_OFFSET;
            pageOffset = (e.phys.getLong(pagePhysid) >>>48) - 6;
        }

        return ret;
    }


    @Test
    public void phys_append_alloc(){
        e.structuralLock.lock();
        long[] ret = e.physAllocate(100,true);
        long expected = 100L<<48 | 16L;
        assertArrayEquals(new long[]{expected}, ret);
    }

    @Test
    public void phys_append_alloc_link2(){
        e.structuralLock.lock();
        long[] ret = e.physAllocate(100 + MAX_REC_SIZE,true);
        long exp1 = MASK_LINKED |((long)MAX_REC_SIZE)<<48 | 16L;
        long exp2 = 108L<<48 | (16L+MAX_REC_SIZE+1);
        assertArrayEquals(new long[]{exp1, exp2}, ret);
    }

    @Test
    public void phys_append_alloc_link3(){
        e.structuralLock.lock();
        long[] ret = e.physAllocate(100 + MAX_REC_SIZE*2,true);
        long exp1 = MASK_LINKED | ((long)MAX_REC_SIZE)<<48 | 16L;
        long exp2 = MASK_LINKED | ((long)MAX_REC_SIZE)<<48 | (16L+MAX_REC_SIZE+1);
        long exp3 = ((long)116)<<48 | (16L+MAX_REC_SIZE*2+2);

        assertArrayEquals(new long[]{exp1, exp2, exp3}, ret);
    }

    @Test public void second_rec_pos_round_to_16(){
        e.structuralLock.lock();
        long[] ret= e.physAllocate(1,true);
        assertArrayEquals(new long[]{1L<<48|16L},ret);
        ret= e.physAllocate(1,true);
        assertArrayEquals(new long[]{1L<<48|32L},ret);

    }


    @Test public void test_index_record_delete(){
        long recid = e.put(1000L, Serializer.LONG_SERIALIZER);
        e.commit();
        assertEquals(1, countIndexRecords());
        e.delete(recid,Serializer.LONG_SERIALIZER);
        e.commit();
        assertEquals(0, countIndexRecords());
        e.structuralLock.lock();
        assertEquals(recid*8 + StoreDirect.IO_USER_START, e.freeIoRecidTake(true));
    }

    @Test public void test_size2IoList(){
        long old= StoreDirect.IO_FREE_RECID;
        for(int size=1;size<= StoreDirect.MAX_REC_SIZE;size++){

            long ioListRecid = size2ListIoRecid(size);
            assertTrue(ioListRecid> StoreDirect.IO_FREE_RECID);
            assertTrue(ioListRecid< StoreDirect.IO_USER_START);

            assertEquals(ioListRecid,old+(size%16==1?8:0));

            old=ioListRecid;
        }
    }



    @Test public void test_index_record_delete_and_reusef(){
        long recid = e.put(1000L, Serializer.LONG_SERIALIZER);
        e.commit();
        assertEquals(1, countIndexRecords());
        assertEquals(e.LAST_RESERVED_RECID+1, recid);
        e.delete(recid,Serializer.LONG_SERIALIZER);
        e.commit();
        assertEquals(0, countIndexRecords());
        long recid2 = e.put(1000L, Serializer.LONG_SERIALIZER);
        e.commit();
        //test that previously deleted index slot was reused
        assertEquals(recid, recid2);
        assertEquals(1, countIndexRecords());
        assertNotEquals(0,e.index.getLong(recid*8+ StoreDirect.IO_USER_START));
    }


    @Test public void test_index_record_delete_and_reuse_large(){
        final long MAX = 10;

        List<Long> recids= new ArrayList<Long>();
        for(int i = 0;i<MAX;i++){
            recids.add(e.put(0L, Serializer.LONG_SERIALIZER));
        }

        for(long recid:recids){
            e.delete(recid,Serializer.LONG_SERIALIZER);
        }

        //now allocate again second recid list
        List<Long> recids2= new ArrayList<Long>();
        for(int i = 0;i<MAX;i++){
            recids2.add(e.put(0L, Serializer.LONG_SERIALIZER));
        }

        //second list should be reverse of first, as Linked Offset List is LIFO
        Collections.reverse(recids);
        assertEquals(recids, recids2);
    }



    @Test public void test_phys_record_reused(){
        final long recid = e.put(1L, Serializer.LONG_SERIALIZER);
        assertEquals((Long)1L, e.get(recid, Serializer.LONG_SERIALIZER));
        final long physRecid = e.index.getLong(recid*8+ StoreDirect.IO_USER_START);
        e.delete(recid, Serializer.LONG_SERIALIZER);
        final long recid2 = e.put(1L, Serializer.LONG_SERIALIZER);
        assertEquals((Long)1L, e.get(recid2, Serializer.LONG_SERIALIZER));

        assertEquals(recid, recid2);
        assertEquals(physRecid, e.index.getLong(recid*8+ StoreDirect.IO_USER_START));

    }



    @Test public void test_index_stores_record_size() throws IOException {
        final long recid = e.put(1, Serializer.INTEGER_SERIALIZER);
        e.commit();
        assertEquals(4, e.index.getUnsignedShort(recid * 8+ StoreDirect.IO_USER_START));
        assertEquals(Integer.valueOf(1), e.get(recid, Serializer.INTEGER_SERIALIZER));

        e.update(recid, 1L, Serializer.LONG_SERIALIZER);
        e.commit();
        assertEquals(8, e.index.getUnsignedShort(recid * 8+ StoreDirect.IO_USER_START));
        assertEquals(Long.valueOf(1), e.get(recid, Serializer.LONG_SERIALIZER));

    }

    @Test public void test_long_stack_puts_record_offset_into_index() throws IOException {
        e.structuralLock.lock();
        e.longStackPut(IO_RECID, 1);
        e.commit();
        assertEquals(8,
                e.index.getLong(IO_RECID)>>>48);

    }

    @Test public void test_long_stack_put_take() throws IOException {
        e.structuralLock.lock();

        final long max = 150;
        for(long i=1;i<max;i++){
            e.longStackPut(IO_RECID, i);
        }

        for(long i = max-1;i>0;i--){
            assertEquals(i, e.longStackTake(IO_RECID));
        }

        assertEquals(0, getLongStack(IO_RECID).size());

    }

    @Test public void test_long_stack_put_take_simple() throws IOException {
        e.structuralLock.lock();
        e.longStackPut(IO_RECID, 111);
        assertEquals(111L, e.longStackTake(IO_RECID));
    }


    @Test public void test_basic_long_stack() throws IOException {
        //dirty hack to make sure we have lock
        e.structuralLock.lock();
        final long max = 150;
        ArrayList<Long> list = new ArrayList<Long>();
        for(long i=1;i<max;i++){
            e.longStackPut(IO_RECID, i);
            list.add(i);
        }

        Collections.reverse(list);
        e.commit();

        assertEquals(list, getLongStack(IO_RECID));

        for(long i =max-1;i>=1;i--){
            assertEquals(i, e.longStackTake(IO_RECID));
        }
    }

    @Test public void test_large_long_stack() throws IOException {
        //dirty hack to make sure we have lock
        e.structuralLock.lock();
        final long max = 15000;
        ArrayList<Long> list = new ArrayList<Long>();
        for(long i=1;i<max;i++){
            e.longStackPut(IO_RECID, i);
            list.add(i);
        }

        Collections.reverse(list);
        e.commit();

        assertEquals(list, getLongStack(IO_RECID));

        for(long i =max-1;i>=1;i--){
            assertEquals(i, e.longStackTake(IO_RECID));
        }
    }

    @Test public void test_basic_long_stack_no_commit() throws IOException {
        //dirty hack to make sure we have lock
        e.structuralLock.lock();
        final long max = 150;
        for(long i=1;i<max;i++){
            e.longStackPut(IO_RECID, i);
        }

        for(long i =max-1;i>=1;i--){
            assertEquals(i, e.longStackTake(IO_RECID));
        }
    }

    @Test public void test_large_long_stack_no_commit() throws IOException {
        //dirty hack to make sure we have lock
        e.structuralLock.lock();
        final long max = 15000;
        for(long i=1;i<max;i++){
            e.longStackPut(IO_RECID, i);
        }


        for(long i =max-1;i>=1;i--){
            assertEquals(i, e.longStackTake(IO_RECID));
        }
    }



    @Test public void long_stack_page_created_after_put() throws IOException {
        e.structuralLock.lock();
        e.longStackPut(IO_RECID, 111);
        e.commit();
        long pageId = e.index.getLong(IO_RECID);
        assertEquals(8, pageId>>>48);
        pageId = pageId & StoreDirect.MASK_OFFSET;
        assertEquals(16L, pageId);
        assertEquals(LONG_STACK_PREF_SIZE, e.phys.getLong(pageId)>>>48);
        assertEquals(0, e.phys.getLong(pageId)& StoreDirect.MASK_OFFSET);
        assertEquals(111, e.phys.getSixLong(pageId + 8));
    }

    @Test public void long_stack_put_five() throws IOException {
        e.structuralLock.lock();
        e.longStackPut(IO_RECID, 111);
        e.longStackPut(IO_RECID, 112);
        e.longStackPut(IO_RECID, 113);
        e.longStackPut(IO_RECID, 114);
        e.longStackPut(IO_RECID, 115);

        e.commit();
        long pageId = e.index.getLong(IO_RECID);
        assertEquals(8+6*4, pageId>>>48);
        pageId = pageId & StoreDirect.MASK_OFFSET;
        assertEquals(16L, pageId);
        assertEquals(LONG_STACK_PREF_SIZE, e.phys.getLong(pageId)>>>48);
        assertEquals(0, e.phys.getLong(pageId)&MASK_OFFSET);
        assertEquals(111, e.phys.getSixLong(pageId + 8));
        assertEquals(112, e.phys.getSixLong(pageId + 14));
        assertEquals(113, e.phys.getSixLong(pageId + 20));
        assertEquals(114, e.phys.getSixLong(pageId + 26));
        assertEquals(115, e.phys.getSixLong(pageId + 32));
    }

    @Test public void long_stack_page_deleted_after_take() throws IOException {
        e.structuralLock.lock();
        e.longStackPut(IO_RECID, 111);
        e.commit();
        assertEquals(111L, e.longStackTake(IO_RECID));
        e.commit();
        assertEquals(0L, e.index.getLong(IO_RECID));
    }

    @Test public void long_stack_page_overflow() throws IOException {
        e.structuralLock.lock();
        //fill page until near overflow
        for(int i=0;i< StoreDirect.LONG_STACK_PREF_COUNT;i++){
            e.longStackPut(IO_RECID, 1000L+i);
        }
        e.commit();

        //check content
        long pageId = e.index.getLong(IO_RECID);
        assertEquals(StoreDirect.LONG_STACK_PREF_SIZE-6, pageId>>>48);
        pageId = pageId & StoreDirect.MASK_OFFSET;
        assertEquals(16L, pageId);
        assertEquals(StoreDirect.LONG_STACK_PREF_SIZE, e.phys.getLong(pageId)>>>48);
        for(int i=0;i< StoreDirect.LONG_STACK_PREF_COUNT;i++){
            assertEquals(1000L+i, e.phys.getSixLong(pageId + 8 + i * 6));
        }

        //add one more item, this will trigger page overflow
        e.longStackPut(IO_RECID, 11L);
        e.commit();
        //check page overflowed
        pageId = e.index.getLong(IO_RECID);
        assertEquals(8, pageId>>>48);
        pageId = pageId & StoreDirect.MASK_OFFSET;
        assertEquals(16L+ StoreDirect.LONG_STACK_PREF_SIZE, pageId);
        assertEquals(LONG_STACK_PREF_SIZE, e.phys.getLong(pageId)>>>48);
        assertEquals(16L, e.phys.getLong(pageId)& StoreDirect.MASK_OFFSET);
        assertEquals(11L, e.phys.getSixLong(pageId + 8));
    }


    @Test public void test_constants(){
        assertTrue(StoreDirect.LONG_STACK_PREF_SIZE%16==0);

    }


    @Test public void delete_files_after_close(){
        File f = Utils.tempDbFile();
        File phys = new File(f.getPath()+StoreDirect.DATA_FILE_EXT);

        DB db = DBMaker.newFileDB(f).writeAheadLogDisable().asyncWriteDisable().deleteFilesAfterClose().make();

        db.getHashMap("test").put("aa","bb");
        db.commit();
        assertTrue(f.exists());
        assertTrue(phys.exists());
        db.close();
        assertFalse(f.exists());
        assertFalse(phys.exists());
    }

    @Test public void freeSpaceWorks(){
        long oldFree = e.getFreeSize();
        long recid = e.put(new byte[10000],Serializer.BYTE_ARRAY_SERIALIZER);
        e.commit();
        assertEquals(oldFree, e.getFreeSize());
        e.delete(recid,Serializer.BYTE_ARRAY_SERIALIZER);
        assertEquals(oldFree+10000,e.getFreeSize());
        e.commit();
        assertEquals(oldFree+10000,e.getFreeSize());

    }

}
