package org.mapdb;


import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.mapdb.StoreDirect.*;

@SuppressWarnings({"rawtypes","unchecked"})
public class StoreDirectTest <E extends StoreDirect> extends EngineTest<E>{

    @Override boolean canRollback(){return false;}

    File f = UtilsTest.tempDbFile();


//    static final long IO_RECID = StoreDirect.IO_FREE_RECID+32;

    @Override protected E openEngine() {
        return (E) new StoreDirect(f.getPath());
    }

//    int countIndexRecords(){
//        int ret = 0;
//        for(int pos = StoreDirect.IO_USER_START; pos<e.volSize; pos+=8){
//            long val = e.vol.getLong(pos);
//            if(val!=0 && val != StoreDirect.MASK_ARCHIVE
//                    && (val&StoreDirect.MUNUSED)==0) {
//                ret++; //TODO proper check for non zero offset and size
//            }
//        }
//        return ret;
//    }
//
//
//    int countIndexPrealloc(){
//        int ret = 0;
//        for(int pos = (int) (StoreDirect.IO_USER_START+Engine.RECID_FIRST*8); pos<e.physSize; pos+=8){
//            long val = e.vol.getLong(pos);
//            if((val&StoreDirect.MUNUSED)!=0){
//                ret++; //TODO check for zero offset and zero size
//            }
//        }
//        return ret;
//    }
//
//
//    List<Long> getLongStack(long ioRecid){
//
//        ArrayList<Long> ret =new ArrayList<Long>();
//
//        long pagePhysid = e.vol.getLong(ioRecid) & StoreDirect.MOFFSET;
//        long pageOffset = e.vol.getLong(ioRecid) >>>48;
//
//
//        while(pagePhysid!=0){
//
//            while(pageOffset>=8){
//                //System.out.println(pagePhysid + " - "+pageOffset);
//                final Long l = e.vol.getSixLong(pagePhysid + pageOffset);
//                pageOffset-=6;
//                ret.add(l);
//            }
//            //System.out.println(ret);
//            //read location of previous page
//            pagePhysid = e.vol.getLong(pagePhysid) & StoreDirect.MOFFSET;
//            pageOffset = (e.vol.getLong(pagePhysid) >>>48) - 6;
//        }
//
//        return ret;
//    }
//
//
//    @Test
//    public void phys_append_alloc(){
//        e.structuralLock.lock();
//        long[] ret = e.physAllocate(100,true,false);
//        long expected = 100L<<48 | 16L;
//        assertArrayEquals(new long[]{expected}, ret);
//    }
//
//    @Test
//    public void phys_append_alloc_link2(){
//        e.structuralLock.lock();
//        long[] ret = e.physAllocate(100 + MAX_REC_SIZE,true,false);
//        long exp1 = MLINKED |((long)MAX_REC_SIZE)<<48 | 16L;
//        long exp2 = 108L<<48 | (16L+MAX_REC_SIZE+1);
//        assertArrayEquals(new long[]{exp1, exp2}, ret);
//    }
//
//    @Test
//    public void phys_append_alloc_link3(){
//        e.structuralLock.lock();
//        long[] ret = e.physAllocate(100 + MAX_REC_SIZE*2,true,false);
//        long exp1 = MLINKED | ((long)MAX_REC_SIZE)<<48 | 16L;
//        long exp2 = MLINKED | ((long)MAX_REC_SIZE)<<48 | (16L+MAX_REC_SIZE+1);
//        long exp3 = ((long)116)<<48 | (16L+MAX_REC_SIZE*2+2);
//
//        assertArrayEquals(new long[]{exp1, exp2, exp3}, ret);
//    }
//
//    @Test public void second_rec_pos_round_to_16(){
//        e.structuralLock.lock();
//        long[] ret= e.physAllocate(1,true,false);
//        assertArrayEquals(new long[]{1L<<48|16L},ret);
//        ret= e.physAllocate(1,true,false);
//        assertArrayEquals(new long[]{1L<<48|32L},ret);
//
//    }
//
//
//    @Test public void test_index_record_delete(){
//        long recid = e.put(1000L, Serializer.LONG);
//        e.commit();
//        assertEquals(1, countIndexRecords());
//        assertEquals(0, countIndexPrealloc());
//        e.delete(recid, Serializer.LONG);
//        e.commit();
//        assertEquals(0, countIndexRecords());
//        assertEquals(1, countIndexPrealloc());
//        e.structuralLock.lock();
//        assertEquals(recid*8 + StoreDirect.IO_USER_START + 8, e.freeIoRecidTake(true));
//    }
//
//
//    @Test public void test_index_record_delete_COMPACT(){
//        long recid = e.put(1000L, Serializer.LONG);
//        e.commit();
//        assertEquals(1, countIndexRecords());
//        e.delete(recid, Serializer.ILLEGAL_ACCESS);
//        e.commit();
//        assertEquals(0, countIndexRecords());
//        assertEquals(1, countIndexPrealloc());
//        e.structuralLock.lock();
//        assertEquals(recid*8 +8+ StoreDirect.IO_USER_START, e.freeIoRecidTake(true));
//    }
//
//    @Test public void test_size2IoList(){
//        long old= StoreDirect.IO_FREE_RECID;
//        for(int size=1;size<= StoreDirect.MAX_REC_SIZE;size++){
//
//            long ioListRecid = size2ListIoRecid(size);
//            assertTrue(ioListRecid> StoreDirect.IO_FREE_RECID);
//            assertTrue(ioListRecid< StoreDirect.IO_USER_START);
//
//            assertEquals(ioListRecid,old+(size%16==1?8:0));
//
//            old=ioListRecid;
//        }
//    }
//
//
//
//    @Test public void test_index_record_delete_and_reusef(){
//        long recid = e.put(1000L, Serializer.LONG);
//        e.commit();
//        assertEquals(1, countIndexRecords());
//        assertEquals(0, countIndexPrealloc());
//        assertEquals(RECID_LAST_RESERVED +1, recid);
//        e.delete(recid,Serializer.LONG);
//        e.commit();
//        assertEquals(0, countIndexRecords());
//        assertEquals(1, countIndexPrealloc());
//        long recid2 = e.put(1000L, Serializer.LONG);
//        e.commit();
//        //test that previously deleted index slot was reused
//        assertEquals(recid+1, recid2);
//        assertEquals(1, countIndexRecords());
//        assertEquals(1, countIndexPrealloc());
//        assertTrue(0!=e.vol.getLong(recid*8+ StoreDirect.IO_USER_START));
//    }
//
//
//
//
//    @Test public void test_index_record_delete_and_reusef_COMPACT(){
//        long recid = e.put(1000L, Serializer.LONG);
//        e.commit();
//        assertEquals(1, countIndexRecords());
//        assertEquals(RECID_LAST_RESERVED +1, recid);
//        e.delete(recid, Serializer.LONG);
//        e.commit();
//        e.compact();
//        assertEquals(0, countIndexRecords());
//        long recid2 = e.put(1000L, Serializer.LONG);
//        e.commit();
//        //test that previously deleted index slot was reused
//        assertEquals(recid, recid2);
//        assertEquals(1, countIndexRecords());
//        assertTrue(0 != e.vol.getLong(recid * 8 + StoreDirect.IO_USER_START));
//    }
//
//
//    @Test public void test_index_record_delete_and_reuse_large(){
//        final long MAX = 10;
//
//        List<Long> recids= new ArrayList<Long>();
//        for(int i = 0;i<MAX;i++){
//            recids.add(e.put(0L, Serializer.LONG));
//        }
//
//        for(long recid:recids){
//            e.delete(recid,Serializer.LONG);
//        }
//
//        //now allocate again second recid list
//        List<Long> recids2= new ArrayList<Long>();
//        for(int i = 0;i<MAX;i++){
//            recids2.add(e.put(0L, Serializer.LONG));
//        }
//
//        for(Long recid: recids){
//            assertFalse(recids2.contains(recid));
//            assertTrue(recids2.contains(recid+MAX));
//        }
//    }
//
//    @Test public void test_index_record_delete_and_reuse_large_COMPACT(){
//        final long MAX = 10;
//
//        List<Long> recids= new ArrayList<Long>();
//        for(int i = 0;i<MAX;i++){
//            recids.add(e.put(0L, Serializer.LONG));
//        }
//
//        for(long recid:recids){
//            e.delete(recid,Serializer.LONG);
//        }
//
//        //compaction will reclai recid
//        e.commit();
//        e.compact();
//
//        //now allocate again second recid list
//        List<Long> recids2= new ArrayList<Long>();
//        for(int i = 0;i<MAX;i++){
//            recids2.add(e.put(0L, Serializer.LONG));
//        }
//
//        //second list should be reverse of first, as Linked Offset List is LIFO
//        Collections.reverse(recids);
//        assertEquals(recids, recids2);
//    }
//
//
//
//    @Test public void test_phys_record_reused(){
//        final long recid = e.put(1L, Serializer.LONG);
//        assertEquals((Long)1L, e.get(recid, Serializer.LONG));
//        final long physRecid = e.vol.getLong(recid*8+ StoreDirect.IO_USER_START);
//        e.delete(recid, Serializer.LONG);
//        final long recid2 = e.put(1L, Serializer.LONG);
//        assertEquals((Long)1L, e.get(recid2, Serializer.LONG));
//        assertNotEquals(recid, recid2);
//        assertEquals(physRecid, e.vol.getLong(recid2*8+ StoreDirect.IO_USER_START));
//    }
//
//    @Test public void test_phys_record_reused_COMPACT(){
//        final long recid = e.put(1L, Serializer.LONG);
//        assertEquals((Long)1L, e.get(recid, Serializer.LONG));
//        final long physRecid = e.vol.getLong(recid*8+ StoreDirect.IO_USER_START);
//        e.delete(recid, Serializer.LONG);
//        e.commit();
//        e.compact();
//        final long recid2 = e.put(1L, Serializer.LONG);
//        assertEquals((Long)1L, e.get(recid2, Serializer.LONG));
//        e.commit();
//        assertEquals((Long)1L, e.get(recid2, Serializer.LONG));
//        assertEquals(recid, recid2);
//
//        long indexVal = e.vol.getLong(recid*8+ StoreDirect.IO_USER_START);
//        assertEquals(8L, indexVal>>>48); // size
//        assertEquals((physRecid&MOFFSET)+StoreDirect.LONG_STACK_PREF_SIZE
//                + (e instanceof StoreWAL?16:0), //TODO investigate why space allocation in WAL works differently
//                indexVal&MOFFSET); //offset
//        assertEquals(0, indexVal & StoreDirect.MLINKED);
//        assertEquals(0, indexVal & StoreDirect.MUNUSED);
//        assertNotEquals(0, indexVal & StoreDirect.MARCHIVE);
//    }
//
//
//
//    @Test public void test_index_stores_record_size() throws IOException {
//        final long recid = e.put(1, Serializer.INTEGER);
//        e.commit();
//        assertEquals(4, e.vol.getUnsignedShort(recid * 8+ StoreDirect.IO_USER_START));
//        assertEquals(Integer.valueOf(1), e.get(recid, Serializer.INTEGER));
//
//        e.update(recid, 1L, Serializer.LONG);
//        e.commit();
//        assertEquals(8, e.vol.getUnsignedShort(recid * 8+ StoreDirect.IO_USER_START));
//        assertEquals(Long.valueOf(1), e.get(recid, Serializer.LONG));
//
//    }
//
//    @Test public void test_long_stack_puts_record_offset_into_index() throws IOException {
//        e.structuralLock.lock();
//        e.longStackPut(IO_RECID, 1,false);
//        e.commit();
//        assertEquals(8,
//                e.vol.getLong(IO_RECID)>>>48);
//
//    }
//
//    @Test public void test_long_stack_put_take() throws IOException {
//        e.structuralLock.lock();
//
//        final long max = 150;
//        for(long i=1;i<max;i++){
//            e.longStackPut(IO_RECID, i,false);
//        }
//
//        for(long i = max-1;i>0;i--){
//            assertEquals(i, e.longStackTake(IO_RECID,false));
//        }
//
//        assertEquals(0, getLongStack(IO_RECID).size());
//
//    }
//
//    @Test public void test_long_stack_put_take_simple() throws IOException {
//        e.structuralLock.lock();
//        e.longStackPut(IO_RECID, 111,false);
//        assertEquals(111L, e.longStackTake(IO_RECID,false));
//    }
//
//
//    @Test public void test_basic_long_stack() throws IOException {
//        //dirty hack to make sure we have lock
//        e.structuralLock.lock();
//        final long max = 150;
//        ArrayList<Long> list = new ArrayList<Long>();
//        for(long i=1;i<max;i++){
//            e.longStackPut(IO_RECID, i,false);
//            list.add(i);
//        }
//
//        Collections.reverse(list);
//        e.commit();
//
//        assertEquals(list, getLongStack(IO_RECID));
//
//        for(long i =max-1;i>=1;i--){
//            assertEquals(i, e.longStackTake(IO_RECID,false));
//        }
//    }
//
//    @Test public void test_large_long_stack() throws IOException {
//        //dirty hack to make sure we have lock
//        e.structuralLock.lock();
//        final long max = 15000;
//        ArrayList<Long> list = new ArrayList<Long>();
//        for(long i=1;i<max;i++){
//            e.longStackPut(IO_RECID, i,false);
//            list.add(i);
//        }
//
//        Collections.reverse(list);
//        e.commit();
//
//        assertEquals(list, getLongStack(IO_RECID));
//
//        for(long i =max-1;i>=1;i--){
//            assertEquals(i, e.longStackTake(IO_RECID,false));
//        }
//    }
//
//    @Test public void test_basic_long_stack_no_commit() throws IOException {
//        //dirty hack to make sure we have lock
//        e.structuralLock.lock();
//        final long max = 150;
//        for(long i=1;i<max;i++){
//            e.longStackPut(IO_RECID, i,false);
//        }
//
//        for(long i =max-1;i>=1;i--){
//            assertEquals(i, e.longStackTake(IO_RECID,false));
//        }
//    }
//
//    @Test public void test_large_long_stack_no_commit() throws IOException {
//        //dirty hack to make sure we have lock
//        e.structuralLock.lock();
//        final long max = 15000;
//        for(long i=1;i<max;i++){
//            e.longStackPut(IO_RECID, i,false);
//        }
//
//
//        for(long i =max-1;i>=1;i--){
//            assertEquals(i, e.longStackTake(IO_RECID,false));
//        }
//    }
//
//
//
//    @Test public void long_stack_page_created_after_put() throws IOException {
//        e.structuralLock.lock();
//        e.longStackPut(IO_RECID, 111,false);
//        e.commit();
//        long pageId = e.vol.getLong(IO_RECID);
//        assertEquals(8, pageId>>>48);
//        pageId = pageId & StoreDirect.MOFFSET;
//        assertEquals(16L, pageId);
//        assertEquals(LONG_STACK_PREF_SIZE, e.vol.getLong(pageId)>>>48);
//        assertEquals(0, e.vol.getLong(pageId)& StoreDirect.MOFFSET);
//        assertEquals(111, e.vol.getSixLong(pageId + 8));
//    }
//
//    @Test public void long_stack_put_five() throws IOException {
//        e.structuralLock.lock();
//        e.longStackPut(IO_RECID, 111,false);
//        e.longStackPut(IO_RECID, 112,false);
//        e.longStackPut(IO_RECID, 113,false);
//        e.longStackPut(IO_RECID, 114,false);
//        e.longStackPut(IO_RECID, 115,false);
//
//        e.commit();
//        long pageId = e.vol.getLong(IO_RECID);
//        assertEquals(8+6*4, pageId>>>48);
//        pageId = pageId & StoreDirect.MOFFSET;
//        assertEquals(16L, pageId);
//        assertEquals(LONG_STACK_PREF_SIZE, e.vol.getLong(pageId)>>>48);
//        assertEquals(0, e.vol.getLong(pageId)&MOFFSET);
//        assertEquals(111, e.vol.getSixLong(pageId + 8));
//        assertEquals(112, e.vol.getSixLong(pageId + 14));
//        assertEquals(113, e.vol.getSixLong(pageId + 20));
//        assertEquals(114, e.vol.getSixLong(pageId + 26));
//        assertEquals(115, e.vol.getSixLong(pageId + 32));
//    }
//
//    @Test public void long_stack_page_deleted_after_take() throws IOException {
//        e.structuralLock.lock();
//        e.longStackPut(IO_RECID, 111,false);
//        e.commit();
//        assertEquals(111L, e.longStackTake(IO_RECID,false));
//        e.commit();
//        assertEquals(0L, e.vol.getLong(IO_RECID));
//    }
//
//    @Test public void long_stack_page_overflow() throws IOException {
//        e.structuralLock.lock();
//        //fill page until near overflow
//        for(int i=0;i< StoreDirect.LONG_STACK_PREF_COUNT;i++){
//            e.longStackPut(IO_RECID, 1000L+i,false);
//        }
//        e.commit();
//
//        //check content
//        long pageId = e.vol.getLong(IO_RECID);
//        assertEquals(StoreDirect.LONG_STACK_PREF_SIZE-6, pageId>>>48);
//        pageId = pageId & StoreDirect.MOFFSET;
//        assertEquals(16L, pageId);
//        assertEquals(StoreDirect.LONG_STACK_PREF_SIZE, e.vol.getLong(pageId)>>>48);
//        for(int i=0;i< StoreDirect.LONG_STACK_PREF_COUNT;i++){
//            assertEquals(1000L+i, e.vol.getSixLong(pageId + 8 + i * 6));
//        }
//
//        //add one more item, this will trigger page overflow
//        e.longStackPut(IO_RECID, 11L,false);
//        e.commit();
//        //check page overflowed
//        pageId = e.vol.getLong(IO_RECID);
//        assertEquals(8, pageId>>>48);
//        pageId = pageId & StoreDirect.MOFFSET;
//        assertEquals(16L+ StoreDirect.LONG_STACK_PREF_SIZE, pageId);
//        assertEquals(LONG_STACK_PREF_SIZE, e.vol.getLong(pageId)>>>48);
//        assertEquals(16L, e.vol.getLong(pageId)& StoreDirect.MOFFSET);
//        assertEquals(11L, e.vol.getSixLong(pageId + 8));
//    }
//
//
//    @Test public void test_constants(){
//        assertTrue(StoreDirect.LONG_STACK_PREF_SIZE%16==0);
//
//    }


    @Test public void delete_files_after_close(){
        File f = UtilsTest.tempDbFile();
        File phys = new File(f.getPath());

        DB db = DBMaker.newFileDB(f).transactionDisable().deleteFilesAfterClose().make();

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
        long recid = e.put(new byte[10000],Serializer.BYTE_ARRAY_NOSIZE);
        e.commit();
        assertEquals(oldFree, e.getFreeSize());
        e.delete(recid,Serializer.BYTE_ARRAY_NOSIZE);
        assertEquals(oldFree+10000,e.getFreeSize());
        e.commit();
        assertEquals(oldFree+10000,e.getFreeSize());
    }


    @Test public void prealloc(){
        long recid = e.preallocate();
        assertNull(e.get(recid,UtilsTest.FAIL));
        e.commit();
        assertNull(e.get(recid,UtilsTest.FAIL));
    }

    @Ignore //TODO deal with store versioning and feature bits
    @Test public void header_index_inc() throws IOException {
        e.put(new byte[10000],Serializer.BYTE_ARRAY_NOSIZE);
        e.commit();
        e.close();

        //increment store version
        Volume v = Volume.volumeForFile(f,true,false,CC.VOLUME_PAGE_SHIFT, 0);
        v.putUnsignedShort(4,StoreDirect.STORE_VERSION+1);
        v.sync();
        v.close();

        try{
            e = openEngine();
            fail();
        }catch(IOError e){
            Throwable e2 = e;
            while (e2 instanceof IOError){
                e2 = e2.getCause();
            }
            assertTrue(e2.getMessage().contains("version"));
        }
    }

    @Test @Ignore //TODO deal with store versioning and feature bits
    public void header_phys_inc() throws IOException {
        e.put(new byte[10000],Serializer.BYTE_ARRAY_NOSIZE);
        e.commit();
        e.close();

        //increment store version
        File phys = new File(f.getPath());
        Volume v = Volume.volumeForFile(phys,true,false,CC.VOLUME_PAGE_SHIFT, 0);
        v.putUnsignedShort(4,StoreDirect.STORE_VERSION+1);
        v.sync();
        v.close();

        try{
            e = openEngine();
            fail();
        }catch(IOError e){
            Throwable e2 = e;
            while (e2 instanceof IOError){
                e2 = e2.getCause();
            }
            assertTrue(e2.getMessage().contains("version"));
        }
    }

}
