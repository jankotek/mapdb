package org.mapdb;


import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;

import static org.junit.Assert.*;
import static org.mapdb.StoreDirect.*;

@SuppressWarnings({"rawtypes","unchecked"})
public class StoreDirectTest <E extends StoreDirect> extends EngineTest<E>{

    @Override boolean canRollback(){return false;}

    File f = UtilsTest.tempDbFile();


//    static final long FREE_RECID_STACK = StoreDirect.IO_FREE_RECID+32;

    @Override protected E openEngine() {
        StoreDirect e =new StoreDirect(f.getPath());
        e.init();
        return (E)e;
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
    @Test public void test_index_record_delete_and_reuse_large_COMPACT(){
        e = openEngine();
        final long MAX = 10;

        List<Long> recids= new ArrayList<Long>();
        for(int i = 0;i<MAX;i++){
            recids.add(e.put(0L, Serializer.LONG));
        }

        for(long recid:recids){
            e.delete(recid,Serializer.LONG);
        }

        //compaction will reclaim recid
        e.commit();
        e.compact();

        //now allocate again second recid list
        List<Long> recids2= new ArrayList<Long>();
        for(int i = 0;i<MAX;i++){
            recids2.add(e.put(0L, Serializer.LONG));
        }

        //second list should be reverse of first, as Linked Offset List is LIFO
        Collections.reverse(recids);
        assertEquals(recids, recids2);
    }
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
    @Test public void test_phys_record_reused_COMPACT(){
        e = openEngine();
        final long recid = e.put(1L, Serializer.LONG);
        assertEquals((Long)1L, e.get(recid, Serializer.LONG));

        e.delete(recid, Serializer.LONG);
        e.commit();
        e.compact();
        final long recid2 = e.put(1L, Serializer.LONG);
        assertEquals((Long)1L, e.get(recid2, Serializer.LONG));
        e.commit();
        assertEquals((Long)1L, e.get(recid2, Serializer.LONG));
        assertEquals(recid, recid2);

        long indexVal = e.indexValGet(recid);
        assertEquals(8L, indexVal>>>48); // size
        assertEquals(e.PAGE_SIZE,
                indexVal&MOFFSET); //offset
        assertEquals(0, indexVal & StoreDirect.MLINKED);
        assertEquals(0, indexVal & StoreDirect.MUNUSED);
        assertNotEquals(0, indexVal & StoreDirect.MARCHIVE);
        e.close();
    }
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
    @Test public void test_long_stack_puts_record_offset_into_index() throws IOException {
        e = openEngine();
        e.structuralLock.lock();
        e.longStackPut(FREE_RECID_STACK, 1,false);
        e.commit();
        assertEquals(8 + 2,
                e.headVol.getLong(FREE_RECID_STACK)>>>48);

    }

    @Test public void test_long_stack_put_take() throws IOException {
        e = openEngine();
        e.structuralLock.lock();

        final long max = 150;
        for(long i=1;i<max;i++){
            e.longStackPut(FREE_RECID_STACK, i,false);
        }

        for(long i = max-1;i>0;i--){
            assertEquals(i, e.longStackTake(FREE_RECID_STACK,false));
        }

        assertEquals(0, getLongStack(FREE_RECID_STACK).size());

    }

    protected List<Long> getLongStack(long masterLinkOffset) {
        List<Long> ret = new ArrayList<Long>();
        for(long v = e.longStackTake(masterLinkOffset,false); v!=0; v=e.longStackTake(masterLinkOffset,false)){
            ret.add(v);
        }
        return ret;
    }

    @Test public void test_long_stack_put_take_simple() throws IOException {
        e = openEngine();
        e.structuralLock.lock();
        e.longStackPut(FREE_RECID_STACK, 111,false);
        assertEquals(111L, e.longStackTake(FREE_RECID_STACK,false));
    }


    @Test public void test_basic_long_stack() throws IOException {
        e = openEngine();
        //dirty hack to make sure we have lock
        e.structuralLock.lock();
        final long max = 150;
        ArrayList<Long> list = new ArrayList<Long>();
        for(long i=1;i<max;i++){
            e.longStackPut(FREE_RECID_STACK, i,false);
            list.add(i);
        }

        Collections.reverse(list);
        e.commit();

        assertEquals(list, getLongStack(FREE_RECID_STACK));
    }

    @Test public void test_large_long_stack() throws IOException {
        e = openEngine();
        //dirty hack to make sure we have lock
        e.structuralLock.lock();
        final long max = 15000;
        ArrayList<Long> list = new ArrayList<Long>();
        for(long i=1;i<max;i++){
            e.longStackPut(FREE_RECID_STACK, i,false);
            list.add(i);
        }

        Collections.reverse(list);
        e.commit();

        assertEquals(list, getLongStack(FREE_RECID_STACK));
    }

    @Test public void test_basic_long_stack_no_commit() throws IOException {
        e = openEngine();
        //dirty hack to make sure we have lock
        e.structuralLock.lock();
        final long max = 150;
        for(long i=1;i<max;i++){
            e.longStackPut(FREE_RECID_STACK, i,false);
        }

        for(long i =max-1;i>=1;i--){
            assertEquals(i, e.longStackTake(FREE_RECID_STACK,false));
        }
    }

    @Test public void test_large_long_stack_no_commit() throws IOException {
        e = openEngine();
        //dirty hack to make sure we have lock
        e.structuralLock.lock();
        final long max = 15000;
        for(long i=1;i<max;i++){
            e.longStackPut(FREE_RECID_STACK, i,false);
        }


        for(long i =max-1;i>=1;i--){
            assertEquals(i, e.longStackTake(FREE_RECID_STACK,false));
        }
    }



    @Test public void long_stack_page_created_after_put() throws IOException {
        e = openEngine();
        e.structuralLock.lock();
        e.longStackPut(FREE_RECID_STACK, 111,false);
        e.commit();

        if(e instanceof StoreWAL){
            //force replay wal
            e.commitLock.lock();
            e.structuralLock.lock();
            ((StoreWAL)e).replayWAL();
            clearEverything();
        }

        long pageId = e.vol.getLong(FREE_RECID_STACK);
        assertEquals(8+2, pageId>>>48);
        pageId = pageId & StoreDirect.MOFFSET;
        assertEquals(PAGE_SIZE, pageId);
        assertEquals(CHUNKSIZE, DataIO.parity4Get(e.vol.getLong(pageId))>>>48);
        assertEquals(0, DataIO.parity4Get(e.vol.getLong(pageId))&MOFFSET);
        assertEquals(DataIO.parity1Set(111<<1), e.vol.getLongPackBidi(pageId + 8)&DataIO.PACK_LONG_RESULT_MASK);
    }

    @Test public void long_stack_put_five() throws IOException {
        e = openEngine();
        e.structuralLock.lock();
        e.longStackPut(FREE_RECID_STACK, 111,false);
        e.longStackPut(FREE_RECID_STACK, 112,false);
        e.longStackPut(FREE_RECID_STACK, 113,false);
        e.longStackPut(FREE_RECID_STACK, 114,false);
        e.longStackPut(FREE_RECID_STACK, 115,false);

        e.commit();
        if(e instanceof  StoreWAL){
            e.commitLock.lock();
            e.structuralLock.lock();
            ((StoreWAL)e).replayWAL();
            clearEverything();
        }
        long pageId = e.vol.getLong(FREE_RECID_STACK);
        long currPageSize = pageId>>>48;
        pageId = pageId & StoreDirect.MOFFSET;
        assertEquals(PAGE_SIZE, pageId);
        assertEquals(CHUNKSIZE, e.vol.getLong(pageId)>>>48);
        assertEquals(0, e.vol.getLong(pageId)&MOFFSET); //next link
        long offset = pageId + 8;
        for(int i=111;i<=115;i++){
            long val = e.vol.getLongPackBidi(offset);
            assertEquals(i, DataIO.parity1Get(val & DataIO.PACK_LONG_RESULT_MASK)>>>1);
            offset += val >>> 56;
        }
        assertEquals(currPageSize, offset-pageId);
    }

    @Test public void long_stack_page_deleted_after_take() throws IOException {
        e = openEngine();
        e.structuralLock.lock();
        e.longStackPut(FREE_RECID_STACK, 111,false);
        e.commit();
        if(e instanceof  StoreWAL){
            e.commitLock.lock();
            e.structuralLock.lock();
            ((StoreWAL)e).replayWAL();
            clearEverything();
            ((StoreWAL)e).walStartNextFile();
        }

        assertEquals(111L, e.longStackTake(FREE_RECID_STACK,false));
        e.commit();
        if(e instanceof  StoreWAL){
            ((StoreWAL)e).replayWAL();
            clearEverything();
            ((StoreWAL)e).walStartNextFile();
        }

        assertEquals(0L, DataIO.parity1Get(e.headVol.getLong(FREE_RECID_STACK)));
    }

    @Test public void long_stack_page_deleted_after_take2() throws IOException {
        e = openEngine();
        e.structuralLock.lock();
        e.longStackPut(FREE_RECID_STACK, 111,false);
        e.commit();

        assertEquals(111L, e.longStackTake(FREE_RECID_STACK,false));
        e.commit();
        if(e instanceof  StoreWAL){
            e.commitLock.lock();
            e.structuralLock.lock();
            ((StoreWAL)e).replayWAL();
            clearEverything();
        }

        assertEquals(0L, DataIO.parity1Get(e.headVol.getLong(FREE_RECID_STACK)));
    }



    @Test public void long_stack_page_overflow() throws IOException {
        e = openEngine();
        e.structuralLock.lock();
        //fill page until near overflow

        int actualChunkSize = 8;
        for(int i=0;;i++){
            long val = 1000L+i;
            e.longStackPut(FREE_RECID_STACK, val ,false);
            actualChunkSize += DataIO.packLongBidi(new byte[8],0,val<<1);
            if(e.headVol.getLong(FREE_RECID_STACK)>>48 >CHUNKSIZE-10)
                break;
        }
        e.commit();
        if(e instanceof  StoreWAL){
            //TODO method to commit and force WAL replay
            e.commitLock.lock();
            e.structuralLock.lock();
            ((StoreWAL)e).replayWAL();
            clearEverything();
            ((StoreWAL)e).walStartNextFile();
        }

        //check content
        long pageId = e.headVol.getLong(FREE_RECID_STACK);
        assertEquals(actualChunkSize, pageId>>>48);
        pageId = pageId & StoreDirect.MOFFSET;
        assertEquals(PAGE_SIZE, pageId);
        assertEquals(StoreDirect.CHUNKSIZE, e.vol.getLong(pageId)>>>48);
        for(long i=1000,pos=8;;i++){
            long val = e.vol.getLongPackBidi(pageId+pos);
            assertEquals(i, DataIO.parity1Get(val&DataIO.PACK_LONG_RESULT_MASK)>>>1);
            pos+=val>>>56;
            if(pos==actualChunkSize){
                break;
            }
        }

        //add one more item, this will trigger page overflow
        e.longStackPut(FREE_RECID_STACK, 11L,false);
        e.commit();
        if(e instanceof  StoreWAL){
            ((StoreWAL)e).replayWAL();
            clearEverything();
            ((StoreWAL)e).walStartNextFile();
        }

        //check page overflowed
        pageId = e.headVol.getLong(FREE_RECID_STACK);
        assertEquals(8+2, pageId>>>48);
        pageId = pageId & StoreDirect.MOFFSET;
        assertEquals(PAGE_SIZE + StoreDirect.CHUNKSIZE, pageId);
        assertEquals(PAGE_SIZE, DataIO.parity4Get(e.vol.getLong(pageId)) & StoreDirect.MOFFSET); //prev link
        assertEquals(CHUNKSIZE, e.vol.getLong(pageId)>>>48); //cur page size
        //overflow value
        assertEquals(11L, DataIO.parity1Get(e.vol.getLongPackBidi(pageId+8)&DataIO.PACK_LONG_RESULT_MASK)>>>1);

        //remaining bytes should be zero
        for(long offset = pageId+8+2;offset<pageId+CHUNKSIZE;offset++){
            assertEquals(0,e.vol.getByte(offset));
        }
    }


    @Test public void test_constants(){
        assertTrue(StoreDirect.CHUNKSIZE%16==0);
        
    }


    @Test public void delete_files_after_close(){
        File f = UtilsTest.tempDbFile();
        File phys = new File(f.getPath());

        DB db = DBMaker.fileDB(f).transactionDisable().deleteFilesAfterClose().make();

        db.hashMap("test").put("aa","bb");
        db.commit();
        assertTrue(f.exists());
        assertTrue(phys.exists());
        db.close();
        assertFalse(f.exists());
        assertFalse(new File(f+".0.wal").exists());
        assertFalse(phys.exists());
    }

    @Test @Ignore //TODO free space stats
    public void freeSpaceWorks(){
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
        e = openEngine();
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
        Volume v = Volume.FileChannelVol.FACTORY.makeVolume(f.getPath(), true);
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
        Volume v = Volume.FileChannelVol.FACTORY.makeVolume(phys.getPath(), true);
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

    //TODO hack remove
    protected void clearEverything(){
        StoreWAL wal = (StoreWAL)e;
        //flush modified records
        for (int segment = 0; segment < wal.locks.length; segment++) {
            Lock lock = wal.locks[segment].writeLock();
            lock.lock();
            try {
                wal.writeCache[segment].clear();
            } finally {
                lock.unlock();
            }
        }

        wal.structuralLock.lock();
        try {
            wal.dirtyStackPages.clear();

            //restore headVol from backup
            byte[] b = new byte[(int) HEAD_END];
            //TODO use direct copy
            wal.headVolBackup.getData(0,b,0,b.length);
            wal.headVol.putData(0,b,0,b.length);

            wal.indexPages = wal.indexPagesBackup.clone();
            wal.pageLongStack.clear();
        } finally {
            wal.structuralLock.unlock();
        }

    }


    @Test public void compact_keeps_volume_type(){
        for(final Fun.Function1<Volume,String> fab : VolumeTest.VOL_FABS){
            Volume.VolumeFactory fac = new Volume.VolumeFactory() {
                @Override
                public Volume makeVolume(String file, boolean readOnly, int sliceShift, long initSize, boolean fixedSize) {
                    return fab.run(file);
                }
            };
            //init
            File f = UtilsTest.tempDbFile();
            e = (E) new StoreDirect(f.getPath(), fac,
                    null,
                    CC.DEFAULT_LOCK_SCALE,
                    0,
                    false,false,null,
                    false,false,0,
                    false,0,
                    null);
            e.init();

            //fill with some data

            Map<Long, String> data = new LinkedHashMap();
            for(int i=0;i<1000;i++){
                String ss = UtilsTest.randomString(1000);
                long recid = e.put(ss,Serializer.STRING);
            }

            //perform compact and check data
            Volume vol = e.vol;
            e.commit();
            e.compact();

            assertEquals(vol.getClass(), e.vol.getClass());
            if(e.vol.getFile()!=null)
                assertEquals(f, e.vol.getFile());

            for(Long recid:data.keySet()){
                assertEquals(data.get(recid), e.get(recid, Serializer.STRING));
            }
            e.close();
            f.delete();
        }
    }

}
