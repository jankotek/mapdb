package org.mapdb;


import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;
import static org.mapdb.DataIO.*;
import static org.mapdb.StoreDirect.*;

@SuppressWarnings({"rawtypes","unchecked"})
public class StoreDirectTest <E extends StoreDirect> extends EngineTest<E>{

    @Override boolean canRollback(){return false;}

    File f = TT.tempDbFile();


    @After
    public void deleteFile(){
        if(e!=null && !e.isClosed()){
            e.close();
            e = null;
        }
        if(f==null)
            return;

        f.delete();
        String name = f.getName();
        for(File f2:f.getParentFile().listFiles()){
            if(f2.getName().startsWith(name))
                f2.delete();
        }
    }

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
//        assertTrue(Arrays.equals(new long[]{expected}, ret);
//    }
//
//    @Test
//    public void phys_append_alloc_link2(){
//        e.structuralLock.lock();
//        long[] ret = e.physAllocate(100 + MAX_REC_SIZE,true,false);
//        long exp1 = MLINKED |((long)MAX_REC_SIZE)<<48 | 16L;
//        long exp2 = 108L<<48 | (16L+MAX_REC_SIZE+1);
//        assertTrue(Arrays.equals(new long[]{exp1, exp2}, ret);
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
//        assertTrue(Arrays.equals(new long[]{exp1, exp2, exp3}, ret);
//    }
//
//    @Test public void second_rec_pos_round_to_16(){
//        e.structuralLock.lock();
//        long[] ret= e.physAllocate(1,true,false);
//        assertTrue(Arrays.equals(new long[]{1L<<48|16L},ret);
//        ret= e.physAllocate(1,true,false);
//        assertTrue(Arrays.equals(new long[]{1L<<48|32L},ret);
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
        Collections.sort(recids);
        Collections.sort(recids);
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
        assertTrue(0 != (indexVal & StoreDirect.MARCHIVE));
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
        e.longStackPut(FREE_RECID_STACK, 1, false);
        e.structuralLock.unlock();
        e.commit();
        assertEquals(8 + 1,
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
            assertEquals(i, e.longStackTake(FREE_RECID_STACK, false));
        }

        assertEquals(0, getLongStack(FREE_RECID_STACK).size());
        e.structuralLock.unlock();
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
        e.longStackPut(FREE_RECID_STACK, 111, false);
        assertEquals(111L, e.longStackTake(FREE_RECID_STACK, false));
        e.structuralLock.unlock();
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
        e.structuralLock.unlock();
        e.commit();
        e.structuralLock.lock();
        assertEquals(list, getLongStack(FREE_RECID_STACK));
        e.structuralLock.unlock();
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

        e.structuralLock.unlock();
        Collections.reverse(list);
        e.commit();
        e.structuralLock.lock();
        assertEquals(list, getLongStack(FREE_RECID_STACK));
        e.structuralLock.unlock();
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
        e.structuralLock.unlock();
    }

    @Test public void test_large_long_stack_no_commit() throws IOException {
        if(TT.scale()==0)
            return;
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
        e.structuralLock.unlock();
    }



    @Test public void long_stack_page_created_after_put() throws IOException {
        e = openEngine();
        e.structuralLock.lock();
        e.longStackPut(FREE_RECID_STACK, 111, false);
        //update max recid, so paranoid check does not complain
        e.maxRecidSet(111L);
        e.structuralLock.unlock();
        e.commit();
        forceFullReplay(e);

        long pageId = e.vol.getLong(FREE_RECID_STACK);
        assertEquals(8+2, pageId>>>48);
        pageId = pageId & StoreDirect.MOFFSET;
        assertEquals(PAGE_SIZE, pageId);
        assertEquals(LONG_STACK_PREF_SIZE, DataIO.parity4Get(e.vol.getLong(pageId))>>>48);
        assertEquals(0, DataIO.parity4Get(e.vol.getLong(pageId))&MOFFSET);
        assertEquals(DataIO.parity1Set(111 << 1), e.vol.getLongPackBidi(pageId + 8) & DataIO.PACK_LONG_RESULT_MASK);
    }

    @Test public void long_stack_put_five() throws IOException {
        e = openEngine();
        e.structuralLock.lock();
        e.longStackPut(FREE_RECID_STACK, 111,false);
        e.longStackPut(FREE_RECID_STACK, 112, false);
        e.longStackPut(FREE_RECID_STACK, 113, false);
        e.longStackPut(FREE_RECID_STACK, 114,false);
        e.longStackPut(FREE_RECID_STACK, 115, false);
        e.structuralLock.unlock();
        e.commit();
        forceFullReplay(e);

        long pageId = e.vol.getLong(FREE_RECID_STACK);
        long currPageSize = pageId>>>48;
        pageId = pageId & StoreDirect.MOFFSET;
        assertEquals(PAGE_SIZE, pageId);
        assertEquals(LONG_STACK_PREF_SIZE, e.vol.getLong(pageId) >>> 48);
        assertEquals(0, e.vol.getLong(pageId) & MOFFSET); //next link
        long offset = pageId + 8;
        for(int i=111;i<=115;i++){
            long val = e.vol.getLongPackBidi(offset);
            assertEquals(i, DataIO.parity1Get(val & DataIO.PACK_LONG_RESULT_MASK)>>>1);
            offset += val >>> 60;
        }
        assertEquals(currPageSize, offset-pageId);

    }

    @Test public void long_stack_page_deleted_after_take() throws IOException {
        e = openEngine();
        e.structuralLock.lock();
        e.longStackPut(FREE_RECID_STACK, 111, false);
        e.structuralLock.unlock();
        e.commit();
        forceFullReplay(e);

        e.structuralLock.lock();
        assertEquals(111L, e.longStackTake(FREE_RECID_STACK, false));
        e.structuralLock.unlock();
        e.commit();
        forceFullReplay(e);

        assertEquals(0L, DataIO.parity1Get(e.headVol.getLong(FREE_RECID_STACK)));
    }

    @Test public void long_stack_page_deleted_after_take2() throws IOException {
        e = openEngine();
        e.structuralLock.lock();
        e.longStackPut(FREE_RECID_STACK, 111, false);
        e.structuralLock.unlock();
        e.commit();
        e.structuralLock.lock();
        assertEquals(111L, e.longStackTake(FREE_RECID_STACK, false));
        e.structuralLock.unlock();
        e.commit();
        forceFullReplay(e);

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
            if(e.headVol.getLong(FREE_RECID_STACK)>>48 >LONG_STACK_PREF_SIZE-10)
                break;
        }
        e.structuralLock.unlock();
        e.commit();
        e.commitLock.lock();
        e.structuralLock.lock();

        forceFullReplay(e);
        //check content
        long pageId = e.headVol.getLong(FREE_RECID_STACK);
        assertEquals(actualChunkSize, pageId>>>48);
        pageId = pageId & StoreDirect.MOFFSET;
        assertEquals(PAGE_SIZE, pageId);
        assertEquals(StoreDirect.LONG_STACK_PREF_SIZE, e.vol.getLong(pageId)>>>48);
        for(long i=1000,pos=8;;i++){
            long val = e.vol.getLongPackBidi(pageId+pos);
            assertEquals(i, DataIO.parity1Get(val&DataIO.PACK_LONG_RESULT_MASK)>>>1);
            pos+=val>>>60;
            if(pos==actualChunkSize){
                break;
            }
        }

        //add one more item, this will trigger page overflow
        e.longStackPut(FREE_RECID_STACK, 11L,false);
        e.structuralLock.unlock();
        e.commitLock.unlock();
        e.commit();
        e.commitLock.lock();
        e.structuralLock.lock();

        forceFullReplay(e);

        //check page overflowed
        pageId = e.headVol.getLong(FREE_RECID_STACK);
        assertEquals(8+1, pageId>>>48);
        pageId = pageId & StoreDirect.MOFFSET;
        assertEquals(PAGE_SIZE + StoreDirect.LONG_STACK_PREF_SIZE, pageId);
        assertEquals(PAGE_SIZE, DataIO.parity4Get(e.vol.getLong(pageId)) & StoreDirect.MOFFSET); //prev link
        assertEquals(LONG_STACK_PREF_SIZE, e.vol.getLong(pageId)>>>48); //cur page size
        //overflow value
        assertEquals(11L, DataIO.parity1Get(e.vol.getLongPackBidi(pageId+8)&DataIO.PACK_LONG_RESULT_MASK)>>>1);

        //remaining bytes should be zero
        for(long offset = pageId+8+2;offset<pageId+LONG_STACK_PREF_SIZE;offset++){
            assertEquals(0,e.vol.getByte(offset));
        }
        e.structuralLock.unlock();
        e.commitLock.unlock();
    }

    private void forceFullReplay(E e) {
        if(e instanceof  StoreWAL) {
            StoreWAL wal = (StoreWAL) e;
            if (wal.commitLock.isHeldByCurrentThread()){
                wal.replaySoft();
            }else {
                wal.commitLock.lock();
                wal.replaySoft();
                wal.commitLock.unlock();
            }
        }
    }


    @Test public void delete_files_after_close(){
        File f = TT.tempDbFile();
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
        e.delete(recid, Serializer.BYTE_ARRAY_NOSIZE);
        assertEquals(oldFree + 10000, e.getFreeSize());
        e.commit();
        assertEquals(oldFree + 10000, e.getFreeSize());
    }


    @Test public void prealloc(){
        e = openEngine();
        long recid = e.preallocate();
        assertNull(e.get(recid, TT.FAIL));
        e.commit();
        assertNull(e.get(recid, TT.FAIL));
    }

    @Ignore //TODO deal with store versioning and feature bits
    @Test public void header_index_inc() throws IOException {
        e.put(new byte[10000],Serializer.BYTE_ARRAY_NOSIZE);
        e.commit();
        e.close();

        //increment store version
        Volume v = Volume.FileChannelVol.FACTORY.makeVolume(f.getPath(), true);
        v.putUnsignedShort(4, StoreDirect.STORE_VERSION + 1);
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
        v.putUnsignedShort(4, StoreDirect.STORE_VERSION + 1);
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

    @Test public void compact_keeps_volume_type(){
        if(TT.scale()==0)
            return;

        for(final Fun.Function1<Volume,String> fab : VolumeTest.VOL_FABS){
            Volume.VolumeFactory fac = new Volume.VolumeFactory() {
                @Override
                public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisable, int sliceShift, long initSize, boolean fixedSize) {
                    return fab.run(file);
                }
            };
            //init
            File f = TT.tempDbFile();
            e = (E) new StoreDirect(f.getPath(), fac,
                    null,
                    CC.DEFAULT_LOCK_SCALE,
                    0,
                    false,false,null,
                    false,false,false,null,
                    null, 0L, 0L, false);
            e.init();

            //fill with some data

            Map<Long, String> data = new LinkedHashMap();
            for(int i=0;i<1000;i++){
                String ss = TT.randomString(1000);
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

    @Test public void test_free_space(){
        if(TT.shortTest())
            return;

        e = openEngine();

        assertTrue(e.getFreeSize()>=0);

        List<Long> recids = new ArrayList<Long>();
        for(int i=0;i<10000;i++){
            recids.add(
                e.put(TT.randomByteArray(1024), Serializer.BYTE_ARRAY_NOSIZE));
        }
        assertEquals(0, e.getFreeSize());

        e.commit();
        for(Long recid:recids){
            e.delete(recid,Serializer.BYTE_ARRAY_NOSIZE);
        }
        e.commit();

        assertEquals(10000 * 1024, e.getFreeSize());

        e.compact();
        assertTrue(e.getFreeSize() < 100000); //some leftovers after compaction

    }


    @Test public void recid2Offset(){
        e=openEngine();

        //create 2 fake index pages
        e.vol.ensureAvailable(PAGE_SIZE * 12);
        e.indexPages = new long[]{0L, PAGE_SIZE * 3, PAGE_SIZE * 6, PAGE_SIZE * 11};


        //control bitset with expected recid layout
        BitSet b = new BitSet((int) (PAGE_SIZE * 7));
        //fill bitset at places where recids should be
        b.set((int) StoreDirect.HEAD_END + 8, (int) PAGE_SIZE);
        b.set((int)PAGE_SIZE*3+16, (int)PAGE_SIZE*4);
        b.set((int) PAGE_SIZE * 6 + 16, (int) PAGE_SIZE * 7);
        b.set((int) PAGE_SIZE * 11 + 16, (int) PAGE_SIZE * 12);

        //bitset with recid layout generated by recid2Offset
        BitSet b2 = new BitSet((int) (PAGE_SIZE * 7));
        long oldOffset = 0;
        recidLoop:
        for(long recid=1;;recid++){
            long offset = e.recidToOffset(recid);

            assertTrue(oldOffset<offset);
            oldOffset = offset;
            b2.set((int)offset,(int)offset+8);
            if(offset==PAGE_SIZE*12-8)
                break recidLoop;
        }

        for(int offset = 0; offset<b.length();offset++){
            if(b.get(offset)!=b2.get(offset))
                throw new AssertionError("error at offset "+offset);
        }


    }

    @Test public void index_pages_init(){
        if(CC.PARANOID)
            return; //generates broken store, does not work in paranoid mode

        e=openEngine();
        e.close();

        //now create tree index pages
        Volume v = Volume.RandomAccessFileVol.FACTORY.makeVolume(f.getPath(),false);
        v.ensureAvailable(PAGE_SIZE*6);

        v.putLong(HEAD_END, parity16Set(PAGE_SIZE * 2));
        v.putLong(PAGE_SIZE*2, parity16Set(PAGE_SIZE * 4));
        v.putLong(PAGE_SIZE*4, parity16Set(PAGE_SIZE*5));
        v.putLong(PAGE_SIZE * 5, parity16Set(0));
        v.sync();
        v.close();

        //reopen and check index pages
        e=openEngine();
        //if store becomes more paranoid this might fail
        assertArrayEquals(new long[]{0L, PAGE_SIZE*2, PAGE_SIZE*4, PAGE_SIZE*5}, e.indexPages);
        e.close();


        f.delete();
    }

    @Test public void index_pages_overflow_compact(){
        StoreDirect e = (StoreDirect) DBMaker.memoryDB()
                .transactionDisable()
                .makeEngine();

        // Overflow a third page
        long MAX = (StoreDirect.PAGE_SIZE / 8) * 4;
        for(int i = 0;i<MAX;i++){
            e.put(0L, Serializer.LONG);
        }

        e.compact();

        e.close();
    }

    @Test public void index_pages_overflow_compact_after_delete(){
        StoreDirect e = (StoreDirect) DBMaker.memoryDB()
                .transactionDisable()
                .makeEngine();

        // Overflow a third page
        long MAX = (StoreDirect.PAGE_SIZE / 8) * 4;

        // Map of recids and values
        Map<Long, Long> recids = new HashMap<Long, Long>();
        for(int i = 0;i<MAX;i++){
            long val = Long.valueOf(i<<2);
            recids.put(e.put(val, Serializer.LONG), val);
        }

        long filledSize = e.getCurrSize();

        // Randomly select a bunch of recids to delete to create gaps for compacting
        Random rand  = new Random();
        List<Long> toDelete = new ArrayList<Long>();
        for(Long recid : recids.keySet()) {
            if(rand.nextBoolean()) {
                toDelete.add(recid);
            }
        }
        // Delete
        for(Long recid : toDelete) {
            e.delete(recid, Serializer.LONG);
            recids.remove(recid);
        }

        e.compact();

        // Assert free space after delete and compact
        Assert.assertTrue(e.getFreeSize() > 0L);

        // Assert store size has dropped after delete and compact
        Assert.assertTrue(e.getCurrSize() < filledSize);

        // Assert the objects are what we expect to get back
        for(Map.Entry<Long, Long> entry : recids.entrySet()) {
            Assert.assertEquals(entry.getValue(), e.get(entry.getKey(), Serializer.LONG));
        }

        e.close();
    }

    @Test public void many_recids(){
        if(TT.shortTest())
            return;

        long recidCount = 1024*1024/8+1000;

        e = openEngine();
        List<Long> recids = new ArrayList<Long>();
        for(long i=0;i<recidCount;i++){
            long recid = e.put(i, Serializer.LONG);
            recids.add(recid);
        }
        e.commit();
        reopen();
        for(long i=0;i<recidCount;i++){
            long recid = recids.get((int) i);
            assertEquals(new Long(i), e.get(recid,Serializer.LONG));
        }

        e.close();
    }
}
