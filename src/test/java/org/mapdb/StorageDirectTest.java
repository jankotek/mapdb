package org.mapdb;


import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class StorageDirectTest extends StorageTestCase {


    StorageDirect engine = (StorageDirect) super.engine;

    /** recid used for testing, it is actually free slot with size 1*/
    static final long TEST_LS_RECID = StorageDirect.RECID_FREE_PHYS_RECORDS_START+1 ;



    @Test public void test_index_record_delete(){
        long recid = engine.put(1000L, Serializer.LONG_SERIALIZER);
        engine.commit();
        assertEquals(1, countIndexRecords());
        engine.delete(recid,Serializer.LONG_SERIALIZER);
        engine.commit();
        assertEquals(0, countIndexRecords());
    }

    @Test public void test_index_record_delete_and_reusef(){
        long recid = engine.put(1000L, Serializer.LONG_SERIALIZER);
        engine.commit();
        assertEquals(1, countIndexRecords());
        assertEquals(Engine.LAST_RESERVED_RECID+1, recid);
        engine.delete(recid,Serializer.LONG_SERIALIZER);
        engine.commit();
        assertEquals(0, countIndexRecords());
        long recid2 = engine.put(1000L, Serializer.LONG_SERIALIZER);
        engine.commit();
        //test that previously deleted index slot was reused
        assertEquals(recid, recid2);
        assertEquals(1, countIndexRecords());
        assertNotEquals(0,engine.index.getLong(recid*8+StorageDirect.INDEX_OFFSET_START*8));
    }

    @Test public void test_index_record_delete_and_reuse_large(){
        final long MAX = 10;

        List<Long> recids= new ArrayList<Long>();
        for(int i = 0;i<MAX;i++){
            recids.add(engine.put(0L, Serializer.LONG_SERIALIZER));
        }

        for(long recid:recids){
            engine.delete(recid,Serializer.LONG_SERIALIZER);
        }

        //now allocate again second recid list
        List<Long> recids2= new ArrayList<Long>();
        for(int i = 0;i<MAX;i++){
            recids2.add(engine.put(0L, Serializer.LONG_SERIALIZER));
        }

        //second list should be reverse of first, as Linked Offset List is LIFO
        Collections.reverse(recids);
        assertEquals(recids, recids2);

    }



    @Test public void test_phys_record_reused(){
        final long recid = engine.put(1L, Serializer.LONG_SERIALIZER);
        final long physRecid = engine.index.getLong(recid*8+StorageDirect.INDEX_OFFSET_START*8);
        engine.delete(recid, Serializer.LONG_SERIALIZER);
        final long recid2 = engine.put(1L, Serializer.LONG_SERIALIZER);

        assertEquals(recid, recid2);
        assertEquals(physRecid, engine.index.getLong(recid*8+StorageDirect.INDEX_OFFSET_START*8));

    }



    @Test public void test_index_stores_record_size() throws IOException {

        final long recid = engine.put(1, Serializer.INTEGER_SERIALIZER);
        engine.commit();
        assertEquals(4, engine.index.getUnsignedShort(recid * 8+StorageDirect.INDEX_OFFSET_START*8));
        assertEquals(Integer.valueOf(1), engine.get(recid, Serializer.INTEGER_SERIALIZER));

        engine.update(recid, 1L, Serializer.LONG_SERIALIZER);
        engine.commit();
        assertEquals(8, engine.index.getUnsignedShort(recid * 8+StorageDirect.INDEX_OFFSET_START*8));
        assertEquals(Long.valueOf(1), engine.get(recid, Serializer.LONG_SERIALIZER));

    }

    @Test public void test_long_stack_puts_record_size_into_index() throws IOException {
        engine.lock.writeLock().lock();
        engine.longStackPut(TEST_LS_RECID, 1);
        engine.commit();
        assertEquals(StorageDirect.LONG_STACK_PAGE_SIZE,
                engine.index.getUnsignedShort(TEST_LS_RECID * 8));

    }

    @Test public void test_long_stack_put_take() throws IOException {
        engine.lock.writeLock().lock();

        final long max = 150;
        for(long i=1;i<max;i++){
            engine.longStackPut(TEST_LS_RECID, i);
        }

        for(long i = max-1;i>0;i--){
            assertEquals(i, engine.longStackTake(TEST_LS_RECID));
        }

        assertEquals(0, getLongStack(TEST_LS_RECID).size());

    }

    @Test public void test_long_stack_put_take_simple() throws IOException {
        engine.lock.writeLock().lock();
        engine.longStackPut(TEST_LS_RECID, 111);
        assertEquals(111L, engine.longStackTake(TEST_LS_RECID));
    }


    @Test public void test_basic_long_stack() throws IOException {
        //dirty hack to make sure we have lock
        engine.lock.writeLock().lock();
        final long max = 150;
        ArrayList<Long> list = new ArrayList<Long>();
        for(long i=1;i<max;i++){
            engine.longStackPut(TEST_LS_RECID, i);
            list.add(i);
        }

        Collections.reverse(list);
        engine.commit();

        assertEquals(list, getLongStack(TEST_LS_RECID));
    }


    @Test public void long_stack_page_created_after_put() throws IOException {
        engine.lock.writeLock().lock();
        engine.longStackPut(TEST_LS_RECID, 111);
        engine.commit();
        long pageId = engine.index.getLong(TEST_LS_RECID*8);
        assertEquals(StorageDirect.LONG_STACK_PAGE_SIZE, pageId>>>48);
        pageId = pageId & StorageDirect.PHYS_OFFSET_MASK;
        assertEquals(8L, pageId);
        assertEquals(1, engine.phys.getByte(pageId));
        assertEquals(0, engine.phys.getLong(pageId)& StorageDirect.PHYS_OFFSET_MASK);
        assertEquals(111, engine.phys.getLong(pageId + 8));
    }

    @Test public void long_stack_put_five() throws IOException {
        engine.lock.writeLock().lock();
        engine.longStackPut(TEST_LS_RECID, 111);
        engine.longStackPut(TEST_LS_RECID, 112);
        engine.longStackPut(TEST_LS_RECID, 113);
        engine.longStackPut(TEST_LS_RECID, 114);
        engine.longStackPut(TEST_LS_RECID, 115);

        engine.commit();
        long pageId = engine.index.getLong(TEST_LS_RECID*8);
        assertEquals(StorageDirect.LONG_STACK_PAGE_SIZE, pageId>>>48);
        pageId = pageId & StorageDirect.PHYS_OFFSET_MASK;
        assertEquals(8L, pageId);
        assertEquals(5, engine.phys.getByte(pageId));
        assertEquals(0, engine.phys.getLong(pageId)& StorageDirect.PHYS_OFFSET_MASK);
        assertEquals(111, engine.phys.getLong(pageId+8));
        assertEquals(112, engine.phys.getLong(pageId+16));
        assertEquals(113, engine.phys.getLong(pageId + 24));
        assertEquals(114, engine.phys.getLong(pageId+32));
        assertEquals(115, engine.phys.getLong(pageId+40));
    }

    @Test public void long_stack_page_deleted_after_take() throws IOException {
        engine.lock.writeLock().lock();
        engine.longStackPut(TEST_LS_RECID, 111);
        engine.commit();
        assertEquals(111L, engine.longStackTake(TEST_LS_RECID));
        engine.commit();
        assertEquals(0L, engine.index.getLong(TEST_LS_RECID*8));
    }

    @Test public void long_stack_page_overflow() throws IOException {
        engine.lock.writeLock().lock();
        //fill page until near overflow
        for(int i=0;i< StorageDirect.LONG_STACK_NUM_OF_RECORDS_PER_PAGE;i++){
            engine.longStackPut(TEST_LS_RECID, 1000L+i);
        }
        engine.commit();

        //check content
        long pageId = engine.index.getLong(TEST_LS_RECID*8);
        assertEquals(StorageDirect.LONG_STACK_PAGE_SIZE, pageId>>>48);
        pageId = pageId & StorageDirect.PHYS_OFFSET_MASK;
        assertEquals(8L, pageId);
        assertEquals(StorageDirect.LONG_STACK_NUM_OF_RECORDS_PER_PAGE, engine.phys.getByte(pageId));
        for(int i=0;i< StorageDirect.LONG_STACK_NUM_OF_RECORDS_PER_PAGE;i++){
            assertEquals(1000L+i, engine.phys.getLong(pageId+8+i*8));
        }

        //add one more item, this will trigger page overflow
        engine.longStackPut(TEST_LS_RECID, 11L);
        engine.commit();
        //check page overflowed
        pageId = engine.index.getLong(TEST_LS_RECID*8);
        assertEquals(StorageDirect.LONG_STACK_PAGE_SIZE, pageId>>>48);
        pageId = pageId & StorageDirect.PHYS_OFFSET_MASK;
        assertEquals(8L+ StorageDirect.LONG_STACK_PAGE_SIZE, pageId);
        assertEquals(1, engine.phys.getByte(pageId));
        assertEquals(8L, engine.phys.getLong(pageId)& StorageDirect.PHYS_OFFSET_MASK);
        assertEquals(11L, engine.phys.getLong(pageId+8));
    }


    @Test public void test_freePhysRecSize2FreeSlot_asserts(){
        try{
            engine.freePhysRecSize2FreeSlot(StorageDirect.MAX_RECORD_SIZE + 1);
            fail();
        }catch(IllegalArgumentException e){
            //expected
        }

        try{
            engine.freePhysRecSize2FreeSlot(-1);
            fail();
        }catch(IllegalArgumentException e){
            //expected
        }
    }

    @Test public void test_freePhysRecSize2FreeSlot_incremental(){
        int oldSlot = -1;
        for(int size = 1;size<= StorageDirect.MAX_RECORD_SIZE;size++){
            int slot = engine.freePhysRecSize2FreeSlot(size);
            assertTrue(slot >= 0);
            assertTrue(oldSlot <= slot);
            assertTrue(slot - oldSlot <= 1);
            oldSlot= slot;
        }
        assertEquals(StorageDirect.NUMBER_OF_PHYS_FREE_SLOT - 1, oldSlot);
    }

    @Test public void test_freePhysRecSize2FreeSlot_max_size_has_unique_slot(){
        int slotMax = engine.freePhysRecSize2FreeSlot(StorageDirect.MAX_RECORD_SIZE);
        int slotMaxMinus1 = engine.freePhysRecSize2FreeSlot(StorageDirect.MAX_RECORD_SIZE - 1);
        assertEquals(slotMax, slotMaxMinus1 + 1);
    }

    @Test  public void test_freePhys_PutAndTake() throws IOException {
        engine.lock.writeLock().lock();

        final long offset = 1111000;
        final int size = 344;
        final long indexVal =(((long)size) <<48) |offset;

        engine.freePhysRecPut(indexVal);

        assertEquals(indexVal, engine.freePhysRecTake(size));
        assertEquals(arrayList(), getLongStack(StorageDirect.RECID_FREE_PHYS_RECORDS_START + engine.freePhysRecSize2FreeSlot(size)));
    }




    @Test public void in_memory_test(){
        StorageDirect engine = new StorageDirect(Volume.memoryFactory(false));
        Map<Long, Integer> recids = new HashMap<Long,Integer>();
        for(int i = 0;i<1000;i++){
            long recid = engine.put(i, Serializer.BASIC_SERIALIZER);
            recids.put(recid, i);
        }
        for(Long recid: recids.keySet()){
            assertEquals(recids.get(recid), engine.get(recid, Serializer.BASIC_SERIALIZER));
        }
    }


    int countIndexRecords(){
        int ret = 0;
        final long indexFileSize = engine.index.getLong(StorageDirect.RECID_CURRENT_INDEX_FILE_SIZE*8);
        for(int pos = StorageDirect.INDEX_OFFSET_START * 8;
            pos<indexFileSize;
            pos+=8){
            if(0!= engine.index.getLong(pos)){
                ret++;
            }
        }
        return ret;
    }


    List<Long> getLongStack(long recid){

        ArrayList<Long> ret =new ArrayList<Long>();

        long pagePhysid = engine.index.getLong(recid*8) & StorageDirect.PHYS_OFFSET_MASK;


        while(pagePhysid!=0){
            final byte numberOfRecordsInPage = engine.phys.getByte((int) (pagePhysid% Volume.BUF_SIZE));

            for(int rec = numberOfRecordsInPage; rec>0;rec--){
                final long l = engine.phys.getLong((int) (pagePhysid% Volume.BUF_SIZE+ rec*8));
                ret.add(l);
            }

            //read location of previous page
            pagePhysid = engine.phys.getLong((int)(pagePhysid% Volume.BUF_SIZE)) & StorageDirect.PHYS_OFFSET_MASK;
        }

        return ret;
    }

    @Test public void compact(){
        Map<Long,Long> recids = new HashMap<Long, Long>();
        for(Long l=0L;l<1000;l++){
            recids.put(l,
                    engine.put(l, Serializer.LONG_SERIALIZER));
        }

        engine.commit();
        engine.compact();

        for(Map.Entry<Long,Long> e:recids.entrySet()){
            Long recid= e.getValue();
            Long value = e.getKey();
            assertEquals(value, engine.get(recid, Serializer.LONG_SERIALIZER));
        }


    }


    @Test public void compact2(){
        Map<Long,Long> recids = new HashMap<Long, Long>();
        for(Long l=0L;l<1000;l++){
            recids.put(l,
                    engine.put(l, Serializer.LONG_SERIALIZER));
        }

        engine.commit();
        engine.compact();
        for(Long l=1000L;l<2000;l++){
            recids.put(l,
                    engine.put(l, Serializer.LONG_SERIALIZER));
        }

        for(Map.Entry<Long,Long> e:recids.entrySet()){
            Long recid= e.getValue();
            Long value = e.getKey();
            assertEquals(value, engine.get(recid, Serializer.LONG_SERIALIZER));
        }
    }

    @Ignore
    @Test public void compact_large_record(){
        byte[] b = new byte[100000];
        long recid = engine.put(b, BYTE_ARRAY_SERIALIZER);
        engine.compact();
        assertArrayEquals(b, engine.get(recid, BYTE_ARRAY_SERIALIZER));
    }


}
