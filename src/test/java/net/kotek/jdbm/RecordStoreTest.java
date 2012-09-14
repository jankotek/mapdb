package net.kotek.jdbm;


import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class RecordStoreTest extends JdbmTestCase {


    @Test public void testSetGet(){

        long recid  = recman.recordPut(Long.valueOf(10000), Serializer.LONG_SERIALIZER);

        Long  s2 = recman.recordGet(recid, Serializer.LONG_SERIALIZER);

        assertEquals(s2, Long.valueOf(10000));
    }


    @Test public void test_index_record_delete(){
        long recid = recman.recordPut(1000L, Serializer.LONG_SERIALIZER);
        assertEquals(1, countIndexRecords());
        recman.recordDelete(recid);
        assertEquals(0, countIndexRecords());
    }

    @Test public void test_index_record_delete_and_reuse(){
        long recid = recman.recordPut(1000L, Serializer.LONG_SERIALIZER);
        assertEquals(1, countIndexRecords());
        assertEquals(RecordStore.INDEX_OFFSET_START, recid);
        recman.recordDelete(recid);
        assertEquals(0, countIndexRecords());
        long recid2 = recman.recordPut(1000L, Serializer.LONG_SERIALIZER);

        //test that previously deleted index slot was reused
        assertEquals(recid, recid2);
        assertEquals(1, countIndexRecords());
        assertTrue(getIndexRecord(recid) != 0);
    }

    @Test public void test_index_record_delete_and_reuse_large(){
        final long MAX = 10;

        List<Long> recids= new ArrayList<Long>();
        for(int i = 0;i<MAX;i++){
            recids.add(recman.recordPut(0L, Serializer.LONG_SERIALIZER));
        }

        for(long recid:recids){
            recman.recordDelete(recid);
        }

        //now allocate again second recid list
        List<Long> recids2= new ArrayList<Long>();
        for(int i = 0;i<MAX;i++){
            recids2.add(recman.recordPut(0L, Serializer.LONG_SERIALIZER));
        }

        //second list should be reverse of first, as Linked Offset List is LIFO
        Collections.reverse(recids);
        assertEquals(recids, recids2);

    }



    @Test public void test_phys_record_reused(){
        final long recid = recman.recordPut(1L, Serializer.LONG_SERIALIZER);
        final long physRecid = getIndexRecord(recid);
        recman.recordDelete(recid);
        final long recid2 = recman.recordPut(1L, Serializer.LONG_SERIALIZER);

        assertEquals(recid, recid2);
        assertEquals(physRecid, getIndexRecord(recid));

    }



    @Test public void test_index_stores_record_size() throws IOException {

        final long recid = recman.recordPut(1, Serializer.INTEGER_SERIALIZER);
        assertEquals(4, readUnsignedShort(recman.indexBufs[0], recid * 8));
        assertEquals(Integer.valueOf(1), recman.recordGet(recid, Serializer.INTEGER_SERIALIZER));

        recman.recordUpdate(recid, 1L, Serializer.LONG_SERIALIZER);
        assertEquals(8, readUnsignedShort(recman.indexBufs[0], recid * 8));
        assertEquals(Long.valueOf(1), recman.recordGet(recid, Serializer.LONG_SERIALIZER));

    }

    @Test public void test_long_buffer_puts_record_size_into_index() throws IOException {
        recman.lock.writeLock().lock();
        recman.longStackPut(RecordStore.RECID_USER_WHOTEVER, 1);
        assertEquals(RecordStore.LONG_STACK_PAGE_SIZE,
                readUnsignedShort(recman.indexBufs[0], RecordStore.RECID_USER_WHOTEVER * 8));

    }

    @Test public void test_long_buffer_put_take(){
        recman.lock.writeLock().lock();

        final long max = 150;
        for(long i=1;i<max;i++){
            recman.longStackPut(RecordStore.RECID_USER_WHOTEVER, i);
        }

        for(long i = max-1;i>0;i--){
            assertEquals(i, recman.longStackTake(RecordStore.RECID_USER_WHOTEVER));
        }

        assertEquals(0, getLongStack(RecordStore.RECID_USER_WHOTEVER).size());

        }

    @Test public void test_long_buffer_put_take_simple(){
        recman.lock.writeLock().lock();
        recman.longStackPut(RecordStore.RECID_USER_WHOTEVER, 111);
        assertEquals(111L, recman.longStackTake(RecordStore.RECID_USER_WHOTEVER));
    }


    @Test public void test_basic_long_buffer(){

        //dirty hack to make sure we have lock
        recman.lock.writeLock().lock();
        final long max = 150;
        ArrayList<Long> list = new ArrayList<Long>();
        for(long i=1;i<max;i++){
            recman.longStackPut(RecordStore.RECID_USER_WHOTEVER, i);
            list.add(i);
        }

        Collections.reverse(list);

        assertEquals(list, getLongStack(RecordStore.RECID_USER_WHOTEVER));
    }


    @Test  public void test_freePhysRecSize2FreeSlot_asserts(){
        if(!CC.ASSERT) return;
        try{
            recman.freePhysRecSize2FreeSlot(RecordStore.MAX_RECORD_SIZE + 1);
            fail();
        }catch(IllegalArgumentException e){
            //expected
        }

        try{
            recman.freePhysRecSize2FreeSlot(-1);
            fail();
        }catch(IllegalArgumentException e){
            //expected
        }
    }

    @Test  public void test_freePhysRecSize2FreeSlot_incremental(){
        int oldSlot = -1;
        for(int size = 1;size<= RecordStore.MAX_RECORD_SIZE;size++){
            int slot = recman.freePhysRecSize2FreeSlot(size);
            assertTrue(slot >= 0);
            assertTrue(oldSlot <= slot);
            assertTrue(slot - oldSlot <= 1);
            oldSlot= slot;
        }
        assertEquals(RecordStore.NUMBER_OF_PHYS_FREE_SLOT - 1, oldSlot);
    }

    @Test public void test_freePhysRecSize2FreeSlot_max_size_has_unique_slot(){
        int slotMax = recman.freePhysRecSize2FreeSlot(RecordStore.MAX_RECORD_SIZE);
        int slotMaxMinus1 = recman.freePhysRecSize2FreeSlot(RecordStore.MAX_RECORD_SIZE - 1);
        assertEquals(slotMax, slotMaxMinus1 + 1);
    }

    @Test  public void test_freePhys_PutAndTake(){
        recman.lock.writeLock().lock();

        final long offset = 1111000;
        final int size = 344;
        final long indexVal =(((long)size) <<48) |offset;

        recman.freePhysRecPut(indexVal);

        assertEquals(indexVal, recman.freePhysRecTake(size));
        assertEquals(arrayList(), getLongStack(RecordStore.RECID_FREE_PHYS_RECORDS_START + recman.freePhysRecSize2FreeSlot(size)));
    }

    @Test public void test_freePhys_Put_and_Take_2(){

        byte[] zero = new byte[RecordStore.NUMBER_OF_PHYS_FREE_SLOT*8];

        final int inc = CC.FULL_TEST ? 1 : 111;


        recman.lock.writeLock().lock();

        for(int origSize =1;origSize<=1500;origSize+=inc){
            for(int newSize = 1;newSize<=origSize;newSize+=inc){

                final long offset = 111111;
                final long indexVal = ((long)origSize)<<48 | offset;

                recman.freePhysRecPut(indexVal);

                //check it is in list
                assertEquals(arrayList(indexVal),
                        getLongStack(RecordStore.RECID_FREE_PHYS_RECORDS_START
                                + recman.freePhysRecSize2FreeSlot(origSize)));

                //require new record
                final long newIndexVal = recman.freePhysRecTake(newSize);

                assertEquals(((long) newSize) << 48 | offset, newIndexVal);

                //find remaining free record if any
                if(origSize!=newSize){
                    final int rsize = origSize - newSize;
                    final int rslot = recman.freePhysRecSize2FreeSlot(rsize);
                    final long rIndexVal = (offset + newSize) | (((long)rsize)<<48);
                    assertEquals(new Long(rIndexVal),
                            getLongStack(RecordStore.RECID_FREE_PHYS_RECORDS_START + rslot).get(0));
                }



                //zero out all records
                recman.indexBufs[0].position(RecordStore.RECID_FREE_PHYS_RECORDS_START*8);
                recman.indexBufs[0].put(zero);
                recman.indexBufs[0].putLong(RecordStore.RECID_CURRENT_PHYS_FILE_SIZE * 8, 8);
            }
        }
    }



    @Test public void test_2GB_over() throws IOException {
        Assume.assumeTrue(CC.FULL_TEST);

       byte[] data = new byte[51111];
       Integer dataHash = Arrays.hashCode(data);

        Set<Long> recids = new TreeSet<Long>();

        for(int i = 0; i<1e5;i++){
            long recid = recman.recordPut(data, Serializer.BYTE_ARRAY_SERIALIZER);
            recids.add(recid);
        }

        Map<Long,Integer> m1 = new TreeMap<Long, Integer>();
        for(Long l:recids){
            m1.put(l,dataHash);
        }

        Map<Long,Integer> m2 = getDataContent();

        assertEquals(m1.size(), m2.size());
        assertTrue(m1.equals(m2));


    }


    @Test public void test_store_reopen(){
        long recid = recman.recordPut("aaa", Serializer.STRING_SERIALIZER);

        reopenStore();

        String aaa = recman.recordGet(recid, Serializer.STRING_SERIALIZER);
        assertEquals("aaa",aaa);
    }

    @Test  public void test_store_reopen_over_2GB(){
        Assume.assumeTrue(CC.FULL_TEST);

        byte[] data = new byte[11111];
        final long max = RecordStore.BUF_SIZE*2L/data.length;
        final Integer hash = Arrays.hashCode(data);

        List<Long> recids = new ArrayList<Long>();

        for(int i = 0;i<max;i++){
            long recid = recman.recordPut(data,Serializer.BYTE_ARRAY_SERIALIZER);
            recids.add(recid);
        }

        reopenStore();

        for(long recid:recids){
            Integer hash2 = recman.recordGet(recid, Serializer.HASH_DESERIALIZER);
            assertEquals(hash,hash2);
        }
    }

    @Test public void in_memory_test(){
        RecordStore recman = new RecordStore(null);
        Map<Long, Integer> recids = new HashMap<Long,Integer>();
        for(int i = 0;i<100000;i++){
            long recid = recman.recordPut(i, Serializer.BASIC_SERIALIZER);
            recids.put(recid, i);
        }
        for(Long recid: recids.keySet()){
            assertEquals(recids.get(recid), recman.recordGet(recid, Serializer.BASIC_SERIALIZER));
        }
    }




}
