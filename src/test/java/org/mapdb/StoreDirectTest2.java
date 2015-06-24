package org.mapdb;

import org.junit.Test;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mapdb.DataIO.*;
import static org.mapdb.StoreDirect.*;

public class StoreDirectTest2 {


    @Test public void store_create(){
        StoreDirect st = newStore();
        assertArrayEquals(new long[]{0},st.indexPages);
        st.structuralLock.lock();
        assertEquals(st.headChecksum(st.vol), st.vol.getInt(StoreDirect.HEAD_CHECKSUM));
        assertEquals(parity16Set(st.PAGE_SIZE), st.vol.getLong(StoreDirect.STORE_SIZE));
        assertEquals(parity16Set(0), st.vol.getLong(StoreDirect.HEAD_END)); //pointer to next page
        assertEquals(parity1Set(st.RECID_LAST_RESERVED * 8), st.vol.getLong(StoreDirect.MAX_RECID_OFFSET));
    }

    @Test public void constants(){
        assertEquals(0,(StoreDirect.MAX_REC_SIZE+1)%16);
    }

    @Test public void preallocate1(){
        StoreDirect st = newStore();
        long recid = st.preallocate();
        assertEquals(Engine.RECID_FIRST,recid);
        assertEquals(st.composeIndexVal(0,0,true,true,true),st.vol.getLong(st.recidToOffset(recid)));
        assertEquals(parity1Set(8 * Engine.RECID_FIRST), st.vol.getLong(st.MAX_RECID_OFFSET));
    }


    @Test public void preallocate_M(){
        StoreDirect st = newStore();
        for(long i=0;i<1e6;i++) {
            long recid = st.preallocate();
            assertEquals(Engine.RECID_FIRST+i, recid);
            assertEquals(st.composeIndexVal(0, 0, true, true, true), st.vol.getLong(st.recidToOffset(recid)));
            assertEquals(parity1Set(8 * (Engine.RECID_FIRST + i)), st.vol.getLong(st.MAX_RECID_OFFSET));
        }
    }

    protected StoreDirect newStore() {
        StoreDirect st =  new StoreDirect(null);
        st.init();
        return st;
    }

    @Test public void round16Up__(){
        assertEquals(0, round16Up(0));
        assertEquals(16, round16Up(1));
        assertEquals(16, round16Up(15));
        assertEquals(16, round16Up(16));
        assertEquals(32, round16Up(17));
        assertEquals(32, round16Up(31));
        assertEquals(32, round16Up(32));
    }



    @Test public void reopen_after_insert(){
        final Volume vol = new Volume.ByteArrayVol(CC.VOLUME_PAGE_SHIFT);

        Volume.VolumeFactory fab = new Volume.VolumeFactory() {
            @Override
            public Volume makeVolume(String file, boolean readOnly, int sliceShift, long initSize, boolean fixedSize) {
                return vol;
            }
        };
        StoreDirect st = new StoreDirect(null, fab, null, CC.DEFAULT_LOCK_SCALE, 0, false, false,null, false,false,  0,false,0, null);
        st.init();

        Map<Long,String> recids = new HashMap();
        for(long i=0;i<1e6;i++){
            String val = "adskasldaksld "+i;
            long recid = st.put(val,Serializer.STRING);
            recids.put(recid,val);
        }

        //close would destroy Volume,so this will do
        st.commit();

        st = new StoreDirect(null, fab, null, CC.DEFAULT_LOCK_SCALE, 0, false, false,null, false, false, 0,false,0, null);
        st.init();

        for(Map.Entry<Long,String> e:recids.entrySet()){
            assertEquals(e.getValue(), st.get(e.getKey(),Serializer.STRING));
        }
    }

    @Test
    public void linked_allocate_two(){
        StoreDirect st = newStore();
        st.structuralLock.lock();
        int recSize = 100000;
        long[] bufs = st.freeDataTake(recSize);

        assertEquals(2,bufs.length);
        assertEquals(MAX_REC_SIZE, bufs[0]>>>48);
        assertEquals(PAGE_SIZE, bufs[0]&MOFFSET);
        assertEquals(MLINKED,bufs[0]&MLINKED);

        assertEquals(recSize-MAX_REC_SIZE+8, bufs[1]>>>48);
        assertEquals(st.PAGE_SIZE + round16Up(MAX_REC_SIZE), bufs[1]&MOFFSET);
        assertEquals(0, bufs[1] & MLINKED);
    }

    @Test
    public void linked_allocate_three(){
        StoreDirect st = newStore();
        st.structuralLock.lock();
        int recSize = 140000;
        long[] bufs = st.freeDataTake(recSize);

        assertEquals(3,bufs.length);
        assertEquals(MAX_REC_SIZE, bufs[0]>>>48);
        assertEquals(PAGE_SIZE, bufs[0]&MOFFSET);
        assertEquals(MLINKED,bufs[0]&MLINKED);

        assertEquals(MAX_REC_SIZE, bufs[1]>>>48);
        assertEquals(st.PAGE_SIZE + round16Up(MAX_REC_SIZE), bufs[1]&MOFFSET);
        assertEquals(MLINKED, bufs[1] & MLINKED);

        assertEquals(recSize-2*MAX_REC_SIZE+2*8, bufs[2]>>>48);
        assertEquals(st.PAGE_SIZE + 2*round16Up(MAX_REC_SIZE), bufs[2]&MOFFSET);
        assertEquals(0, bufs[2] & MLINKED);
    }

    DataOutputByteArray newBuf(int size){
        DataOutputByteArray ret = new DataOutputByteArray();
        for(int i=0;i<size;i++){
            try {
                ret.writeByte(i%255);
            } catch (IOException e) {
                throw new IOError(e);
            }
        }
        return ret;
    }

    @Test public void put_data_single(){
        StoreDirect st = newStore();
        st.structuralLock.lock();
        int totalSize = round16Up(1000);
        long o = st.freeDataTakeSingle(totalSize)&MOFFSET;

        //write data
        long recid = RECID_FIRST;
        long[] offsets = {19L << 48 | o};
        st.locks[st.lockPos(recid)].writeLock().lock();
        st.putData(recid,offsets,newBuf(19).buf,19);

        //verify index val
        assertEquals(19L << 48 | o | MARCHIVE, st.indexValGet(recid));
        //and read data
        for(int i=0;i<totalSize;i++){
            int b = st.vol.getUnsignedByte(o+i);
            assertEquals(i<19?i:0,b);
        }
    }

    @Test public void put_data_double(){
        StoreDirect st = newStore();
        st.structuralLock.lock();
        int totalSize = round16Up(1000);
        long o = st.freeDataTakeSingle(totalSize)&MOFFSET;

        //write data
        long recid = RECID_FIRST;
        long[] offsets = {
                19L << 48 | o | MLINKED,
                100L <<48 | o+round16Up(19)
        };
        st.locks[st.lockPos(recid)].writeLock().lock();
        int bufSize = 19+100-8;
        st.putData(recid,offsets,newBuf(bufSize).buf,bufSize);

        //verify index val
        assertEquals(19L << 48 | o | MLINKED | MARCHIVE, st.indexValGet(recid));
        //verify second pointer
        assertEquals((100L)<<48 | o+round16Up(19) , parity3Get(st.vol.getLong(o)));

        //and read data
        for(int i=0;i<19-8;i++){
            int b = st.vol.getUnsignedByte(o+8+i);
            assertEquals(i,b);
        }
        for(int i=19-8;i<19+100-8;i++){
            int b = st.vol.getUnsignedByte(o+round16Up(19)+i-19+8);
            assertEquals(i,b);
        }

    }

    @Test public void put_data_triple(){
        StoreDirect st = newStore();
        st.structuralLock.lock();
        int totalSize = round16Up(1000);
        long o = st.freeDataTakeSingle(totalSize)&MOFFSET;

        //write data
        long recid = RECID_FIRST;
        long[] offsets = {
                101L << 48 | o | MLINKED,
                102L <<48 | o+round16Up(101) | MLINKED,
                103L <<48 | o+round16Up(101)+round16Up(102)

        };
        st.locks[st.lockPos(recid)].writeLock().lock();
        int bufSize = 101+102+103-2*8;
        st.putData(recid,offsets,newBuf(bufSize).buf,bufSize);

        //verify pointers
        assertEquals(101L << 48 | o | MLINKED | MARCHIVE, st.indexValGet(recid));
        assertEquals(102L<<48 | o+round16Up(101) | MLINKED , parity3Get(st.vol.getLong(o)));

        assertEquals(103L<<48 | o+round16Up(101)+round16Up(102) , parity3Get(st.vol.getLong(o+round16Up(101))));

        //and read data
        for(int i=0;i<101-8;i++){
            int b = st.vol.getUnsignedByte(o+8+i);
            assertEquals(i,b);
        }
        for(int i=0;i<102-8;i++){
            int b = st.vol.getUnsignedByte(o+round16Up(101)+8+i);
            assertEquals(i+101-8,b);
        }

        for(int i=0;i<103-16;i++){
            int b = st.vol.getUnsignedByte(o+round16Up(101)+round16Up(102)+i);
            assertEquals((i+101+102-2*8)%255,b);
        }

    }

    @Test public void zero_index_page_checksum() throws IOException {
        File f = File.createTempFile("mapdb", "mapdb");
        StoreDirect st = (StoreDirect) DBMaker.fileDB(f)
                .transactionDisable()
                .checksumEnable()
                .mmapFileEnableIfSupported()
                .makeEngine();

        //verify checksum of zero index page
        verifyIndexPageChecksum(st);

        st.commit();
        st.close();
        st = (StoreDirect) DBMaker.fileDB(f)
                .transactionDisable()
                .checksumEnable()
                .mmapFileEnableIfSupported()
                .makeEngine();

        for(int i=0;i<2e6;i++){
            st.put(i,Serializer.INTEGER);
        }

        verifyIndexPageChecksum(st);

        st.commit();
        st.close();

        st = (StoreDirect) DBMaker.fileDB(f)
                .transactionDisable()
                .checksumEnable()
                .mmapFileEnableIfSupported()
                .makeEngine();

        verifyIndexPageChecksum(st);

        st.close();
    }

    protected void verifyIndexPageChecksum(StoreDirect st) {
        assertTrue(st.checksum);
        //zero page
        for(long offset=HEAD_END+8;offset+10<=PAGE_SIZE;offset+=10){
            long indexVal = st.vol.getLong(offset);
            int check = st.vol.getUnsignedShort(offset+8);
            if(indexVal==0){
                assertEquals(0,check);
                continue; // not set
            }
            assertEquals(check, DataIO.longHash(indexVal)&0xFFFF);
        }


        for(long page:st.indexPages){
            if(page==0)
                continue;

            for(long offset=page+8;offset+10<=page+PAGE_SIZE;offset+=10){
                long indexVal = st.vol.getLong(offset);
                int check = st.vol.getUnsignedShort(offset+8);
                if(indexVal==0){
                    assertEquals(0,check);
                    continue; // not set
                }
                assertEquals(check, DataIO.longHash(indexVal)&0xFFFF);
            }
        }
    }

    @Test public void recidToOffset(){
        StoreDirect st = (StoreDirect) DBMaker.memoryDB()
                .transactionDisable()
                .makeEngine();

        //fake index pages
        st.indexPages = new long[]{0, PAGE_SIZE*10, PAGE_SIZE*20, PAGE_SIZE*30, PAGE_SIZE*40};
        //put expected content
        Set<Long> m = new HashSet<Long>();
        for(long offset=HEAD_END+8;offset<PAGE_SIZE;offset+=8){
            m.add(offset);
        }

        for(long page=PAGE_SIZE*10;page<=PAGE_SIZE*40; page+=PAGE_SIZE*10){
            for(long offset=page+8;offset<page+PAGE_SIZE;offset+=8){
                m.add(offset);
            }
        }

        long maxRecid = PAGE_SIZE-8-HEAD_END + 4*PAGE_SIZE-4*8;
        //maxRecid is multiple of 8, reduce
        assertEquals(0,maxRecid%8);
        maxRecid/=8;

        //now run recids
        for(long recid=1;recid<=maxRecid;recid++){
            long offset = st.recidToOffset(recid);
            assertTrue(""+recid + " - "+offset+" - "+(offset%PAGE_SIZE),
                    m.remove(offset));
        }
        assertTrue(m.isEmpty());
    }

    @Test public void recidToOffset_with_checksum(){
        StoreDirect st = (StoreDirect) DBMaker.memoryDB()
                .transactionDisable()
                .checksumEnable()
                .makeEngine();

        //fake index pages
        st.indexPages = new long[]{0, PAGE_SIZE*10, PAGE_SIZE*20, PAGE_SIZE*30, PAGE_SIZE*40};
        //put expected content
        Set<Long> m = new HashSet<Long>();
        for(long offset=HEAD_END+8;offset<=PAGE_SIZE-10;offset+=10){
            m.add(offset);
        }

        for(long page=PAGE_SIZE*10;page<=PAGE_SIZE*40; page+=PAGE_SIZE*10){
            for(long offset=page+8;offset<=page+PAGE_SIZE-10;offset+=10){
                m.add(offset);
            }
        }

        long maxRecid = (PAGE_SIZE-8-HEAD_END)/10 + 4*((PAGE_SIZE-8)/10);


        //now run recids
        for(long recid=1;recid<=maxRecid;recid++){
            long offset = st.recidToOffset(recid);
            assertTrue("" + recid + " - " + offset + " - " + (offset % PAGE_SIZE)+ " - " + (offset - PAGE_SIZE),
                    m.remove(offset));
        }
        assertTrue(m.isEmpty());
    }

}