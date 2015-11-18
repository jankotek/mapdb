package org.mapdb;

import org.junit.Test;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;
import static org.mapdb.DataIO.*;
import static org.mapdb.StoreDirect.*;

public class StoreDirectTest2 {


    @Test public void store_create(){
        StoreDirect st = newStore();
        assertTrue(Arrays.equals(new long[]{0}, st.indexPages));
        st.structuralLock.lock();
        assertEquals(st.headChecksum(st.vol), st.vol.getInt(StoreDirect.HEAD_CHECKSUM));
        assertEquals(parity16Set(st.PAGE_SIZE), st.vol.getLong(StoreDirect.STORE_SIZE));
        assertEquals(parity16Set(0), st.vol.getLong(StoreDirect.HEAD_END)); //pointer to next page
        assertEquals(parity4Set(st.RECID_LAST_RESERVED <<4), st.vol.getLong(StoreDirect.MAX_RECID_OFFSET));
    }

    @Test public void constants(){
        assertEquals(0,(StoreDirect.MAX_REC_SIZE+1)%16);
        assertEquals(0,(StoreDirect.LONG_STACK_MAX_SIZE)%16);
        assertEquals(0,(StoreDirect.LONG_STACK_MIN_SIZE)%16);
        assertEquals(0,(StoreDirect.LONG_STACK_PREF_SIZE)%16);
    }

    @Test public void preallocate1(){
        StoreDirect st = newStore();
        long recid = st.preallocate();
        assertEquals(Engine.RECID_FIRST,recid);
        assertEquals(st.composeIndexVal(0,0,true,true,true),st.vol.getLong(st.recidToOffset(recid)));
        assertEquals(parity4Set(Engine.RECID_FIRST <<4), st.vol.getLong(st.MAX_RECID_OFFSET));
    }


    @Test public void preallocate_M(){
        StoreDirect st = newStore();
        for(long i=0;i<1e6;i++) {
            long recid = st.preallocate();
            assertEquals(Engine.RECID_FIRST+i, recid);
            assertEquals(st.composeIndexVal(0, 0, true, true, true), st.vol.getLong(st.recidToOffset(recid)));
            assertEquals(parity4Set((Engine.RECID_FIRST + i) <<4), st.vol.getLong(st.MAX_RECID_OFFSET));
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
        if(TT.shortTest())
            return;

        File f = TT.tempDbFile();

        StoreDirect st = new StoreDirect(f.getPath(), CC.DEFAULT_FILE_VOLUME_FACTORY,
                null, CC.DEFAULT_LOCK_SCALE, 0, false, false,null, false,false, false, null, null, 0L, 0L, false);
        st.init();

        Map<Long,String> recids = new HashMap();
        for(long i=0;i<1e6;i++){
            String val = "adskasldaksld "+i;
            long recid = st.put(val,Serializer.STRING);
            recids.put(recid,val);
        }

        st.commit();
        st.close();

        st = new StoreDirect(f.getPath(), CC.DEFAULT_FILE_VOLUME_FACTORY,
                null, CC.DEFAULT_LOCK_SCALE, 0, false, false,null, false, false, false, null, null, 0L, 0L, false);
        st.init();

        for(Map.Entry<Long,String> e:recids.entrySet()){
            assertEquals(e.getValue(), st.get(e.getKey(),Serializer.STRING));
        }
        st.close();
        f.delete();
    }

    @Test
    public void linked_allocate_two(){
        StoreDirect st = newStore();
        st.structuralLock.lock();
        int recSize = 100000;
        long[] bufs = st.freeDataTake(recSize);

        assertEquals(2,bufs.length);
        assertEquals(MAX_REC_SIZE, bufs[0] >>> 48);
        assertEquals(PAGE_SIZE, bufs[0] & MOFFSET);
        assertEquals(MLINKED,bufs[0]&MLINKED);

        assertEquals(recSize - MAX_REC_SIZE + 8, bufs[1] >>> 48);
        assertEquals(st.PAGE_SIZE + round16Up(MAX_REC_SIZE), bufs[1] & MOFFSET);
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
        long o = st.freeDataTakeSingle(totalSize,false)&MOFFSET;

        //write data
        long recid = RECID_FIRST;
        long[] offsets = {19L << 48 | o};
        st.locks[st.lockPos(recid)].writeLock().lock();
        st.putData(recid, offsets, newBuf(19).buf, 19);

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
        long o = st.freeDataTakeSingle(totalSize,false)&MOFFSET;

        //write data
        long recid = RECID_FIRST;
        long[] offsets = {
                19L << 48 | o | MLINKED,
                100L <<48 | o+round16Up(19)
        };
        st.locks[st.lockPos(recid)].writeLock().lock();
        int bufSize = 19+100-8;
        st.putData(recid, offsets, newBuf(bufSize).buf, bufSize);

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
        long o = st.freeDataTakeSingle(totalSize,false)&MOFFSET;

        //write data
        long recid = RECID_FIRST;
        long[] offsets = {
                101L << 48 | o | MLINKED,
                102L <<48 | o+round16Up(101) | MLINKED,
                103L <<48 | o+round16Up(101)+round16Up(102)

        };
        st.locks[st.lockPos(recid)].writeLock().lock();
        int bufSize = 101+102+103-2*8;
        st.putData(recid, offsets, newBuf(bufSize).buf, bufSize);

        //verify pointers
        assertEquals(101L << 48 | o | MLINKED | MARCHIVE, st.indexValGet(recid));
        assertEquals(102L << 48 | o + round16Up(101) | MLINKED, parity3Get(st.vol.getLong(o)));

        assertEquals(103L << 48 | o + round16Up(101) + round16Up(102), parity3Get(st.vol.getLong(o + round16Up(101))));

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
        File f = File.createTempFile("mapdbTest", "mapdb");
        StoreDirect st = (StoreDirect) DBMaker.fileDB(f)
                .transactionDisable()
                .checksumEnable()
                .fileMmapEnableIfSupported()
                .makeEngine();

        //verify checksum of zero index page
        verifyIndexPageChecksum(st);

        st.commit();
        st.close();
        st = (StoreDirect) DBMaker.fileDB(f)
                .transactionDisable()
                .checksumEnable()
                .fileMmapEnableIfSupported()
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
                .fileMmapEnableIfSupported()
                .makeEngine();

        verifyIndexPageChecksum(st);

        st.close();
    }

    protected void verifyIndexPageChecksum(StoreDirect st) {
        assertTrue(st.checksum);

        //TODO
//        //zero page
//        for(long offset=HEAD_END+8;offset+10<=PAGE_SIZE;offset+=10){
//            long indexVal = st.vol.getLong(offset);
//            int check = st.vol.getUnsignedShort(offset+8);
//            if(indexVal==0){
//                assertEquals(0,check);
//                continue; // not set
//            }
//            assertEquals(check, DataIO.longHash(indexVal)&0xFFFF);
//        }
//
//
//        for(long page:st.indexPages){
//            if(page==0)
//                continue;
//
//            for(long offset=page+8;offset+10<=page+PAGE_SIZE;offset+=10){
//                long indexVal = st.vol.getLong(offset);
//                int check = st.vol.getUnsignedShort(offset+8);
//                if(indexVal==0){
//                    assertEquals(0,check);
//                    continue; // not set
//                }
//                assertEquals(check, DataIO.longHash(indexVal)&0xFFFF);
//            }
//        }
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
            for(long offset=page+16;offset<page+PAGE_SIZE;offset+=8){
                m.add(offset);
            }
        }

        long maxRecid = PAGE_SIZE-8-HEAD_END + 4*PAGE_SIZE-4*16;
        //maxRecid is multiple of 8, reduce
        assertEquals(0,maxRecid%8);
        maxRecid/=8;

        //now run recids
        for(long recid=1;recid<=maxRecid;recid++){
            long offset = st.recidToOffset(recid);
            assertTrue("" + recid + " - " + offset + " - " + (offset % PAGE_SIZE),
                    m.remove(offset));
        }
        assertTrue(m.isEmpty());
    }

    @Test public void larger_does_not_cause_overlaps(){
        if(TT.shortTest())
            return;

        File f = TT.tempDbFile();
        String s = TT.randomString(40000);

        DB db = DBMaker.fileDB(f).allocateIncrement(2*1024*1024).fileMmapEnable().transactionDisable().make();
        Map m = db.hashMap("test");
        for(int i=0;i<10000;i++){
            m.put(i,s);
        }
        db.close();
        db = DBMaker.fileDB(f).fileMmapEnable().transactionDisable().make();
        m = db.hashMap("test");
        for(int i=0;i<10000;i++){
            assertEquals(s, m.get(i));
        }
        db.close();
        f.delete();
    }

    @Test public void dump_long_stack(){
        StoreDirect st = (StoreDirect) DBMaker.memoryDB()
                .transactionDisable()
                .makeEngine();

        st.structuralLock.lock();
        List<Long> a = new ArrayList<Long>();
        for(long i=10000;i<11000;i++){
            a.add(i);
            st.longStackPut(StoreDirect.FREE_RECID_STACK, i, false);
        }
        List<Long> content = st.longStackDump(StoreDirect.FREE_RECID_STACK);
        Collections.sort(content);
        assertEquals(a.size(), content.size());
        assertEquals(a, content);
    }


    @Test public void storeCheck(){
        StoreDirect st = (StoreDirect) DBMaker.memoryDB()
                .transactionDisable()
                .makeEngine();
        st.storeCheck();
        st.put("aa", Serializer.STRING);
        st.storeCheck();
    }

    @Test public void storeCheck_large(){
        StoreDirect st = (StoreDirect) DBMaker.memoryDB()
                .transactionDisable()
                .makeEngine();
        st.storeCheck();
        st.put(TT.randomString((int) 1e6), Serializer.STRING);
        st.storeCheck();
    }

    @Test public void storeCheck_many_recids(){
        StoreDirect st = (StoreDirect) DBMaker.memoryDB()
                .transactionDisable()
                .makeEngine();
        st.storeCheck();
        for(int i=0;i<1e6;i++){
            st.preallocate();
            if(!TT.shortTest() && i%100==0)
                st.storeCheck();
        }
        st.storeCheck();
    }

    @Test public void storeCheck_map(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        ((StoreDirect)db.engine).storeCheck();
        synchronized (db) {
            db.catPut("DSAADsa", "dasdsa");
        }
        ((StoreDirect)db.engine).storeCheck();
        Map map = db.hashMap("map", Serializer.INTEGER, Serializer.BYTE_ARRAY);
        ((StoreDirect)db.engine).storeCheck();
        long n = (long) (1000);
        Random r = new Random(1);
        while(n-->0){  //LOL :)
            int key = r.nextInt(10000);
            map.put(key, new byte[r.nextInt(100000)]);
            if(r.nextInt(10)<2)
                map.remove(key);

            if(!TT.shortTest())
                ((StoreDirect)db.engine).storeCheck();
        }
        ((StoreDirect)db.engine).storeCheck();
    }

    @Test public void dumpLongStack(){
        StoreDirect st = (StoreDirect) DBMaker.memoryDB()
                .transactionDisable()
                .makeEngine();

        st.structuralLock.lock();
        st.longStackPut(st.longStackMasterLinkOffset(16), 110000L, false);
        Map m = new LinkedHashMap();
        List l = new ArrayList();
        l.add(110000L);
        m.put(16, l);

        assertEquals(m.toString(), st.longStackDumpAll().toString());
    }


    @Test public void recid2Offset(){
        StoreDirect s = (StoreDirect) DBMaker.memoryDB()
                .transactionDisable()
                .makeEngine();

        //create 2 fake index pages
        s.vol.ensureAvailable(PAGE_SIZE * 12);
        s.indexPages = new long[]{0L, PAGE_SIZE * 3, PAGE_SIZE*6, PAGE_SIZE*11};

        //control bitset with expected recid layout
        BitSet b = new BitSet((int) (PAGE_SIZE * 7));
        //fill bitset at places where recids should be
        b.set((int)StoreDirect.HEAD_END+8, (int)PAGE_SIZE);
        b.set((int)PAGE_SIZE*3+16, (int)PAGE_SIZE*4);
        b.set((int) PAGE_SIZE * 6 + 16, (int) PAGE_SIZE * 7);
        b.set((int) PAGE_SIZE * 11 + 16, (int) PAGE_SIZE * 12);

        //bitset with recid layout generated by recid2Offset
        BitSet b2 = new BitSet((int) (PAGE_SIZE * 7));
        long oldOffset = 0;
        recidLoop:
        for(long recid=1;;recid++){
            long offset = s.recidToOffset(recid);

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

    @Test public void longStack_space_reuse(){
        StoreDirect s = (StoreDirect) DBMaker.memoryDB()
                .transactionDisable()
                .makeEngine();

        //create new record and than release it
        long recid = s.put(new byte[256],Serializer.BYTE_ARRAY_NOSIZE);
        s.put(new byte[16], Serializer.BYTE_ARRAY_NOSIZE); //this will make sure store does not collapse
        s.delete(recid, Serializer.BYTE_ARRAY_NOSIZE);

        //get sized of free page
        long indexVal = s.headVol.getLong(FREE_RECID_STACK);
        long offset = indexVal & MOFFSET;
        long pageSize = s.vol.getLong(offset)>>>48;

        //this might change if recid is marked as free first
        assertEquals(256, pageSize);
    }
}