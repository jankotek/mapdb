package org.mapdb;


import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.*;

@SuppressWarnings({"rawtypes","unchecked"})
public class StoreWALTest<E extends StoreWAL> extends StoreCachedTest<E>{

    @Override boolean canRollback(){return true;}

    File f = UtilsTest.tempDbFile();


    @Override protected E openEngine() {
        StoreWAL e =new StoreWAL(f.getPath());
        e.init();
        return (E)e;
    }



    @Test
    public void WAL_created(){
        File wal0 = new File(f.getPath()+".wal.0");
        File wal1 = new File(f.getPath()+".wal.1");
        File wal2 = new File(f.getPath()+".wal.2");

        StoreWAL w = openEngine();

        assertTrue(wal0.exists());
        assertTrue(wal0.length()>16);
        assertFalse(wal1.exists());

        w.put("aa",Serializer.STRING);
        w.commit();
        assertTrue(wal0.exists());
        assertTrue(wal0.length()>16);
        assertTrue(wal1.exists());
        assertTrue(wal1.length()>16);
        assertFalse(wal2.exists());

        w.put("aa",Serializer.STRING);
        w.commit();
        assertTrue(wal0.exists());
        assertTrue(wal0.length() > 16);
        assertTrue(wal1.exists());
        assertTrue(wal1.length() > 16);
        assertTrue(wal2.exists());
    }

    @Test public void WAL_replay_long(){
        StoreWAL e = openEngine();
        long v = e.composeIndexVal(1000, e.round16Up(10000), true, true, true);
        long offset = 0xF0000;
        e.walPutLong(offset,v);
        e.commit();
        e.structuralLock.lock();
        e.commitLock.lock();
        e.replayWAL();
        assertEquals(v,e.vol.getLong(offset));
    }

    @Test public void WAL_replay_mixed(){
        StoreWAL e = openEngine();
        e.structuralLock.lock();

        for(int i=0;i<3;i++) {
            long v = e.composeIndexVal(100+i, e.round16Up(10000)+i*16, true, true, true);
            e.walPutLong(0xF0000+i*8, v);
            byte[] d  = new byte[9];
            Arrays.fill(d, (byte) i);
            e.putDataSingleWithoutLink(-1,e.round16Up(100000)+64+i*16,d,0,d.length);
        }
        e.commit();
        e.structuralLock.lock();
        e.commitLock.lock();
        e.replayWAL();

        for(int i=0;i<3;i++) {
            long v = e.composeIndexVal(100+i, e.round16Up(10000)+i*16, true, true, true);
            assertEquals(v, e.vol.getLong(0xF0000+i*8));

            byte[] d  = new byte[9];
            Arrays.fill(d, (byte) i);
            byte[] d2  = new byte[9];

            e.vol.getData(e.round16Up(100000)+64+i*16,d2,0,d2.length);
            assertArrayEquals(d,d2);
        }

    }

    Map<Long,String> fill(StoreWAL e){
        Map<Long,String> ret = new LinkedHashMap<Long, String>();

        for(int i=0;i<1e4;i++){
            String s = UtilsTest.randomString((int) (Math.random()*10000));
            long recid = e.put(s,Serializer.STRING);
            ret.put(recid,s);
        }

        return ret;
    }

    @Test public void compact_file_swap_if_seal(){
        walCompactSwap(true);
    }

    @Test public void compact_file_notswap_if_notseal(){
        walCompactSwap(false);
    }

    protected void walCompactSwap(boolean seal) {
        StoreWAL e = openEngine();
        Map<Long,String> m = fill(e);
        e.commit();
        e.close();

        //copy file into new location
        String compactTarget = e.getWalFileName("c.compactXXX");
        Volume f = new Volume.FileChannelVol(new File(compactTarget));
        Volume.copy(e.vol, f);
        f.sync();
        f.close();

        e = openEngine();
        //modify orig file and close
        Long recid = m.keySet().iterator().next();
        e.update(recid,"aaa", Serializer.STRING);
        if(!seal)
            m.put(recid,"aaa");
        e.commit();
        e.close();

        //now move file so it is valid compacted file
        assertTrue(
                new File(compactTarget)
                        .renameTo(
                                new File(e.getWalFileName("c.compact")))
        );

        //create compaction seal
        String compactSeal = e.getWalFileName("c");
        Volume sealVol = new Volume.FileChannelVol(new File(compactSeal));
        sealVol.ensureAvailable(16);
        sealVol.putLong(8,StoreWAL.WAL_SEAL + (seal?0:1));
        sealVol.sync();
        sealVol.close();

        //now reopen file and check its content
        // change should be reverted, since compaction file was used
        e = openEngine();

        for(Long recid2:m.keySet()){
            assertEquals(m.get(recid2), e.get(recid2,Serializer.STRING));
        }
    }

    @Test(timeout = 100000)
    public void compact_commit_works_during_compact() throws InterruptedException {
        compact_tx_works(false,true);
    }

    @Test(timeout = 100000)
    public void compact_commit_works_after_compact() throws InterruptedException {
        compact_tx_works(false,false);
    }

    @Test(timeout = 100000)
    public void compact_rollback_works_during_compact() throws InterruptedException {
        compact_tx_works(true,true);
    }

    @Test(timeout = 100000)
    public void compact_rollback_works_after_compact() throws InterruptedException {
        compact_tx_works(true,false);
    }

    void compact_tx_works(final boolean rollbacks, final boolean pre) throws InterruptedException {
        final StoreWAL w = openEngine();
        Map<Long,String> m = fill(w);
        w.commit();

        if(pre)
            w.$_TEST_HACK_COMPACT_PRE_COMMIT_WAIT = true;
        else
            w.$_TEST_HACK_COMPACT_POST_COMMIT_WAIT = true;

        Thread t = new Thread(){
            @Override
            public void run() {
                w.compact();
            }
        };
        t.start();

        Thread.sleep(1000);

        //we should be able to commit while compaction is running
        for(Long recid: m.keySet()){
            boolean revert = rollbacks && Math.random()<0.5;
            w.update(recid,"ZZZ",Serializer.STRING);
            if(revert){
                w.rollback();
            }else {
                w.commit();
                m.put(recid, "ZZZ");
            }
        }

        if(pre)
            assertTrue(t.isAlive());

        Thread.sleep(1000);

        w.$_TEST_HACK_COMPACT_PRE_COMMIT_WAIT = false;
        w.$_TEST_HACK_COMPACT_POST_COMMIT_WAIT = false;

        t.join();

        for(Long recid:m.keySet()){
            assertEquals(m.get(recid),w.get(recid,Serializer.STRING));
        }

    }

    @Test public void compact_record_file_used() throws IOException {
        StoreWAL w = openEngine();
        Map<Long,String> m = fill(w);
        w.commit();
        w.close();

        //now create fake compaction file, that should be ignored since seal is broken
        String csealFile = w.getWalFileName("c");
        Volume cseal = new Volume.FileChannelVol(new File(csealFile));
        cseal.ensureAvailable(16);
        cseal.putLong(8,234238492376748923L);

        //create record wal file
        String r0 = w.getWalFileName("r0");
        Volume r = new Volume.FileChannelVol(new File(r0));
        r.ensureAvailable(100000);
        r.putLong(8,StoreWAL.WAL_SEAL);

        long offset = 16;
        //modify all records in map via record wal
        for(long recid:m.keySet()){
            r.putUnsignedByte(offset++, 5 << 5);
            r.putSixLong(offset, recid);
            offset+=6;
            String val = "aa"+recid;
            m.put(recid, val);
            DataIO.DataOutputByteArray b = new DataIO.DataOutputByteArray();
            Serializer.STRING.serialize(b, val);
            int size = b.pos;
            r.putInt(offset,size);
            offset+=4;
            r.putData(offset,b.buf,0,size);
            offset+=size;
        }
        r.putUnsignedByte(offset,0);
        r.sync();
        r.putLong(8,StoreWAL.WAL_SEAL);
        r.sync();
        r.close();

        //reopen engine, record WAL should be replayed
        w = openEngine();

        //check content of log file replayed into main store
        for(long recid:m.keySet()){
            assertEquals(m.get(recid), w.get(recid,Serializer.STRING));
        }
    }

}
