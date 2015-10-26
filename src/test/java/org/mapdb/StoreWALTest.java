package org.mapdb;


import org.junit.Ignore;
import org.junit.Test;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.*;

@SuppressWarnings({"rawtypes","unchecked"})
public class StoreWALTest<E extends StoreWAL> extends StoreCachedTest<E>{

    @Ignore //TODO finish this test
    public static class ReplaySoftEqualsReplayHard extends StoreWALTest{
        @Override
        protected StoreWAL openEngine() {

            StoreWAL e =new StoreWAL(f.getPath()){
                @Override
                protected void replaySoft() {
                    //take copy of all files including WAL before replay
                    File curFile = new File(fileName);
                    if(!curFile.exists()){
                        super.replaySoft();
                        return;
                    }
                    File dir = TT.tempDbDir();

                    for(File from:curFile.getParentFile().listFiles()){
                        if(from.getName().contains(curFile.getName())) {
                            copyFile(from, new File(dir, from.getName()));
                        }
                    }

                    assertTrue(dir.listFiles().length>0);

                    super.replaySoft();
                    storeCheck();

                    //open the other file, that will replay WAL
                    StoreWAL walCopy = new StoreWAL(dir.getPath()+"/"+curFile.getName());
                    walCopy.init();
                    walCopy.storeCheck();
                    walCopy.close();

                    //TODO compare records from both files

                    TT.dirDelete(dir);
                }
            };
            e.init();
            return e;
        }

        static void copyFile(File from, File to){
            try {
                FileChannel inputChannel = new FileInputStream(from).getChannel();
                FileChannel outputChannel = new FileOutputStream(to).getChannel();
                outputChannel.transferFrom(inputChannel, 0, inputChannel.size());
                inputChannel.close();
                outputChannel.close();
            }catch( IOException e ) {
                throw new IOError(e);
            }
        }
    }

    @Override boolean canRollback(){return true;}


    @Override protected E openEngine() {
        StoreWAL e =new StoreWAL(f.getPath());
        e.init();
        return (E)e;
    }



    @Test @Ignore //TODO do not replay on every commit
    public void WAL_created(){
        File wal0 = new File(f.getPath()+".wal.0");
        File wal1 = new File(f.getPath()+".wal.1");
        File wal2 = new File(f.getPath()+".wal.2");

        e = openEngine();

        assertFalse(wal0.exists());
        assertFalse(wal1.exists());

        e.put("aa", Serializer.STRING);
        e.commit();
        assertTrue(wal0.exists());
        assertFalse(wal1.exists());
        assertFalse(wal2.exists());

        e.put("aa", Serializer.STRING);
        e.commit();
        assertTrue(wal0.exists());
        assertFalse(wal1.exists());
        assertFalse(wal2.exists());
    }

    Map<Long,String> fill(StoreWAL e){
        Map<Long,String> ret = new LinkedHashMap<Long, String>();

        for(int i=0;i<1000;i++){
            String s = TT.randomString((int) (Math.random() * 10000));
            long recid = e.put(s,Serializer.STRING);
            ret.put(recid, s);
        }

        return ret;
    }

    @Test @Ignore
    public void compact_file_swap_if_seal(){
        walCompactSwap(true);
    }

    @Ignore
    @Test public void test_index_record_delete_and_reuse_large_COMPACT() {
    }

    @Ignore
    @Test public void compact_double_recid_reuse(){
    }

    @Test @Ignore
    public void get_non_existent_after_delete_and_compact() {
    }

    @Test public void compact_file_notswap_if_notseal(){
        walCompactSwap(false);
    }

    protected void walCompactSwap(boolean seal) {
        e = openEngine();
        Map<Long,String> m = fill(e);
        e.commit();
        e.close();

        //copy file into new location
        String compactTarget = e.wal.getWalFileName("c.compactXXX");
        Volume f0 = new Volume.FileChannelVol(f);
        Volume f = new Volume.FileChannelVol(new File(compactTarget));
        f0.copyEntireVolumeTo(f);
        f0.close();
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
                                new File(e.wal.getWalFileName("c.compact")))
        );

        //create compaction seal
        String compactSeal = e.wal.getWalFileName("c");
        Volume sealVol = new Volume.FileChannelVol(new File(compactSeal));
        sealVol.ensureAvailable(16);
        sealVol.putLong(8,WriteAheadLog.WAL_SEAL + (seal?0:1));
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
        if(TT.scale()==0)
            return;
        e = openEngine();
        Map<Long,String> m = fill(e);
        e.commit();

        if(pre)
            e.$_TEST_HACK_COMPACT_PRE_COMMIT_WAIT = true;
        else
            e.$_TEST_HACK_COMPACT_POST_COMMIT_WAIT = true;

        Thread t = new Thread(){
            @Override
            public void run() {
                e.compact();
            }
        };
        t.start();

        Thread.sleep(1000);

        //we should be able to commit while compaction is running
        for(Long recid: m.keySet()){
            boolean revert = rollbacks && Math.random()<0.5;
            e.update(recid, "ZZZ", Serializer.STRING);
            if(revert){
                e.rollback();
            }else {
                e.commit();
                m.put(recid, "ZZZ");
            }
        }

        if(pre)
            assertTrue(t.isAlive());

        Thread.sleep(1000);

        e.$_TEST_HACK_COMPACT_PRE_COMMIT_WAIT = false;
        e.$_TEST_HACK_COMPACT_POST_COMMIT_WAIT = false;

        t.join();

        for(Long recid:m.keySet()){
            assertEquals(m.get(recid), e.get(recid, Serializer.STRING));
        }

        e.close();
    }

    @Ignore
    @Test public void compact_record_file_used() throws IOException {
        e = openEngine();
        Map<Long,String> m = fill(e);
        e.commit();
        e.close();

        //now create fake compaction file, that should be ignored since seal is broken
        String csealFile = e.wal.getWalFileName("c");
        Volume cseal = new Volume.FileChannelVol(new File(csealFile));
        cseal.ensureAvailable(16);
        cseal.putLong(8,234238492376748923L);
        cseal.close();

        //create record wal file
        String r0 = e.wal.getWalFileName("r0");
        Volume r = new Volume.FileChannelVol(new File(r0));
        r.ensureAvailable(100000);
        r.putLong(8,WriteAheadLog.WAL_SEAL);

        long offset = 16;
        //modify all records in map via record wal
        for(long recid:m.keySet()){
            r.putUnsignedByte(offset++, 5 << 4);
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
        r.putLong(8,WriteAheadLog.WAL_SEAL);
        r.sync();
        r.close();

        //reopen engine, record WAL should be replayed
        e = openEngine();

        //check content of log file replayed into main store
        for(long recid:m.keySet()){
            assertEquals(m.get(recid), e.get(recid, Serializer.STRING));
        }
        e.close();
    }

    @Test public void header(){
        StoreWAL s = openEngine();
        s.wal.walPutLong(111L, 1111L);
        assertEquals(StoreWAL.HEADER,s.vol.getInt(0));
        assertEquals(WriteAheadLog.WAL_HEADER,s.wal.curVol.getInt(0));
    }

    @Test public void freed_remove_creates_tomstone(){
        e = openEngine();

        long recid = e.put("aaaa",Serializer.STRING_NOSIZE);
        int segment = e.lockPos(recid);
        e.commitLock.lock();
        e.flushWriteCache();
        e.commitLock.unlock();
        long[] orig = e.uncommittedDataLongs[segment].table.clone();
        assertEquals(1,e.uncommittedDataLongs[segment].size());

        e.delete(recid,Serializer.STRING_NOSIZE);
        e.commitLock.lock();
        e.flushWriteCache();
        e.commitLock.unlock();
        assertEquals(1,e.uncommittedDataLongs[segment].size());
        assertFalse(Arrays.equals(orig, e.uncommittedDataLongs[segment].table));

        e.commit();
        e.commitLock.lock();
        e.replaySoft();
        e.commitLock.unlock();
    }

    @Test public void crash_recovery(){
        long c = 0;
        e = (E) DBMaker.fileDB(f).fileLockDisable().makeEngine();
        long recid = e.put(0L, Serializer.LONG);
        e.commit();
        e.close();
        for(int i=0;i<50;i++){
            e = (E) DBMaker.fileDB(f).fileLockDisable().makeEngine();
            assertEquals(new Long(c), e.get(recid,Serializer.LONG));

            if(i%5==0){
                //no commit
                e.update(recid, -c, Serializer.LONG);
            }else{
                c++;
                e.update(recid, c, Serializer.LONG);
                e.commit();
            }
        }
    }
}
