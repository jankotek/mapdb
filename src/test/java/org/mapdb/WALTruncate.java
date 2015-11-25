package org.mapdb;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)

public class WALTruncate {


    final int commitNum;
    final int cutPointSeed;

    public WALTruncate(int commitNum, int cutPointSeed) {
        this.commitNum = commitNum;
        this.cutPointSeed = cutPointSeed;
    }

    @Parameterized.Parameters
    public static List<Object[]> params() throws IOException {
        List ret = new ArrayList();
        int inc = TT.shortTest()?200:20;

        for(int commitNum=1;commitNum<1000;commitNum+=inc){
            for(int cutPointSeed=0;cutPointSeed<600;cutPointSeed+=inc){
                ret.add(new Object[]{commitNum, cutPointSeed});
            }
        }

        return ret;
    }

    @Test public void test(){
        File f = TT.tempDbFile();
        WriteAheadLog wal = new WriteAheadLog(f.getPath());

        for(int i=0;i<commitNum;i++){
            for(int j=0;j<6;j++){
                wal.walPutLong(111L, i);
            }
            wal.commit();
        }

        int cutPoint = new Random(cutPointSeed).nextInt((int) wal.curVol.length());
        wal.curVol.sync();
        wal.curVol.clear(cutPoint, wal.curVol.length());
        File f2 = wal.curVol.getFile();
        wal.close();

        wal = new WriteAheadLog(f.getPath());
        final AtomicLong i = new AtomicLong();
        final AtomicLong c = new AtomicLong();
        wal.open(new WriteAheadLog.WALReplay() {
            @Override
            public void beforeReplayStart() {

            }

            @Override
            public void afterReplayFinished() {

            }

            @Override
            public void writeLong(long offset, long value) {
                assertEquals(111L, offset);
                assertEquals(i.get(), value);
                assertTrue(c.getAndIncrement()<6);
            }

            @Override
            public void writeRecord(long recid, long walId, Volume vol, long volOffset, int length) {
                fail();
            }

            @Override
            public void writeByteArray(long offset, long walId, Volume vol, long volOffset, int length) {
                fail();
            }

            @Override
            public void commit() {
                assertEquals(6, c.get());
                c.set(0);
                i.incrementAndGet();
            }

            @Override
            public void rollback() {
                fail();
            }

            @Override
            public void writeTombstone(long recid) {
                fail();
            }

            @Override
            public void writePreallocate(long recid) {
                fail();
            }
        });

        assertEquals(0,c.get());

        f.delete();
        f2.delete();
    }
}
