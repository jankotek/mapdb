package org.mapdb;

import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class WriteAheadLogTest {


    @Test public void null_record(){
        testRecord(11111L, null);
    }

    @Test public void zero_record(){
        testRecord(11111L, new byte[0]);
    }

    @Test public void ten_record(){
        testRecord(11111L, TT.randomByteArray(10));
    }


    @Test public void large_record(){
        testRecord(11111L, TT.randomByteArray(1000000));
    }


    void testRecord(final long recid, final byte[] data){
        WriteAheadLog wal = new WriteAheadLog(null);
        wal.open(WriteAheadLog.NOREPLAY);
        wal.startNextFile();

        final AtomicBoolean called = new AtomicBoolean();

        long pointer = wal.walPutRecord(recid,data,0, data==null?0:data.length);

        for(int i=0;i<1;i++) {
            byte[] val = wal.walGetRecord(pointer);

            if (data == null)
                assertNull(val);
            else
                assertTrue(Arrays.equals(data, val));
            wal.seal();
        }

        WriteAheadLog.WALReplay r = new WriteAheadLog.WALReplay() {
            @Override
            public void beforeReplayStart() {
            }

            @Override
            public void writeLong(long offset, long value) {
                fail();
            }

            @Override
            public void writeRecord(long recid2, byte[] data) {
                assertFalse(called.getAndSet(true));

                assertEquals(recid, recid2);
                if(data==null)
                    assertNull(data);
                else
                    assertTrue(Arrays.equals(data,data));
            }

            @Override
            public void writeByteArray(long offset2, byte[] val) {
                fail();
            }

            @Override
            public void beforeDestroyWAL() {
            }

            @Override
            public void commit() {
                fail();
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
        };

        wal.replayWAL(r);

        assertTrue(called.get());
    }


    @Test public void tombstone(){
        WriteAheadLog wal = new WriteAheadLog(null);
        wal.open(WriteAheadLog.NOREPLAY);
        wal.startNextFile();

        wal.walPutTombstone(111111L);
        wal.seal();

        final AtomicInteger c = new AtomicInteger();

        wal.replayWAL(new WriteAheadLog.WALReplay() {
            @Override
            public void beforeReplayStart() {
            }

            @Override
            public void writeLong(long offset, long value) {
                fail();
            }

            @Override
            public void writeRecord(long recid, byte[] data) {
                fail();
            }

            @Override
            public void writeByteArray(long offset, byte[] val) {
                fail();
            }

            @Override
            public void beforeDestroyWAL() {
            }

            @Override
            public void commit() {
                fail();
            }

            @Override
            public void rollback() {
                fail();
            }

            @Override
            public void writeTombstone(long recid) {
                c.incrementAndGet();
                assertEquals(111111L, recid);
            }

            @Override
            public void writePreallocate(long recid) {
                fail();
            }
        });
        assertEquals(1,c.get());
    }

    @Test public void preallocate(){
        WriteAheadLog wal = new WriteAheadLog(null);
        wal.open(WriteAheadLog.NOREPLAY);
        wal.startNextFile();

        wal.walPutPreallocate(111111L);
        wal.seal();

        final AtomicInteger c = new AtomicInteger();

        wal.replayWAL(new WriteAheadLog.WALReplay() {
            @Override
            public void beforeReplayStart() {
            }

            @Override
            public void writeLong(long offset, long value) {
                fail();
            }

            @Override
            public void writeRecord(long recid, byte[] data) {
                fail();
            }

            @Override
            public void writeByteArray(long offset, byte[] val) {
                fail();
            }

            @Override
            public void beforeDestroyWAL() {
            }

            @Override
            public void commit() {
                fail();
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
                c.incrementAndGet();
                assertEquals(111111L, recid);
            }
        });
        assertEquals(1,c.get());
    }

    @Test public void commit(){
        WriteAheadLog wal = new WriteAheadLog(null);
        wal.open(WriteAheadLog.NOREPLAY);
        wal.startNextFile();

        wal.commit();
        wal.seal();

        final AtomicInteger c = new AtomicInteger();

        wal.replayWAL(new WriteAheadLog.WALReplay() {
            @Override
            public void beforeReplayStart() {
            }

            @Override
            public void writeLong(long offset, long value) {
                fail();
            }

            @Override
            public void writeRecord(long recid, byte[] data) {
                fail();
            }

            @Override
            public void writeByteArray(long offset, byte[] val) {
                fail();
            }

            @Override
            public void beforeDestroyWAL() {
            }

            @Override
            public void commit() {
                c.incrementAndGet();
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
        assertEquals(1,c.get());
    }
    @Test public void rollback(){
        WriteAheadLog wal = new WriteAheadLog(null);
        wal.open(WriteAheadLog.NOREPLAY);
        wal.startNextFile();

        wal.rollback();
        wal.seal();

        final AtomicInteger c = new AtomicInteger();

        wal.replayWAL(new WriteAheadLog.WALReplay() {
            @Override
            public void beforeReplayStart() {
            }

            @Override
            public void writeLong(long offset, long value) {
                fail();
            }

            @Override
            public void writeRecord(long recid, byte[] data) {
                fail();
            }

            @Override
            public void writeByteArray(long offset, byte[] val) {
                fail();
            }

            @Override
            public void beforeDestroyWAL() {
            }

            @Override
            public void commit() {
                fail();
            }

            @Override
            public void rollback() {
                c.incrementAndGet();
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
        assertEquals(1,c.get());
    }

    @Test
    public void test_sequence(){
        WALSequence s = new WALSequence(
                new Object[]{WALSequence.commit},
                new Object[]{WALSequence.rollback}
        );

        s.commit();
        s.rollback();
        assertTrue(s.seq.isEmpty());
    }


}