package org.mapdb;

import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class WriteAheadLogTest {


    @Test
    public void null_record() {
        testRecord(11111L, null);
    }

    @Test
    public void zero_record() {
        testRecord(11111L, new byte[0]);
    }

    @Test
    public void ten_record() {
        testRecord(11111L, TT.randomByteArray(10));
    }


    @Test
    public void large_record() {
        testRecord(11111L, TT.randomByteArray(1000000));
    }


    void testRecord(final long recid, final byte[] data) {
        WriteAheadLog wal = new WriteAheadLog(null);
        wal.open(WriteAheadLog.NOREPLAY);
        wal.startNextFile();

        final AtomicBoolean called = new AtomicBoolean();

        final long pointer = wal.walPutRecord(recid, data, 0, data == null ? 0 : data.length);

        for (int i = 0; i < 1; i++) {
            byte[] val = wal.walGetRecord(pointer, recid);

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
            public void afterReplayFinished() {

            }

            @Override
            public void writeLong(long offset, long value) {
                fail();
            }

            @Override
            public void writeRecord(long recid2, long walId, Volume vol, long volOffset, int length) {

                assertFalse(called.getAndSet(true));

                assertEquals(recid, recid2);
                if (data == null) {
                    assertNull(vol);
                    assertEquals(walId, 0);
                    assertEquals(volOffset, 0);
                    assertEquals(length, 0);
                } else {
                    byte[] data = new byte[length];
                    vol.getData(volOffset, data, 0, data.length);
                    assertTrue(Arrays.equals(data, data));
                    assertEquals(pointer, walId);
                }
            }

            @Override
            public void writeByteArray(long offset2, long walId, Volume vol, long volOffset, int length) {
                fail();
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


    @Test
    public void tombstone() {
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
            public void afterReplayFinished() {

            }

            @Override
            public void writeLong(long offset, long value) {
                fail();
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
        assertEquals(1, c.get());
    }

    @Test
    public void preallocate() {
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
            public void afterReplayFinished() {

            }

            @Override
            public void writeLong(long offset, long value) {
                fail();
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
        assertEquals(1, c.get());
    }

    @Test
    public void commit() {
        WriteAheadLog wal = new WriteAheadLog(null);
        wal.open(WriteAheadLog.NOREPLAY);
        wal.walPutLong(111L, 1111L);
        wal.commit();
        wal.seal();

        wal.replayWAL(new WALSequence(
                new Object[]{WALSequence.beforeReplayStart},
                new Object[]{WALSequence.writeLong, 111L, 1111L},
                new Object[]{WALSequence.commit}
        ));
    }

    @Test
    public void rollback() {
        WriteAheadLog wal = new WriteAheadLog(null);
        wal.open(WriteAheadLog.NOREPLAY);
        wal.startNextFile();

        wal.walPutLong(111L, 1000);
        wal.rollback();
        wal.seal();

        wal.replayWAL(new WALSequence(
                new Object[]{WALSequence.beforeReplayStart},
                new Object[]{WALSequence.writeLong, 111L, 1000L},
                new Object[]{WALSequence.rollback}
        ));
    }


    @Test
    public void commitChecksum() {
        WriteAheadLog wal = new WriteAheadLog(null);
        wal.open(WriteAheadLog.NOREPLAY);
        wal.startNextFile();

        wal.walPutLong(111L, 1000);
        wal.commit();
        long offset1 = wal.fileOffset - 5;
        int checksum1 = DataIO.longHash(wal.curVol.hash(16, offset1 - 16, 111L));

        assertEquals(checksum1, wal.curVol.getInt(offset1 + 1));
        wal.walPutLong(111L, 1000);
        wal.commit();
        long offset2 = wal.fileOffset - 5;
        int checksum2 = checksum1 + DataIO.longHash(wal.curVol.hash(offset1 + 5, offset2 - offset1 - 5, 111L));
        assertEquals(checksum2, wal.curVol.getInt(offset2 + 1));
    }


    @Test
    public void test_sequence() {
        WALSequence s = new WALSequence(
                new Object[]{WALSequence.commit},
                new Object[]{WALSequence.rollback}
        );

        s.commit();
        s.rollback();
        assertTrue(s.seq.isEmpty());
    }


    //*******************************************

    @Test
    public void lazy_file_create() {
        File f = TT.tempDbFile();
        f.delete();
        File f2 = new File(f.getPath() + ".wal.0");
        WriteAheadLog wal = new WriteAheadLog(f.getPath());
        wal.open(WriteAheadLog.NOREPLAY);

        assertTrue(!f2.exists());
        wal.walPutLong(111L, 111L);
        assertTrue(f2.exists());
        wal.close();
        f2.delete();
    }

    @Test
    public void overflow_byte_array() {
        File f = TT.tempDbFile();
        f.delete();
        File f0 = new File(f.getPath() + ".wal.0");
        File f1 = new File(f.getPath() + ".wal.1");
        WriteAheadLog wal = new WriteAheadLog(f.getPath());
        wal.open(WriteAheadLog.NOREPLAY);

        long lastPos = 0;
        while (!f1.exists()) {
            lastPos = wal.fileOffset;
            wal.walPutByteArray(111L, new byte[100], 0, 100);
            assertTrue(f0.exists());
        }
        assertTrue(WriteAheadLog.MAX_FILE_SIZE - 1000 < lastPos);
        assertTrue(WriteAheadLog.MAX_FILE_SIZE + 120 > lastPos);
        wal.destroyWalFiles();
    }

    @Test
    public void overflow_record() {
        File f = TT.tempDbFile();
        f.delete();
        File f0 = new File(f.getPath() + ".wal.0");
        File f1 = new File(f.getPath() + ".wal.1");
        WriteAheadLog wal = new WriteAheadLog(f.getPath());
        wal.open(WriteAheadLog.NOREPLAY);

        long lastPos = 0;
        while (!f1.exists()) {
            lastPos = wal.fileOffset;
            wal.walPutRecord(111L, new byte[100], 0, 100);
            assertTrue(f0.exists());
        }
        assertTrue(WriteAheadLog.MAX_FILE_SIZE - 1000 < lastPos);
        assertTrue(WriteAheadLog.MAX_FILE_SIZE + 120 > lastPos);
        wal.destroyWalFiles();
    }

    @Test
    public void open_ignores_rollback() {
        File f = TT.tempDbFile();
        WriteAheadLog wal = new WriteAheadLog(f.getPath());
        wal.walPutLong(1L, 11L);
        wal.commit();
        wal.walPutLong(2L, 33L);
        wal.rollback();
        wal.walPutLong(3L, 33L);
        wal.commit();
        wal.seal();
        wal.close();

        wal = new WriteAheadLog(f.getPath());
        wal.open(new WALSequence(
                new Object[]{WALSequence.beforeReplayStart},
                new Object[]{WALSequence.writeLong, 1L, 11L},
                new Object[]{WALSequence.commit},
                // 2L is ignored, rollback section is skipped on hard replay
                new Object[]{WALSequence.writeLong, 3L, 33L},
                new Object[]{WALSequence.commit}
        ));
        wal.destroyWalFiles();
        wal.close();

        f.delete();
    }

    @Test
    public void skip_rollback() {
        WriteAheadLog wal = new WriteAheadLog(null);
        wal.walPutLong(1L, 11L);
        wal.commit();
        long o1 = wal.fileOffset;
        wal.walPutLong(2L, 33L);
        wal.rollback();
        long o2 = wal.fileOffset;
        wal.walPutLong(3L, 33L);
        wal.commit();
        long o3 = wal.fileOffset;
        wal.seal();


        assertEquals(o2, wal.skipRollbacks(o1));
        assertEquals(o2, wal.skipRollbacks(o2));
        assertEquals(0, wal.skipRollbacks(o3));
    }

    @Test
    public void skip_rollback_last_rollback() {
        WriteAheadLog wal = new WriteAheadLog(null);
        wal.walPutLong(1L, 11L);
        wal.commit();
        long o1 = wal.fileOffset;
        wal.walPutLong(2L, 33L);
        wal.commit();
        long o2 = wal.fileOffset;
        wal.walPutLong(3L, 33L);
        wal.rollback();
        wal.seal();

        assertEquals(o1, wal.skipRollbacks(o1));
        assertEquals(0, wal.skipRollbacks(o2));
    }

    @Test
    public void cut_broken_end() {
        String f = TT.tempDbFile().getPath();
        WriteAheadLog wal = new WriteAheadLog(f);
        wal.walPutLong(1L, 11L);
        wal.commit();
        wal.walPutLong(2L, 22L);
        wal.rollback();
        wal.walPutLong(3L, 33L);
        wal.commit();
        wal.walPutLong(4L, 44L);
        wal.curVol.sync();
        wal.close();

        wal = new WriteAheadLog(f);
        wal.open(new WALSequence(
                new Object[]{WALSequence.beforeReplayStart},
                new Object[]{WALSequence.writeLong, 1L, 11L},
                new Object[]{WALSequence.commit},
                new Object[]{WALSequence.writeLong, 3L, 33L},
                new Object[]{WALSequence.commit}
        ));
    }

    @Test
    public void cut_broken_end_rollback() {
        String f = TT.tempDbFile().getPath();
        WriteAheadLog wal = new WriteAheadLog(f);
        wal.walPutLong(1L, 11L);
        wal.commit();
        wal.walPutLong(2L, 22L);
        wal.commit();
        wal.walPutLong(3L, 33L);
        wal.rollback();
        wal.walPutLong(4L, 44L);
        wal.curVol.sync();
        wal.close();

        wal = new WriteAheadLog(f);
        wal.open(new WALSequence(
                new Object[]{WALSequence.beforeReplayStart},
                new Object[]{WALSequence.writeLong, 1L, 11L},
                new Object[]{WALSequence.commit},
                new Object[]{WALSequence.writeLong, 2L, 22L},
                new Object[]{WALSequence.commit}
        ));

    }

    @Test public void replay_commit_over_file_edge(){
        String f = TT.tempDbFile().getPath();
        WriteAheadLog wal = new WriteAheadLog(f);

        byte[] b = TT.randomByteArray(20 * 1024 * 1024);
        wal.walPutRecord(11L, b, 0, b.length);
        wal.walPutRecord(33L, b, 0, b.length);
        wal.commit();
        wal.close();

        wal = new WriteAheadLog(f);
        wal.open(new WALSequence(
                new Object[]{WALSequence.beforeReplayStart},
                new Object[]{WALSequence.writeRecord, 11L, 16L,  b},
                new Object[]{WALSequence.writeRecord, 33L, 4294967312L, b},
                new Object[]{WALSequence.commit}
        ));
    }

    @Test public void empty_commit(){
        String f = TT.tempDbFile().getPath();
        WriteAheadLog wal = new WriteAheadLog(f);

        byte[] b = TT.randomByteArray(1024);
        wal.walPutRecord(33L, b, 0, b.length);
        wal.commit();
        wal.commit();
        wal.seal();
        wal.close();

        wal = new WriteAheadLog(f);
        wal.open(new WALSequence(
                new Object[]{WALSequence.beforeReplayStart},
                new Object[]{WALSequence.writeRecord, 33L, 16L,  b},
                new Object[]{WALSequence.commit},
                new Object[]{WALSequence.commit}
        ));
    }
}