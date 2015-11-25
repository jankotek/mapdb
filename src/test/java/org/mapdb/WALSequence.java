package org.mapdb;

import java.util.LinkedList;

import static org.junit.Assert.*;

/**
 * Test if sequence is matching
 */
public class WALSequence implements WriteAheadLog.WALReplay {

    final java.util.LinkedList<Object[]> seq;



    static final String beforeReplayStart = "beforeReplayStart";
    static final String writeLong = "writeLong";
    static final String writeRecord = "writeRecord";
    static final String writeByteArray = "writeByteArray";
    static final String commit = "commit";
    static final String rollback = "rollback";
    static final String writeTombstone = "writeTombstone";
    static final String writePreallocate = "writePreallocate";

    public WALSequence(Object[]... params) {
        seq = new LinkedList<Object[]>();
        for(Object[] p:params){
            seq.add(p);
        }
    }

    @Override
    public void beforeReplayStart() {
        Object[] r = seq.remove();
        assertEquals(beforeReplayStart, r[0]);
        assertEquals(1,r.length);
    }

    @Override
    public void writeLong(long offset, long value) {
        Object[] r = seq.remove();
        assertEquals(writeLong, r[0]);
        assertEquals(offset,r[1]);
        assertEquals(value,r[2]);
        assertEquals(3,r.length);
    }

    @Override
    public void writeRecord(long recid, long walId, Volume vol, long volOffset, int length) {
        Object[] r = seq.remove();

        byte[] data = new byte[length];
        vol.getData(volOffset, data,0,data.length);

        assertEquals(writeRecord, r[0]);
        assertEquals(recid,r[1]);
        assertEquals(walId, r[2]);
        assertArrayEquals(data, (byte[]) r[3]);
        assertEquals(4,r.length);
    }

    @Override
    public void writeByteArray(long offset, long walId, Volume vol, long volOffset, int length) {
        Object[] r = seq.remove();

        byte[] data = new byte[length];
        vol.getData(volOffset, data,0,data.length);

        assertEquals(writeByteArray, r[0]);
        assertEquals(offset, r[1]);
        assertEquals(walId, r[2]);
        assertArrayEquals(data, (byte[]) r[3]);
        assertEquals(4,r.length);
    }

    @Override
    public void afterReplayFinished() {
        assertTrue(seq.isEmpty());
    }

    @Override
    public void commit() {
        Object[] r = seq.remove();
        assertEquals(commit, r[0]);
        assertEquals(1,r.length);
    }

    @Override
    public void rollback() {
        Object[] r = seq.remove();
        assertEquals(rollback, r[0]);
        assertEquals(1,r.length);
    }

    @Override
    public void writeTombstone(long recid) {
        Object[] r = seq.remove();
        assertEquals(writeTombstone, r[0]);
        assertEquals(recid, r[1]);
        assertEquals(2,r.length);
    }

    @Override
    public void writePreallocate(long recid) {
        Object[] r = seq.remove();
        assertEquals(writePreallocate, r[0]);
        assertEquals(recid, r[1]);
        assertEquals(2,r.length);
    }


}
