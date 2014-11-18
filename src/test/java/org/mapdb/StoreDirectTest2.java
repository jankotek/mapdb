package org.mapdb;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mapdb.DataIO.*;

public class StoreDirectTest2 {

    @Test public void parity1() {
        assertEquals(Long.parseLong("1", 2), parity1Set(0));
        assertEquals(Long.parseLong("10", 2), parity1Set(2));
        assertEquals(Long.parseLong("111", 2), parity1Set(Long.parseLong("110", 2)));
        assertEquals(Long.parseLong("1110", 2), parity1Set(Long.parseLong("1110", 2)));
        assertEquals(Long.parseLong("1011", 2), parity1Set(Long.parseLong("1010", 2)));
        assertEquals(Long.parseLong("11111", 2), parity1Set(Long.parseLong("11110", 2)));

        assertEquals(0, parity1Get(Long.parseLong("1", 2)));
        try {
            parity1Get(Long.parseLong("0", 2));
            fail();
        }catch(InternalError e){
            //TODO check mapdb specific error;
        }
        try {
            parity1Get(Long.parseLong("110", 2));
            fail();
        }catch(InternalError e){
            //TODO check mapdb specific error;
        }
    }

    @Test public void store_create(){
        StoreDirect st = newStore();
        assertArrayEquals(new long[]{0},st.indexPages);
        st.structuralLock.lock();
        assertEquals(st.headChecksum(), st.vol.getInt(StoreDirect.HEAD_CHECKSUM));
        assertEquals(parity16Set(st.PAGE_SIZE), st.vol.getLong(StoreDirect.STORE_SIZE));
        assertEquals(parity1Set(0), st.vol.getLong(StoreDirect.INDEX_PAGE));
        assertEquals(parity3Set(st.RECID_LAST_RESERVED * 8), st.vol.getLong(StoreDirect.MAX_RECID_OFFSET));
    }

    @Test public void constants(){
        assertEquals(0,(StoreDirect.MAX_REC_SIZE+1)%16);
    }

    @Test public void preallocate1(){
        StoreDirect st = newStore();
        long recid = st.preallocate();
        assertEquals(Engine.RECID_FIRST,recid);
        assertEquals(st.composeIndexVal(0,0,false,true,true),st.vol.getLong(st.recidToOffset(recid)));
        assertEquals(parity3Set(8 * Engine.RECID_FIRST), st.vol.getLong(st.MAX_RECID_OFFSET));
    }


    @Test public void preallocate_M(){
        StoreDirect st = newStore();
        for(long i=0;i<1e6;i++) {
            long recid = st.preallocate();
            assertEquals(Engine.RECID_FIRST+i, recid);
            assertEquals(st.composeIndexVal(0, 0, false, true, true), st.vol.getLong(st.recidToOffset(recid)));
            assertEquals(parity3Set(8 * (Engine.RECID_FIRST + i)), st.vol.getLong(st.MAX_RECID_OFFSET));
        }
    }

    protected StoreDirect newStore() {
        return new StoreDirect(null);
    }

    @Test public void round16Up(){
        assertEquals(0, StoreDirect.round16Up(0));
        assertEquals(16, StoreDirect.round16Up(1));
        assertEquals(16, StoreDirect.round16Up(15));
        assertEquals(16, StoreDirect.round16Up(16));
        assertEquals(32, StoreDirect.round16Up(17));
        assertEquals(32, StoreDirect.round16Up(31));
        assertEquals(32, StoreDirect.round16Up(32));
    }

    @Test public void putGetUpdateDelete(){
        StoreDirect st = newStore();
        String s = "aaaad9009";
        long recid = st.put(s,Serializer.STRING);

        assertEquals(s,st.get(recid,Serializer.STRING));

        s = "da8898fe89w98fw98f9";
        st.update(recid,s,Serializer.STRING);
        assertEquals(s,st.get(recid,Serializer.STRING));

        st.delete(recid,Serializer.STRING);
        assertNull(st.get(recid, Serializer.STRING));
    }

    @Test public void reopen_after_insert(){
        final Volume vol = new Volume.ByteArrayVol(CC.VOLUME_PAGE_SHIFT);

        Fun.Function1<Volume, String> fab = new Fun.Function1<Volume, String>() {
            @Override public Volume run(String s) {
                return vol;
            }
        };
        StoreDirect st = new StoreDirect(null, fab, false, false,null, false,false, 0,false,0);

        Map<Long,String> recids = new HashMap();
        for(long i=0;i<1e6;i++){
            String val = "adskasldaksld "+i;
            long recid = st.put(val,Serializer.STRING);
            recids.put(recid,val);
        }

        //close would destroy Volume,so this will do
        st.commit();

        st = new StoreDirect(null, fab, false, false,null, false,false, 0,false,0);

        for(Map.Entry<Long,String> e:recids.entrySet()){
            assertEquals(e.getValue(), st.get(e.getKey(),Serializer.STRING));
        }


    }


}