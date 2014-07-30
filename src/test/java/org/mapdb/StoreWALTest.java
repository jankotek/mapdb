package org.mapdb;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class StoreWALTest extends StoreDirectTest<StoreWAL>{

    Volume.Factory fac;



    @Before public void init(){
        fac = Volume.fileFactory(f,0,false, 0L,CC.VOLUME_SLICE_SHIFT,0);
        super.init();
    }


    @Override
    protected StoreWAL openEngine() {
        return new StoreWAL(fac);
    }

    @Override
    boolean canRollback() {
        return true;
    }

    @Test
    public void delete_files_after_close2(){
        File f = UtilsTest.tempDbFile();
        File phys = new File(f.getPath()+StoreDirect.DATA_FILE_EXT);
        File wal = new File(f.getPath()+StoreWAL.TRANS_LOG_FILE_EXT);

        DB db = DBMaker.newFileDB(f).deleteFilesAfterClose().make();

        db.getHashMap("test").put("aa","bb");
        db.commit();
        assertTrue(f.exists());
        assertTrue(phys.exists());
        assertTrue(wal.exists());
        db.getHashMap("test").put("a12a","bb");
        assertTrue(wal.exists());
        db.close();
        assertFalse(f.exists());
        assertFalse(phys.exists());
        assertFalse(wal.exists());
    }



    @Test public void header_index_ver() throws IOException {
        e.put(new byte[10000],Serializer.BYTE_ARRAY_NOSIZE);
        e.commit();
        e.close();

        //increment store version
        File index = new File(f.getPath()+StoreWAL.TRANS_LOG_FILE_EXT);
        Volume v = Volume.volumeForFile(index,true,false,0,CC.VOLUME_SLICE_SHIFT,0);
        v.ensureAvailable(100);
        v.putInt(0,StoreWAL.HEADER);
        v.putUnsignedShort(4,StoreDirect.STORE_VERSION+1);
        v.putLong(8,StoreWAL.LOG_SEAL);
        v.putInt(80,1);
        v.sync();
        v.close();

        try{
            e = openEngine();
            fail();
        }catch(IOError e){
            Throwable e2 = e;
            while (e2 instanceof IOError){
                e2 = e2.getCause();
            }
            assertTrue(e2.getMessage().contains("version"));
        }
    }


    @Test public void replay_good_log() throws IOException {

        final AtomicBoolean replay = new AtomicBoolean(true);

        StoreWAL wal = new StoreWAL(fac){
            @Override
            protected void replayLogFile() {
                if(replay.get())
                    super.replayLogFile();
                else
                    throw new IllegalAccessError();
            }
        };

        DB db = new DB(wal);

        Map m = db.getHashMap("map");

        //fill map and commit
        int max = (int) 1e5;
        for(int i=0;i<max;i++){
            m.put(i,i);
        }
        wal.commit();

        //fill log, commit but do not replay
        replay.set(false);
        try {
            for (int i = max; i < max*2; i++) {
                m.put(i, i);
            }
            wal.commit();
            fail("Should throw an error");
        }catch(IllegalAccessError e){
        }

        wal.log.close();
        wal.phys.close();
        wal.index.close();

        //now reopen and check content
        wal = new StoreWAL(fac);

        db = new DB(wal);

        m = db.getHashMap("map");

        assertEquals(max*2, m.size());
        for(int i=0;i<max;i++){
            assertEquals(i,m.get(i));
        }

    }

    @Test public void discard_corrupted_log() throws IOException {

        final AtomicBoolean replay = new AtomicBoolean(true);

        StoreWAL wal = new StoreWAL(fac){
            @Override
            protected void replayLogFile() {
                if(replay.get())
                    super.replayLogFile();
                else throw new IllegalAccessError();
            }
        };

        DB db = new DB(wal);

        Map m = db.getHashMap("map");

        //fill map and commit
        int max = (int) 1e5;
        for(int i=0;i<max;i++){
            m.put(i,i);
        }
        wal.commit();

        //fill log, commit but do not replay
        replay.set(false);
        try {
            for (int i = max; i < max*2; i++) {
                m.put(i, i);
            }
            wal.commit();
            fail("Should throw an error");
        }catch(IllegalAccessError e){
        }

        //corrupt log randomly
        wal.log.putLong(1000,111111111L);
        wal.log.putLong(2000,111111111L);
        wal.log.sync();
        wal.log.close();
        wal.phys.close();
        wal.index.close();

        //now reopen and check content
        wal = new StoreWAL(fac);

        db = new DB(wal);

        m = db.getHashMap("map");

        assertEquals(max, m.size());
        for(int i=0;i<max;i++){
            assertEquals(i,m.get(i));
        }

    }

}
