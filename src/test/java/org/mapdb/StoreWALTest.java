package org.mapdb;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StoreWALTest extends StoreDirectTest<StoreWAL>{

    Volume.Factory fac = Volume.fileFactory(false,false,Utils.tempDbFile());

    @Override
    protected StoreWAL openEngine() {
        return new StoreWAL(fac,false,false);
    }

    @Test
    public void delete_files_after_close2(){
        File f = Utils.tempDbFile();
        File phys = new File(f.getPath()+StoreDirect.DATA_FILE_EXT);
        File wal = new File(f.getPath()+StoreWAL.TRANS_LOG_FILE_EXT);

        DB db = DBMaker.newFileDB(f).asyncWriteDisable().deleteFilesAfterClose().make();

        db.getHashMap("test").put("aa","bb");
        db.commit();
        assertTrue(f.exists());
        assertTrue(phys.exists());
        assertFalse(wal.exists());
        db.getHashMap("test").put("a12a","bb");
        assertTrue(wal.exists());
        db.close();
        assertFalse(f.exists());
        assertFalse(phys.exists());
        assertFalse(wal.exists());
    }

}
