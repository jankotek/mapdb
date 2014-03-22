package org.mapdb;

import org.junit.Test;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.RandomAccessFile;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StoreWALTest extends StoreDirectTest<StoreWAL>{

    Volume.Factory fac = Volume.fileFactory(false,0,f, 0L);

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
        Volume v = Volume.volumeForFile(index,true,false,0);
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
}
