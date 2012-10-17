package net.kotek.jdbm;


import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class StorageTransTest extends StorageDirectTest {
    
    final int stackId = StorageDirect.RECID_FREE_PHYS_RECORDS_START+1;

    protected Storage openRecordManager() {
        return new StorageTrans(fileName,true,false,false,false);
    }


    @Test public void long_stack_basic(){
        StorageTrans t = new StorageTrans(null, false,true,false,false);
        t.longStackPut(stackId,111L);
        t.longStackPut(stackId,112L);
        t.longStackPut(stackId,113L);
        t.longStackPut(stackId,114L);
        t.longStackPut(stackId,115L);
        commit();

        assertEquals(115L, t.longStackTake(stackId));
        assertEquals(114L, t.longStackTake(stackId));
        assertEquals(113L, t.longStackTake(stackId));
        assertEquals(112L, t.longStackTake(stackId));
        assertEquals(111L, t.longStackTake(stackId));
        assertEquals(0L, t.longStackTake(stackId));
        assertEquals(null, t.longStackAdded[stackId]);
        assertEquals(0, t.longStackAddedSize[stackId]);
    }


    @Test public void long_stack_reuse() throws IOException {
        File f = File.createTempFile("test","test");
        StorageDirect r = new StorageDirect(f, false,false,false,false);
        for(int i=1;i<1000;i++){
            r.longStackPut(stackId,i);
        }
        r.close();

        StorageTrans t = new StorageTrans(f,false,true,false,false);
        for(int i=999;i!=0;i--){
            assertEquals(i, t.longStackTake(stackId));
        }
    }

    @Test public void transaction_basics() throws IOException {
        File f = File.createTempFile("test","test");
        StorageDirect r = new StorageDirect(f, false,false,false,false);
        long recid = r.recordPut("aa",Serializer.STRING_SERIALIZER);
        r.close();
        StorageTrans t = new StorageTrans(f,false,true,false,false);
        assertEquals("aa", t.recordGet(recid, Serializer.STRING_SERIALIZER));
        t.recordUpdate(recid,"bb", Serializer.STRING_SERIALIZER);
        assertEquals("bb", t.recordGet(recid, Serializer.STRING_SERIALIZER));
        t.recordUpdate(recid,"ccc", Serializer.STRING_SERIALIZER);
        assertEquals("ccc", t.recordGet(recid, Serializer.STRING_SERIALIZER));

        long recid2 = t.recordPut("ZZ",Serializer.STRING_SERIALIZER);
        assertEquals("ZZ", t.recordGet(recid2, Serializer.STRING_SERIALIZER));
        t.recordUpdate(recid2,"ZZZ", Serializer.STRING_SERIALIZER);
        assertEquals("ZZZ", t.recordGet(recid2, Serializer.STRING_SERIALIZER));

        t.recordDelete(recid);
        t.recordDelete(recid2);
        assertEquals(null, t.recordGet(recid, Serializer.STRING_SERIALIZER));
        assertEquals(null, t.recordGet(recid2, Serializer.STRING_SERIALIZER));
    }

    @Test public void persisted() throws IOException {
        File f = File.createTempFile("test","test");
        StorageTrans t = new StorageTrans(f,false,false,false,false);
        final long recid = t.recordPut("aa",Serializer.STRING_SERIALIZER);
        t.commit();
        t.close();
        t = new StorageTrans(f,false,false,false,false);
        assertEquals("aa", t.recordGet(recid, Serializer.STRING_SERIALIZER));

        t.recordUpdate(recid, "bb", Serializer.STRING_SERIALIZER);
        t.commit();
        t.close();
        t = new StorageTrans(f,false,false,false,false);
        assertEquals("bb", t.recordGet(recid, Serializer.STRING_SERIALIZER));

        t.recordDelete(recid);
        t.commit();
        t.close();
        t = new StorageTrans(f,false,true, false,false);
        assertEquals(null,t.recordGet(recid,Serializer.STRING_SERIALIZER));

    }

    @Test public void long_stack_put_take() throws IOException {
        File f = File.createTempFile("test","test");
        StorageTrans t = new StorageTrans(f,false,false, false,false);
        t.longStackPut(Storage.RECID_FREE_PHYS_RECORDS_START+1, 112L);
        t.commit();
        t.close();
        t = new StorageTrans(f,false,false,false,false);
        assertEquals(112L, t.longStackTake(Storage.RECID_FREE_PHYS_RECORDS_START + 1));

        t.commit();
        t.close();
        t = new StorageTrans(f,false,true, false,false);
        assertEquals(0L, t.longStackTake(Storage.RECID_FREE_PHYS_RECORDS_START+1));
    }

    @Test public void index_page_created_from_empty() throws IOException {
        File f = File.createTempFile("test","test");
        StorageTrans t = new StorageTrans(f,false,false,false,false);
        t.longStackPut(Storage.RECID_FREE_PHYS_RECORDS_START+1, 112L);
        t.commit();
        t.close();
        t = new StorageTrans(f,false,true,false,false);

    }


    @Test public void delete_file_on_exit() throws IOException {
        File f = File.createTempFile("test","test");
        StorageTrans t = new StorageTrans(f,false,true,false,false);
        t.recordPut("t",Serializer.STRING_SERIALIZER);
        t.close();
        assertFalse(f.exists());
        assertFalse(new File(f.getPath()+Storage.DATA_FILE_EXT).exists());
        assertFalse(new File(f.getPath()+StorageTrans.TRANS_LOG_FILE_EXT).exists());
    }


}
