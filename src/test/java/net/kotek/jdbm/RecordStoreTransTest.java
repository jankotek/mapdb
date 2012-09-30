package net.kotek.jdbm;


import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class RecordStoreTransTest {
    
    final int stackId = RecordStore.RECID_FREE_PHYS_RECORDS_START+1;

    @Test public void long_stack_basic(){
        RecordStoreTrans t = new RecordStoreTrans(null, false);
        t.longStackPut(stackId,111L);
        t.longStackPut(stackId,112L);
        t.longStackPut(stackId,113L);
        t.longStackPut(stackId,114L);
        t.longStackPut(stackId,115L);

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
        RecordStore r = new RecordStore(f, false);
        for(int i=1;i<1000;i++){
            r.longStackPut(stackId,i);
        }
        r.close();

        RecordStoreTrans t = new RecordStoreTrans(f,false);
        for(int i=999;i!=0;i--){
            assertEquals(i, t.longStackTake(stackId));
        }
    }

    @Test public void transaction_basics() throws IOException {
        File f = File.createTempFile("test","test");
        RecordStore r = new RecordStore(f, false);
        long recid = r.recordPut("aa",Serializer.STRING_SERIALIZER);
        r.close();
        RecordStoreTrans t = new RecordStoreTrans(f,false);
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

}
