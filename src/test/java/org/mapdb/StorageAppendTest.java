package org.mapdb;


import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StorageAppendTest extends StorageTestCase {

    StorageAppend engine = (StorageAppend) super.engine;

    @Override
    protected Engine openEngine() {
        return new StorageAppend(index, false, false, false);
    }

    @Test
    public void compact_file_deleted(){
        File f = Utils.tempDbFile();
        StorageAppend engine = new StorageAppend(f, false,false,false);
        File f1 = engine.getFileNum(1);
        File f2 = engine.getFileNum(2);
        long recid = engine.put(111L, Serializer.LONG_SERIALIZER);
        Long i=0L;
        for(;i<StorageAppend.MAX_FILE_SIZE+1000; i+=8){
            engine.update(recid, i, Serializer.LONG_SERIALIZER);
        }
        i-=8;

        assertTrue(f1.exists());
        assertTrue(f2.exists());
        assertEquals(i, engine.get(recid, Serializer.LONG_SERIALIZER));

        engine.commit();
        assertTrue(f1.exists());
        assertTrue(f2.exists());
        assertEquals(i, engine.get(recid, Serializer.LONG_SERIALIZER));

        engine.compact();
        assertFalse(f1.exists());
        assertTrue(f2.exists());
        assertEquals(i, engine.get(recid, Serializer.LONG_SERIALIZER));

        f1.delete();
        f2.delete();

        engine.close();
    }
}
