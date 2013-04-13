package org.mapdb;


import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StoreAppendTest extends StoreTestCase {

    StoreAppend engine = (StoreAppend) super.engine;

    @Override
    protected Engine openEngine() {
        return new StoreAppend(index, false, false, false,false);
    }

    @Test
    public void compact_file_deleted(){
        File f = Utils.tempDbFile();
        StoreAppend engine = new StoreAppend(f, false,false,false,false);
        File f1 = engine.getFileNum(1);
        File f2 = engine.getFileNum(2);
        long recid = engine.put(111L, Serializer.LONG_SERIALIZER);
        Long i=0L;
        for(;i< StoreAppend.MAX_FILE_SIZE+1000; i+=8){
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

    @Test public void delete_files_after_close(){
        File f = Utils.tempDbFile();
        File f2 = new File(f.getPath()+".0");
        DB db = DBMaker.newAppendFileDB(f).deleteFilesAfterClose().make();

        db.getHashMap("test").put("aa","bb");
        db.commit();
        assertTrue(f2.exists());
        db.close();
        assertFalse(f2.exists());
    }

}
