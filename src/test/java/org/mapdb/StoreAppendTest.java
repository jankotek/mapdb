package org.mapdb;


import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import static org.junit.Assert.*;

public class StoreAppendTest<E extends StoreAppend> extends EngineTest<E>{


    File f = UtilsTest.tempDbFile();


    @Override
    protected E openEngine() {
        return (E) new StoreAppend(f);
    }

    @Test @Ignore
    public void compact_file_deleted(){
        File f = UtilsTest.tempDbFile();
        StoreAppend engine = new StoreAppend(f);
        File f1 = engine.getFileFromNum(0);
        File f2 = engine.getFileFromNum(1);
        long recid = engine.put(111L, Serializer.LONG);
        Long i=0L;
        for(;i< StoreAppend.FILE_MASK+1000; i+=8){
            engine.update(recid, i, Serializer.LONG);
        }
        i-=8;

        assertTrue(f1.exists());
        assertTrue(f2.exists());
        assertEquals(i, engine.get(recid, Serializer.LONG));

        engine.commit();
        assertTrue(f1.exists());
        assertTrue(f2.exists());
        assertEquals(i, engine.get(recid, Serializer.LONG));

        engine.compact();
        assertFalse(f1.exists());
        assertTrue(f2.exists());
        assertEquals(i, engine.get(recid, Serializer.LONG));

        f1.delete();
        f2.delete();

        engine.close();
    }

    @Test public void delete_files_after_close(){
        File f = UtilsTest.tempDbFile();
        File f2 = new File(f.getPath()+".0");
        DB db = DBMaker.newAppendFileDB(f).deleteFilesAfterClose().make();

        db.getHashMap("test").put("aa","bb");
        db.commit();
        assertTrue(f2.exists());
        db.close();
        assertFalse(f2.exists());
    }

    @Test public void header_created() throws IOException {
        //check offset
        assertEquals(StoreAppend.LAST_RESERVED_RECID, e.maxRecid);
        assertEquals(1+8+2*StoreAppend.LAST_RESERVED_RECID, e.currPos);
        RandomAccessFile raf = new RandomAccessFile(e.getFileFromNum(0),"r");
        //check header
        raf.seek(0);
        assertEquals(StoreAppend.HEADER, raf.readLong());
        //check reserved recids
        for(int recid=1;recid<=StoreAppend.LAST_RESERVED_RECID;recid++){
            assertEquals(0, e.index.getLong(recid*8));
            assertEquals(recid+StoreAppend.RECIDP,raf.read()); //packed long
            assertEquals(0+StoreAppend.SIZEP,raf.read()); //packed long
        }

        assertEquals(StoreAppend.END+StoreAppend.RECIDP,raf.read()); //packed long
        //check recid iteration
        assertFalse(e.getFreeRecids().hasNext());
    }

    @Test public void put(){
        long oldPos = e.currPos;
        Volume vol = e.currVolume;
        assertEquals(0, vol.getUnsignedByte(oldPos));

        long maxRecid = e.maxRecid;
        long value = 11111111111111L;
        long recid = e.put(value,Serializer.LONG);
        assertEquals(maxRecid+1, recid);
        assertEquals(e.maxRecid, recid);

        assertEquals(recid+StoreAppend.RECIDP, vol.getPackedLong(oldPos));
        assertEquals(8+StoreAppend.SIZEP, vol.getPackedLong(oldPos+1));
        assertEquals(value, vol.getLong(oldPos+2));

        assertEquals(Long.valueOf(oldPos+1), e.indexInTx.get(recid));
        e.commit();
        assertEquals(oldPos+1, e.index.getLong(recid*8));

    }

}
