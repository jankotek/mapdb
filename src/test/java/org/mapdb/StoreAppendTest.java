package org.mapdb;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
@SuppressWarnings({"rawtypes","unchecked"})
public class StoreAppendTest<E extends StoreAppend> extends EngineTest<E>{


    public static class WithChecksums extends StoreAppendTest{
        @Override
        protected StoreAppend openEngine() {
            StoreAppend s =  new StoreAppend(f.getPath(),
                    Volume.RandomAccessFileVol.FACTORY,
                    null,
                    16,
                    0,
                    true,
                    false,
                    null,
                    false,
                    false,
                    false,
                    null,
                    false,
                    null,
                    0L,
                    0L
            );
            s.init();
            return  s;
        }

    }

    File f = TT.tempDbFile();


    @After
    public void deleteFile(){
        if(e!=null && !e.isClosed()){
            e.close();
            e = null;
        }
        if(f==null)
            return;

        f.delete();
        String name = f.getName();
        for(File f2:f.getParentFile().listFiles()){
            if(f2.getName().startsWith(name))
                f2.delete();
        }
    }

    @Override
    protected E openEngine() {
        StoreAppend s =  new StoreAppend(f.getPath());
        s.init();
        return (E) s;
    }

    /*
    @Test
    public void compact_file_deleted(){
        StoreAppend engine = new StoreAppend(f.getPath());
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
        assertEquals(StoreAppend.RECID_LAST_RESERVED, e.maxRecid);
        assertEquals(1+8+2*StoreAppend.RECID_LAST_RESERVED, e.currPos);
        RandomAccessFile raf = new RandomAccessFile(e.getFileFromNum(0),"r");
        //check header
        raf.seek(0);
        assertEquals(StoreAppend.HEADER, raf.readLong());
        //check reserved recids
        for(int recid=1;recid<=StoreAppend.RECID_LAST_RESERVED;recid++){
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


    @Override public void large_record_larger(){
        //TODO ignored test
    }
    */

    @Test public void header(){
        StoreAppend s = openEngine();
        assertEquals(WriteAheadLog.WAL_HEADER,s.wal.curVol.getInt(0));
        assertEquals(StoreAppend.HEADER, new Volume.RandomAccessFileVol(f,false,true,0).getInt(0));
    }

    @Override
    public void commit_huge() {
        //TODO this test is ignored, causes OOEM
    }

    @Test public void patch_on_broken(){
        e = openEngine();
        List<Long> recids = new ArrayList<Long>();
        for(int i=0;i<100;i++){
            long recid = e.put(TT.randomByteArray(10,i),Serializer.BYTE_ARRAY_NOSIZE);
            recids.add(recid);
        }
        e.commit();

        for(int loop=0;loop<100;loop++) {
            reopen();
            for (int i = 0; i < recids.size(); i++) {
                e.update(recids.get(i), TT.randomByteArray(20, i+loop), Serializer.BYTE_ARRAY_NOSIZE);
            }
            e.commit();
            long initOffset = e.wal.fileOffset;
            for (int i = 0; i < recids.size(); i++) {
                e.update(recids.get(i), TT.randomByteArray(30, i+loop), Serializer.BYTE_ARRAY_NOSIZE);
            }
            long preCommitOffset = e.wal.fileOffset;
            File file = e.wal.curVol.getFile();
            e.commit();
            e.close();

            //corrupt last file, destroy commit
            Volume vol = Volume.RandomAccessFileVol.FACTORY.makeVolume(file.getPath(), false);
            vol.clear(preCommitOffset, vol.length());
            vol.sync();
            vol.close();

            e = openEngine();
            assertEquals(initOffset, e.wal.fileOffset);
            for (int i = 0; i < recids.size(); i++) {
                byte[] b = e.get(recids.get(i), Serializer.BYTE_ARRAY_NOSIZE);
                assertEquals(20, b.length);
                assertArrayEquals(TT.randomByteArray(20, i+loop), b);
            }

            for (int i = 0; i < recids.size(); i++) {
                e.update(recids.get(i), TT.randomByteArray(40, i+loop), Serializer.BYTE_ARRAY_NOSIZE);
            }
            e.commit();
            for (int i = 0; i < recids.size(); i++) {
                e.update(recids.get(i), TT.randomByteArray(41, i+loop), Serializer.BYTE_ARRAY_NOSIZE);
            }
            e.commit();
            reopen();

            for (int i = 0; i < recids.size(); i++) {
                byte[] b = e.get(recids.get(i), Serializer.BYTE_ARRAY_NOSIZE);
                assertEquals(41, b.length);
                assertArrayEquals(TT.randomByteArray(41, i+loop), b);
            }
        }

    }

}
