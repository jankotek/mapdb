package org.mapdb;

import org.junit.Test;

import java.io.File;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StoreArchiveTest {

    @Test
    public void pump(){
        File f = TT.tempDbFile();
        StoreArchive e = new StoreArchive(
                f.getPath(),
                Volume.RandomAccessFileVol.FACTORY,
                false);
        e.init();

        List a = new ArrayList();
        for(int i=0;i<10000;i++){
            a.add(i);
        }
        Collections.reverse(a);

        long recid = Pump.buildTreeMap(
                a.iterator(),
                e,
                Fun.extractNoTransform(),
                Fun.extractNoTransform(),
                false,
                32,
                false,
                0,
                BTreeKeySerializer.INTEGER,
                (Serializer)Serializer.INTEGER,
                null
        );



        e.commit();

        assertTrue(recid>0);
        e.close();
        f.delete();
    }

    @Test public void update_same_size(){
        if(TT.shortTest())
            return;

        StoreArchive e = new StoreArchive(
                null,
                Volume.ByteArrayVol.FACTORY,
                false);
        e.init();
        assertTrue(!e.readonly);

        long max = 100000;
        List<Long> recids = new ArrayList<Long>();
        for(long i=0;i<max;i++){
            recids.add(e.put(i,Serializer.LONG));
        }

        for(long i=max;i<max*100;i++){
            long recid = recids.get((int) (i%max));
            assertTrue(i-max == e.get(recid,Serializer.LONG));
            e.update(recid, i, Serializer.LONG);
        }

    }

    @Test public void reserved_recid_update(){
        StoreArchive e = new StoreArchive(
                null,
                Volume.ByteArrayVol.FACTORY,
                false);
        e.init();

        for(long recid=1; recid<Engine.RECID_LAST_RESERVED;recid++){
            assertEquals(null, e.get(recid, Serializer.BYTE_ARRAY_NOSIZE));
            byte[] b = TT.randomByteArray(1000);
            e.update(recid,b,Serializer.BYTE_ARRAY_NOSIZE);
            assertTrue(Arrays.equals(b, e.get(recid, Serializer.BYTE_ARRAY_NOSIZE)));
            e.update(recid, null, Serializer.BYTE_ARRAY_NOSIZE);
            assertEquals(null, e.get(recid, Serializer.BYTE_ARRAY_NOSIZE));
        }
    }

    @Test public void reserved_recid_update_reopen(){
        File f = TT.tempDbFile();
        StoreArchive e = new StoreArchive(
                f.getPath(),
                Volume.RandomAccessFileVol.FACTORY,
                false);
        e.init();

        byte[] b = TT.randomByteArray(10000);
        e.update(Engine.RECID_NAME_CATALOG, b, Serializer.BYTE_ARRAY_NOSIZE);
        e.close();

        e = new StoreArchive(
                f.getPath(),
                Volume.RandomAccessFileVol.FACTORY,
                false);
        e.init();

        assertTrue(Arrays.equals(b, e.get(Engine.RECID_NAME_CATALOG, Serializer.BYTE_ARRAY_NOSIZE)));

        e.close();
        f.delete();
    }

    @Test public void large_record(){
        if(TT.shortTest())
            return;

        StoreArchive e = new StoreArchive(
                null,
                Volume.ByteArrayVol.FACTORY,
                false);
        e.init();
        assertTrue(!e.readonly);

        byte[] b = TT.randomByteArray((int) 1e8);
        long recid = e.put(b,Serializer.BYTE_ARRAY_NOSIZE);
        assertTrue(Arrays.equals(b, e.get(recid,Serializer.BYTE_ARRAY_NOSIZE)));
    }

    @Test public void pump_copy_named_btree(){
        File f = TT.tempDbFile();
        NavigableMap source = new TreeMap();
        for(int i=0;i<10000;i++){
            source.put(i,""+i);
        }

        Pump.archiveTreeMap(
                source,
                f,
                new DB.BTreeMapMaker("name")
                    .keySerializer(Serializer.INTEGER)
                    .valueSerializer(Serializer.STRING)
        );

        DB db = DBMaker.archiveFileDB(f).make();
        Map m = db.treeMap("name");

        assertTrue(source.entrySet().containsAll(m.entrySet()));
        assertTrue(m.entrySet().containsAll(source.entrySet()));
        db.close();
        f.delete();
    }
}