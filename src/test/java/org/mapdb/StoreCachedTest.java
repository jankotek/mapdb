package org.mapdb;


import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

@SuppressWarnings({"rawtypes","unchecked"})
public class StoreCachedTest<E extends StoreCached> extends StoreDirectTest<E>{

    @Override boolean canRollback(){return false;}

    File f = UtilsTest.tempDbFile();


    @Override protected E openEngine() {
        StoreCached e =new StoreCached(f.getPath());
        e.init();
        return (E)e;
    }

    @Test public void put_delete(){
        long recid = e.put(1L, Serializer.LONG);
        int pos = e.lockPos(recid);
        assertEquals(1, e.writeCache[pos].size);
        e.delete(recid,Serializer.LONG);
        assertEquals(1,e.writeCache[pos].size);
    }

    @Test public void put_update_delete(){
        long recid = e.put(1L, Serializer.LONG);
        int pos = e.lockPos(recid);
        assertEquals(1, e.writeCache[pos].size);
        e.update(2L, recid,Serializer.LONG);
        assertEquals(1,e.writeCache[pos].size);
        e.delete(recid,Serializer.LONG);
        assertEquals(1,e.writeCache[pos].size);
    }

}
