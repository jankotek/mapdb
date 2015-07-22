package org.mapdb;


import org.junit.Test;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.*;

@SuppressWarnings({"rawtypes","unchecked"})
public class
        StoreCachedTest<E extends StoreCached> extends StoreDirectTest<E>{

    @Override boolean canRollback(){return false;}

    File f = TT.tempDbFile();


    @Override protected E openEngine() {
        StoreCached e =new StoreCached(f.getPath());
        e.init();
        return (E)e;
    }

    @Test public void put_delete(){
        e = openEngine();
        long recid = e.put(1L, Serializer.LONG);
        int pos = e.lockPos(recid);
        assertEquals(1, e.writeCache[pos].size);
        e.delete(recid,Serializer.LONG);
        assertEquals(1, e.writeCache[pos].size);
    }

    @Test public void put_update_delete(){
        e = openEngine();
        long recid = e.put(1L, Serializer.LONG);
        int pos = e.lockPos(recid);
        assertEquals(1, e.writeCache[pos].size);
        e.update(2L, recid,Serializer.LONG);
        assertEquals(1,e.writeCache[pos].size);
        e.delete(recid,Serializer.LONG);
        assertEquals(1,e.writeCache[pos].size);
    }

    @Test(timeout = 100000)
    public void flush_write_cache(){
        if(TT.scale()==0)
            return;
        for(ScheduledExecutorService E:
                new ScheduledExecutorService[]{
                        null,
                        Executors.newSingleThreadScheduledExecutor()
                }) {
            final int M = 1234;
            StoreCached e = new StoreCached(
                    null,
                    Volume.ByteArrayVol.FACTORY,
                    null,
                    1,
                    0,
                    false,
                    false,
                    null,
                    false,
                    false,
                    false,
                    null,
                    0,
                    false,
                    0,
                    E,
                    1024,
                    M
            );
            e.init();

            assertEquals(M, e.writeQueueSize);
            assertEquals(0, e.writeCache[0].size);

            //write some stuff so cache is almost full
            for (int i = 0; i < M ; i++) {
                e.put("aa", Serializer.STRING);
            }

            assertEquals(M, e.writeCache[0].size);

            //one extra item causes overflow
            e.put("bb", Serializer.STRING);


            while(E!=null && e.writeCache[0].size>0){
                LockSupport.parkNanos(1000);
            }

            assertEquals(0, e.writeCache[0].size);

            if(E!=null)
                E.shutdown();

            e.close();
        }
    }

}
