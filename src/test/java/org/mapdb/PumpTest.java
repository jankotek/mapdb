package org.mapdb;


import org.junit.Test;

import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class PumpTest {

    @Test
    public void copy(){
        DB db1 = new DB(new StoreHeap());
        Map m = db1.getHashMap("test");
        for(int i=0;i<1000;i++){
            m.put(i, "aa"+i);
        }

        DB db2 = DBMaker.newMemoryDB().make();
        Pump.copy(db1,db2);

        Map m2 = db2.getHashMap("test");
        for(int i=0;i<1000;i++){
            assertEquals("aa"+i, m.get(i));
        }

    }

    DB makeDB(int i){
        if(i==0) return DBMaker.newAppendFileDB(Utils.tempDbFile()).asyncWriteDisable().deleteFilesAfterClose().make();
        if(i==1) return DBMaker.newMemoryDB().asyncWriteDisable().make();
        if(i==2) return DBMaker.newMemoryDB().asyncWriteDisable().writeAheadLogDisable().make();
        return new DB(new StoreHeap());
    }
    final int dbmax = 4;


    @Test public void copy_all_stores_simple(){
        for(int srcc=0;srcc<dbmax;srcc++){
            for(int targetc=0;targetc<dbmax;targetc++) try{
                if(targetc==3) continue;

                DB src = makeDB(srcc);
                DB target = makeDB(targetc);

                long recid1 = src.engine.put("1", Serializer.STRING_SERIALIZER);
                long recid2 = src.engine.put("2", Serializer.STRING_SERIALIZER);

                Pump.copy(src,target);

                assertEquals("1", target.engine.get(recid1, Serializer.STRING_SERIALIZER));
                assertEquals("2", target.engine.get(recid2, Serializer.STRING_SERIALIZER));
                assertEquals("1", src.engine.get(recid1, Serializer.STRING_SERIALIZER));
                assertEquals("2", src.engine.get(recid2, Serializer.STRING_SERIALIZER));

                src.close();
                target.close();
            } catch(Throwable e){
                throw new RuntimeException("Failed with "+srcc+" - "+targetc,e);
            }
        }


    }

    @Test public void copy_all_stores(){
        for(int srcc=0;srcc<dbmax;srcc++){
            for(int targetc=0;targetc<dbmax;targetc++) try{
                if(targetc==3) continue;

                DB src = makeDB(srcc);
                DB target = makeDB(targetc);

                Map m = src.getHashMap("test");
                for(int i=0;i<1000;i++) m.put(i,"99090adas d"+i);
                src.commit();

                Pump.copy(src,target);

                assertEquals(src.getNameDir(), target.getNameDir());
                Map m2 = target.getHashMap("test");
                assertFalse(m2.isEmpty());
                assertEquals(m,m2);
                src.close();
                target.close();
            } catch(Throwable e){
                throw new RuntimeException("Failed with "+srcc+" - "+targetc,e);
            }
        }


    }

}
