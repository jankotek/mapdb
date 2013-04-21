package org.mapdb;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Data Pump moves data from one source to other.
 * It can be used to import data from text file, or copy store from memory to disk.
 */
public class Pump {

    /** copies all data from first DB to second DB */
    public static void copy(DB db1, DB db2){
        copy(storeForDB(db1), storeForDB(db2));
        db2.engine.clearCache();
        db2.reinit();
    }

    public static void copy(Store s1, Store s2){
        long maxRecid =s1.getMaxRecid();
        for(long recid=1;recid<=maxRecid;recid++){
            ByteBuffer bb = s1.getRaw(recid);
            //System.out.println(recid+" - "+(bb==null?0:bb.remaining()));
            s2.updateRaw(recid, bb);
        }

        //now release unused recids
        for(Iterator<Long> iter = s1.getFreeRecids(); iter.hasNext();){
            s2.delete(iter.next(), null);
        }
    }


    protected static Store storeForDB(DB db){
        Engine e = db.getEngine();
        while(e instanceof EngineWrapper) e = ((EngineWrapper) e).getWrappedEngine();
        return (Store) e;
    }


}
