package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TT;

import java.io.File;

public class Issue674Test {

    @Test public void crash(){
        File f = TT.tempDbFile();

        long time = TT.nowPlusMinutes(1);

        while(time>System.currentTimeMillis()) {
            DB db = DBMaker.fileDB(f)
                    .closeOnJvmShutdown()
                    .cacheSize(2048)
                    .checksumEnable()
                    .fileMmapEnable()
                    .make();

            BTreeMap map = db.treeMap("test");


            for(int i = 0; i<10000; i++){
                map.put(i,i);
            }
            db.commit();
            db.close();
        }
        f.delete();
    }

}
