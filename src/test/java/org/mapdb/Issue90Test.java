package org.mapdb;

import org.junit.Ignore;
import org.junit.Test;
import java.io.File;

public class Issue90Test {

    @Test @Ignore //for now
    public void testCounter() throws Exception {
        final DB mapDb = createTempMapDb();
        final Atomic.Long myCounter = Atomic.getLong(mapDb, "MyCounter");

        final BTreeMap<String, Fun.Tuple2<String, Integer>> treeMap = mapDb.getTreeMap("map");
        Bind.size(treeMap, myCounter);

        for (int i = 0; i < 3; i++) {
            treeMap.put("key_" + i, new Fun.Tuple2<String, Integer>("value_", i));
        }
    }


    private DB createTempMapDb() throws Exception {
        final File wordDataFile = Utils.tempDbFile();
        return createMapDB(wordDataFile);
    }

    private DB createMapDB(File file) {
        return DBMaker.newAppendFileDB(file)
                .closeOnJvmShutdown()
                .compressionEnable()  //This is the cause of the exception. If compression is not used, no exception occurs.

                .cacheDisable()
                .make();
    }

}