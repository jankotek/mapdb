package org.mapdb;

import org.junit.Ignore;
import org.junit.Test;
import java.io.File;

public class Issue90Test {

    @Test
    public void testCounter() throws Exception {
        File file = Utils.tempDbFile();


        final DB mapDb =DBMaker.newAppendFileDB(file)
                .closeOnJvmShutdown()
                .compressionEnable()  //This is the cause of the exception. If compression is not used, no exception occurs.

                .cacheDisable()
                .make();
        final Atomic.Long myCounter = Atomic.getLong(mapDb, "MyCounter");

        final BTreeMap<String, Fun.Tuple2<String, Integer>> treeMap = mapDb.getTreeMap("map");
        Bind.size(treeMap, myCounter);

        for (int i = 0; i < 3; i++) {
            treeMap.put("key_" + i, new Fun.Tuple2<String, Integer>("value_", i));
        }
    }



}