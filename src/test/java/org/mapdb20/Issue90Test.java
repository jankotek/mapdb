package org.mapdb20;

import org.junit.Test;

import java.io.File;

public class Issue90Test {

    @Test
    public void testCounter() throws Exception {
        File file = UtilsTest.tempDbFile();


        final DB mapDb =DBMaker.appendFileDB(file)
                .closeOnJvmShutdown()
                .compressionEnable()  //This is the cause of the exception. If compression is not used, no exception occurs.
                .make();
        final Atomic.Long myCounter = mapDb.atomicLong("MyCounter");

        final BTreeMap<String, Fun.Pair<String, Integer>> treeMap = mapDb.treeMap("map");
        Bind.size(treeMap, myCounter);

        for (int i = 0; i < 3; i++) {
            treeMap.put("key_" + i, new Fun.Pair<String, Integer>("value_", i));
        }
    }



}