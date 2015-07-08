package org.mapdb10;


import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.UUID;

public class Issues364Test {


    @Test
    public void crash(){

        DB db = DBMaker.newMemoryDB().
                asyncWriteEnable()
                .closeOnJvmShutdown()
                .make();
        Map map = db.getHashMap("test");
        Runtime run = Runtime.getRuntime();
        String value = null;
        for (int i = 0; i < 1000000; i++) {
            String key = UUID.randomUUID().toString();
            if (value == null || i % 2 == 0) {
                value = UUID.randomUUID().toString();
            }
            map.put(key, value);
            if (i % 50000 == 0) {
                double free = run.freeMemory() / 1024.0 / 1024.0;
                double total = run.totalMemory() / 1024.0 / 1024.0;
                System.out.format("read %10d free = %10.2f total=%10.2f %n", i, free, total);
                db.commit();
            }
        }
        db.commit();
        db.close();
    }
}
