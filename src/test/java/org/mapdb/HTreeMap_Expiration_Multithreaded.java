package org.mapdb;


import org.junit.Test;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class HTreeMap_Expiration_Multithreaded {

    final long duration = 10 * 60 * 1000;

    static byte[] b = new byte[100];

    @Test public void expireUUID(){
        if(TT.shortTest())
            return;

        final long endTime = duration+System.currentTimeMillis();

        DB db = DBMaker.memoryDB().cacheSize(10000).make();
        final Map m = db.hashMapCreate("aa")
                .keySerializer(Serializer.UUID)
                .valueSerializer(Serializer.BYTE_ARRAY)
                .expireAfterWrite(1, TimeUnit.MINUTES)
                .expireTick(0)
                .make();

        Exec.execNTimes(10, new Callable() {
            @Override
            public Object call() throws Exception {

                    Random r = new Random(1);
                    for (int i = 0; i < 2e5; i++) {
                        UUID u = new UUID(r.nextLong(), r.nextLong());
                        m.put(u, b);
                    }

                    while (System.currentTimeMillis()<endTime) {
                        r = new Random(1);
                        for (int i = 0; i < 1e5; i++) {
                            UUID u = new UUID(r.nextLong(), r.nextLong());
                            m.get(u);
                            m.put(u,b);
                        }
                        for (int i = (int) 1e5; i < 2e5; i++) {
                            UUID u = new UUID(r.nextLong(), r.nextLong());
                            m.get(u);
                        }
                    }
                    return null;
            }
        });
    }
}
