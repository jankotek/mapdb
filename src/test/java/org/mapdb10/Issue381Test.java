package org.mapdb10;

import org.junit.Test;

import java.io.File;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public class Issue381Test {


    @Test
    public void testCorruption()
            throws Exception
    {

        File f = UtilsTest.tempDbFile();

        for(int j=0;j<10;j++) {
            final int INSTANCES = 100000;
            DBMaker maker = DBMaker
                    .newFileDB(f);

            TxMaker txMaker = maker.makeTxMaker();
            DB tx = txMaker.makeTx();
            byte[] data = new byte[128];

            ConcurrentMap<Long, byte[]> map = tx.getHashMap("persons");
            map.clear();
            for (int i = 0; i < INSTANCES; i++) {
                map.put((long) i, data);
            }
            tx.commit();
            txMaker.close();
        }

    }

    static boolean r = false;

    @Test public void test2(){
        File f = UtilsTest.tempDbFile();

        DB db = DBMaker.newFileDB(f)
                .closeOnJvmShutdown()
                .make();

        Set<String> set = db.getHashSet("SET");

        long i;
        for (i = 0; i < 100000; i++)
            set.add("Test" + i);

        db.commit();
        r=true;
        set.clear();
        db.commit();
        db.close();
    }
}
