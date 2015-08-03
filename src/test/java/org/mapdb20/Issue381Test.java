package org.mapdb20;

import org.junit.Test;

import java.io.File;
import java.util.concurrent.ConcurrentMap;

public class Issue381Test {


    @Test
    public void testCorruption()
            throws Exception
    {

        File f = TT.tempDbFile();

        for(int j=0;j<10;j++) {
            final int INSTANCES = 1000;
            TxMaker txMaker  = DBMaker.fileDB(f).makeTxMaker();
          
            DB tx = txMaker.makeTx();
            byte[] data = new byte[128];

            ConcurrentMap<Long, byte[]> map = tx.hashMap("persons");
            map.clear();
            for (int i = 0; i < INSTANCES; i++) {
                map.put((long) i, data);
            }
            tx.commit();
            txMaker.close();
        }

    }
}
