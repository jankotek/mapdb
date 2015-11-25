package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TT;
import org.mapdb.TxMaker;

import java.io.File;
import java.util.concurrent.ConcurrentMap;

public class Issue381Test {


    @Test
    public void testCorruption()
            throws Exception
    {

        File f = TT.tempDbFile();
        int max = 10+TT.scale()*1000;

        for(int j=0;j<max;j++) {
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
