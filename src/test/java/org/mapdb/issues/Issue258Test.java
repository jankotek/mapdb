package org.mapdb.issues;


import org.junit.Test;
import org.mapdb.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertEquals;

public class Issue258Test {

    int max = TT.scale()*100000;

    @Test
    public void test() throws IOException {

        File tmp = File.createTempFile("mapdbTest","");


        for(int i=0;i<10;i++){
        DB db = DBMaker.fileDB(tmp)
                .mmapFileEnable()
//                .closeOnJvmShutdown()
//                .compressionEnable()
//                .cacheLRUEnable()
//                .asyncWriteEnable()
                .make();

        BlockingQueue<Object> map = db.getStack("undolog");

        for(int j=0; !map.isEmpty() && j < 100; j++)
        {
            Object obj = map.poll();

        }
        map.clear();

        for (int k=0; k < max; k++)
        {

            String cmd = "iasdkaokdas"+i;
            map.add(cmd);
        }

        db.commit();
        db.close();
        }

    }


    @Test
    public void testWithChecksum() throws IOException {

        File tmp = File.createTempFile("mapdbTest","");


        for(int i=0;i<10;i++){
            DB db = DBMaker.fileDB(tmp)
                    .mmapFileEnable()
                    .checksumEnable()
//                .closeOnJvmShutdown()
//                .compressionEnable()
//                .cacheLRUEnable()
//                .asyncWriteEnable()
                    .make();

            BlockingQueue<Object> map = db.getStack("undolog");

            for(int j=0; !map.isEmpty() && j < 100; j++)
            {
                Object obj = map.poll();

            }
            map.clear();

            for (int k=0; k < max; k++)
            {

                String cmd = "iasdkaokdas"+i;
                map.add(cmd);
            }

            db.commit();
            db.close();
        }

    }



    @Test
    public void testWithChecksumEmpty() throws IOException {

        File tmp = File.createTempFile("mapdbTest","");


        for(int i=0;i<10;i++){
            DB db = DBMaker.fileDB(tmp)
                    .mmapFileEnable()
                    .checksumEnable()
                    .make();
            db.close();
        }

    }

    @Test public void many_recids_reopen_with_checksum() throws IOException {
        File tmp = File.createTempFile("mapdbTest","");

        Engine e = DBMaker.fileDB(tmp)
                .transactionDisable()
                .checksumEnable()
                .makeEngine();

        Map<Long,Integer> m = new HashMap();
        for(int i=0;i<max;i++){
            long recid = e.put(i, Serializer.INTEGER);
            m.put(recid,i);
        }

        e.commit();
        e.close();

        e = DBMaker.fileDB(tmp)
                .transactionDisable()
                .checksumEnable()
                .makeEngine();

        for(Long recid:m.keySet()){
            assertEquals(m.get(recid), e.get(recid,Serializer.INTEGER));
        }
        e.close();
    }

}
