package org.mapdb;


import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class Issue258Test {


    @Test
    public void test() throws IOException {

        File tmp = File.createTempFile("mapdb","");


        for(int i=0;i<10;i++){
        DB db = DBMaker.newFileDB(tmp)
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

        for (int k=0; k < 100000; k++)
        {

            String cmd = "iasdkaokdas"+i;
            map.add(cmd);
        }

        db.commit();
        db.close();
        }

    }
}
