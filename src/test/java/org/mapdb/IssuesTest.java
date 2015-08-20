package org.mapdb;

import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class IssuesTest {

    @Test public void issue130(){
        DB db = DBMaker.appendFileDB(TT.tempDbFile())
                .closeOnJvmShutdown()
                .make();

        Map store = db.treeMap("collectionName");


    }


    @Test public void issue561(){
        final File file = TT.tempDbFile();
        final String queueName = "testqueue";
        DB db = DBMaker
                .fileDB(file)
                .fileMmapEnable()
                .transactionDisable()
                .cacheSize(128)
                .closeOnJvmShutdown()
                .make();
        BlockingQueue<String> queue = db.getQueue(queueName);
        String next = queue.poll();
        db.compact();
        db.commit();
        next = queue.poll();
    }
}
