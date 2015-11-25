package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TT;

import java.io.File;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertEquals;


public class Issue237Test {

    File file = TT.tempDbFile();


    @Test
    public void testReopenAsync() throws InterruptedException {
        DB database = DBMaker.fileDB(file).asyncWriteEnable().make();
        testQueue( database );

        database = DBMaker.fileDB( file ).asyncWriteEnable().make();
        testQueue( database );
    }

    @Test
    public void testReopenSync() throws InterruptedException {
        file.delete();

        DB database = DBMaker.fileDB( file ).make();
        testQueue( database );

        database = DBMaker.fileDB( file ).make();
        testQueue( database );
    }

    private void testQueue( DB database ) throws InterruptedException {
        BlockingQueue<String> queue = database.getQueue( "test-queue" );
        queue.add( "test-value" );
        database.commit();
        assertEquals(queue.take(), "test-value");
        database.commit();
        database.close();
    }

}