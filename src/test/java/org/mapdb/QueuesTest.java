package org.mapdb;


import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class QueuesTest {



    @Test public void stack_persisted(){
        File f = UtilsTest.tempDbFile();
        DB db = DBMaker.newFileDB(f).transactionDisable().cacheDisable().make();
        Queue<Object> stack = db.getStack("test");
        stack.add("1");
        stack.add("2");
        stack.add("3");
        stack.add("4");

        db.close();
        db = DBMaker.newFileDB(f).transactionDisable().cacheDisable().deleteFilesAfterClose().make();
        stack = db.getStack("test");

        assertEquals("4",stack.poll());
        assertEquals("3",stack.poll());
        assertEquals("2",stack.poll());
        assertEquals("1",stack.poll());
        assertNull(stack.poll());
        db.close();
    }


    @Test public void queue_persisted(){
        File f = UtilsTest.tempDbFile();
        DB db = DBMaker.newFileDB(f).transactionDisable().cacheDisable().make();
        Queue<Object> queue = db.getQueue("test");
        queue.add("1");
        queue.add("2");
        queue.add("3");
        queue.add("4");

        db.close();
        db = DBMaker.newFileDB(f).transactionDisable().cacheDisable().deleteFilesAfterClose().make();
        queue = db.getQueue("test");

        assertEquals("1", queue.poll());
        assertEquals("2", queue.poll());
        assertEquals("3", queue.poll());
        assertEquals("4", queue.poll());
        assertNull(queue.poll());
        db.close();
    }

    @Test public void circular_queue_persisted(){
        //i put disk limit 4 objects ,
        File f = UtilsTest.tempDbFile();
        DB db = DBMaker.newFileDB(f).transactionDisable().cacheDisable().make();
        Queue queue = db.createCircularQueue("test",null, 4);
        //when i put 6 objects to queue
        queue.add(0);
        queue.add(1);
        queue.add(2);
        queue.add(3);
        //now deletes 0 on first
        queue.add(4);
        //now deletes 1
        queue.add(5);

        db.close();
        db = DBMaker.newFileDB(f).transactionDisable().cacheDisable().deleteFilesAfterClose().make();
        queue = db.getCircularQueue("test");

        assertEquals(2, queue.poll());
        assertEquals(3, queue.poll());
        assertEquals(4, queue.poll());
        assertEquals(5, queue.poll());
        assertNull(queue.poll());
        db.close();

    }

    @Test
    public void testMapDb() throws InterruptedException {
        DB database = DBMaker.newMemoryDB().make();
        BlockingQueue<String> queue = database.getQueue( "test-queue" );
        queue.put( "test-value" );
        database.commit();
        assertThat( queue.take(), is( "test-value" ) );
        database.commit();
        database.close();
    }

    @Test(timeout=1000)
    public void queueTakeRollback() throws IOException, InterruptedException {
        File f = File.createTempFile("mapdb","aa");
        {
            DB db = DBMaker.newFileDB(f).make();
            boolean newQueue = !db.exists("test");
            BlockingQueue queue = db.getQueue("test");
            if (newQueue) {
                queue.add("abc");
                db.commit();
            }
            Object x = queue.take();
            db.rollback();
            x = queue.take();

            System.out.println("got it");
            db.close();
        }

        {
            DB db = DBMaker.newFileDB(f).make();
            boolean newQueue = !db.exists("test");
            BlockingQueue queue = db.getQueue("test");
            if (newQueue) {
                queue.add("abc");
                db.commit();
            }
            Object x = queue.take();
            db.rollback();
            x = queue.take();

            System.out.println("got it");
            db.commit();
            db.close();
        }
    }
}