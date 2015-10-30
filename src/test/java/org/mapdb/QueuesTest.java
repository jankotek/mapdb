package org.mapdb;


import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

@SuppressWarnings({"rawtypes","unchecked"})
public class QueuesTest {



    @Test public void stack_persisted(){
        File f = TT.tempDbFile();
        DB db = DBMaker.fileDB(f).transactionDisable().make();
        Queue<Object> stack = db.getStack("test");
        stack.add("1");
        stack.add("2");
        stack.add("3");
        stack.add("4");

        db.close();
        db = DBMaker.fileDB(f).transactionDisable().deleteFilesAfterClose().make();
        stack = db.getStack("test");

        assertEquals("4",stack.poll());
        assertEquals("3",stack.poll());
        assertEquals("2",stack.poll());
        assertEquals("1",stack.poll());
        assertNull(stack.poll());
        db.close();
    }


    @Test public void queue_persisted(){
        File f = TT.tempDbFile();
        DB db = DBMaker.fileDB(f).transactionDisable().make();
        Queue<Object> queue = db.getQueue("test");
        queue.add("1");
        queue.add("2");
        queue.add("3");
        queue.add("4");

        db.close();
        db = DBMaker.fileDB(f).transactionDisable().deleteFilesAfterClose().make();
        queue = db.getQueue("test");

        assertEquals("1", queue.poll());
        assertEquals("2", queue.poll());
        assertEquals("3", queue.poll());
        assertEquals("4", queue.poll());
        assertNull(queue.poll());
        db.close();
    }

    @Test
    public void circular_queue_persisted_Not_Full(){
        //i put disk limit 4 objects ,
        File f = TT.tempDbFile();
        DB db = DBMaker.fileDB(f).transactionDisable().make();
        Queue queue = db.createCircularQueue("test", null, 4);
        //when i put 6 objects to queue
        queue.add(0);
        queue.add(1);
        queue.add(2);

        db.close();
        db = DBMaker.fileDB(f).transactionDisable().deleteFilesAfterClose().make();
        queue = db.getCircularQueue("test");

        assertEquals(0, queue.poll());
        assertEquals(1, queue.poll());
        assertEquals(2, queue.poll());
        assertNull(queue.poll());
        db.close();

    }

    @Test
    public void circular_queue_persisted(){
        //i put disk limit 4 objects ,
        File f = TT.tempDbFile();
        DB db = DBMaker.fileDB(f).transactionDisable().make();
        Queue queue = db.createCircularQueue("test",null, 3);
        //when i put 6 objects to queue
        queue.add(0);
        queue.add(1);
        queue.add(2);
        //now deletes 0 on first
        queue.add(3);
        //now deletes 1
        queue.add(4);

        db.close();
        db = DBMaker.fileDB(f).transactionDisable().deleteFilesAfterClose().make();
        queue = db.getCircularQueue("test");

        assertEquals(2, queue.poll());
        assertEquals(3, queue.poll());
        assertEquals(4, queue.poll());
        assertNull(queue.poll());

        //Now queue is empty.
        //Then try to add and poll 3 times to check every position
        for(int i = 0; i < 3; i++) {
            queue.add(5);
            assertEquals(5, queue.poll());
        }

        // Now queue should be empty.
        assertTrue(queue.isEmpty());

        db.close();

    }

    @Test
    public void testMapDb() throws InterruptedException {
        DB database = DBMaker.memoryDB().make();
        BlockingQueue<String> queue = database.getQueue( "test-queue" );
        queue.put( "test-value" );
        database.commit();
        assertThat( queue.take(), is( "test-value" ) );
        database.commit();
        database.close();
    }

    @Test(timeout=100000)
    public void queueTakeRollback() throws IOException, InterruptedException {
        File f = File.createTempFile("mapdbTest","aa");
        {
            DB db = DBMaker.fileDB(f).make();
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
            DB db = DBMaker.fileDB(f).make();
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