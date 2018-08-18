package org.mapdb.examples;

import org.junit.Test;
import org.mapdb.db.DB;
import org.mapdb.db.QueueMaker;

import java.util.Queue;

public class DBExample {

    @Test
    public void dbQueue(){
        DB db = DB.newOnHeapDB().make();

        Queue<String> q = QueueMaker
                .newLinkedFifoQueue(db, "name", String.class)
                .make();


        db.close();
    }


    @Test public void db_maker_fun(){
        DB db = DB
                .newOnHeapDB()
                .txBlock()
                .make();

        Queue<String> q = QueueMaker
                .newLinkedFifoQueue(db, "name", String.class)
                .make();

        // because TX are enabled, the Q is just a proxy, can be only accessed within transaction?
        //this fails
        // q.add("ss");

        db.tx(()->{
            // Q and other collections can be accessed from here
            // this block is repeated multiple times, until there is no conflict and it succusseds
            q.add("something");
        });


        /*
        Problems:
        - bytecode proxy generator
        - iterators and other embedded classes returned by other functions
        - poisoning, access should throw an exception after it expires
        - mixing the same class within multiple tx?
        - what if function is executed multiple times?
         */



        int size = db.tx(()->{
            // Q and other collections can be accessed from here
            // this block is repeated multiple times, until there is no conflict and it succusseds
            return q.size();
        });
    }
}
