package net.kotek.jdbm.examples;

import net.kotek.jdbm.ConcurrentSortedMap;
import net.kotek.jdbm.DB;
import net.kotek.jdbm.DBMaker;

import java.io.File;


public class E01_BasicExample {

    public static void main(String[] args){

        //Configure and open database using builder pattern.
        //All options are available with code auto-completion.
        DB db = DBMaker.newFileDB(new File("testdb"))
                .closeOnJvmShutdown()
                .encryptionEnable("password")
                .make();

        //open an collection, TreeMap has better performance then HashMap
        ConcurrentSortedMap<Integer,String> map = db.getTreeMap("collectionName");

        map.put(1,"one");
        map.put(2,"two");
        //map.keySet() is now [1,2] even before commit

        db.commit();  //persist changes into disk

        map.put(3,"three");
        //map.keySet() is now [1,2,3]
        db.rollback(); //revert recent changes
        //map.keySet() is now [1,2]

        db.close();

    }
}
