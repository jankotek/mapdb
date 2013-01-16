package examples;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Utils;

import java.io.File;
import java.util.concurrent.ConcurrentNavigableMap;


/**
 *  Hello world application to demonstrate storage open, commit and close operations
 */
public class _HelloWorld {

    public static void main(String[] args){

        //Configure and open database using builder pattern.
        //All options are available with code auto-completion.
        File dbFile = Utils.tempDbFile();
        DB db = DBMaker.newFileDB(dbFile)
                .closeOnJvmShutdown()
                .encryptionEnable("password")
                .make();

        //open an collection, TreeMap has better performance then HashMap
        ConcurrentNavigableMap<Integer,String> map = db.getTreeMap("collectionName");

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
