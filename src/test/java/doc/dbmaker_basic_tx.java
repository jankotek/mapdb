package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.util.concurrent.ConcurrentNavigableMap;


public class dbmaker_basic_tx {

    public static void main(String[] args) {
        DB db = DBMaker
                .memoryDB()
                .make();
        //a
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

        //z
    }
}
