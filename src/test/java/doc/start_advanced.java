package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;
import java.util.concurrent.ConcurrentNavigableMap;

public class start_advanced {
    public static void main(String[] args) {
        //a
        // import org.mapdb.*;

        // configure and open database using builder pattern.
        // all options are available with code auto-completion.
        DB db = DBMaker.fileDB(new File("testdb"))
                .closeOnJvmShutdown()
                .encryptionEnable("password")
                .make();

        // open existing an collection (or create new)
        ConcurrentNavigableMap<Integer,String> map = db.treeMap("collectionName");

        map.put(1, "one");
        map.put(2, "two");
        // map.keySet() is now [1,2]

        db.commit();  //persist changes into disk

        map.put(3, "three");
        // map.keySet() is now [1,2,3]
        db.rollback(); //revert recent changes
        // map.keySet() is now [1,2]

        db.close();
        //z
    }
}
