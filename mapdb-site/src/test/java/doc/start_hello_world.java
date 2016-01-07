package doc;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.util.concurrent.ConcurrentMap;

/**
 * Hello world
 */
public class start_hello_world {

    @Test public void hello_world(){
        //a

        // import org.mapdb.*

        // DB is container which manages multiple collections.
        // This creates new off-heap DB, which serializes data into byte[]
        DB db = DBMaker
                .memoryDB()
                .make();

        // Creates new Map, it uses builder pattern to apply configuration.
        // Each collection has name, and some other settings, such as Serializers.
        // Serializer converts object into binary form, but also provides hash.
        ConcurrentMap<String, Long> map =
                db.hashMap("map")
                    .keySerializer(Serializer.STRING)
                    .valueSerializer(Serializer.LONG)
                    .valueInline()  // values are small, so store them inside Leaf Node
                    .create();      // create new Map from configuration

        map.put("something",111L);

        // Once finished close DB.
        // This will release resources (open files, background threads etc...)
        db.close();
        //z
    }
}
