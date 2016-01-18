package doc;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;


public class htreemap_noserializer {

    @Test public void run() {
        DB db = DBMaker.memoryDB().make();
        //a
        HTreeMap<String, Long> map =
                (HTreeMap<String,Long>)db
                .hashMap("name_of_map")
                .create();
        //z
    }
}
