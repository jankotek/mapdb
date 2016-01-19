package doc;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import static org.junit.Assert.assertEquals;


public class htreemap_value_loader {

    @Test
    public void run(){
        DB db = DBMaker.memoryDB().make();
        //a
        HTreeMap<String,Long> map = db
                .hashMap("map", Serializer.STRING, Serializer.LONG)
                .valueLoader(s -> 1L)
                .create();

        //return 1, even if key does not exist
        Long one = map.get("Non Existent");

        // Value Creator output was added to Map
        map.size(); //  => 1
        //z

        assertEquals(1l, one.longValue());
        assertEquals(1, map.size());
    }
}
