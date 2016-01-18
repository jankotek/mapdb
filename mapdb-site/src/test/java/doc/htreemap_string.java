package doc;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;


public class htreemap_string {

    @Test
    public void run(){
        DB db = DBMaker.memoryDB().make();
        //a
        //this will use strong XXHash for Strings
        HTreeMap<String, Long> map = db.hashMap("map")
                // by default it uses strong XXHash
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.LONG)
                .create();

        //this will use weak `String.hashCode()`
        HTreeMap<String, Long> map2 = db.hashMap("map2")
                // use weak String.hashCode()
                .keySerializer(Serializer.STRING_ORIGHASH)
                .valueSerializer(Serializer.LONG)
                .create();
        //z
    }
}
