package doc;

import org.junit.Test;
import org.mapdb.*;


public class htreemap_serializer {

    @Test public void run() {
        DB db = DBMaker.memoryDB().make();
        //a
        HTreeMap<String, Long> map = db.hashMap("name_of_map")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.LONG)
                .create();

        //or shorter form
        HTreeMap<String, Long> map2 = db
                .hashMap("some_other_map", Serializer.STRING, Serializer.LONG)
                .create();
        //z
    }
}
