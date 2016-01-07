package doc;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;


public class htreemap_byte_array {

    @Test
    public void run(){
        DB db = DBMaker.memoryDB().make();
        //a
        HTreeMap<byte[], Long> map = db.hashMap("map")
                .keySerializer(Serializer.BYTE_ARRAY)
                .valueSerializer(Serializer.LONG)
                .create();
        //z
    }
}
