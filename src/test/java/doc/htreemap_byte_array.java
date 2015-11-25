package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;


public class htreemap_byte_array {

    public static void main(String[] args) {
        DB db = DBMaker.memoryDB().make();
        //a
        HTreeMap<byte[], Long> map = db.hashMapCreate("map")
                .keySerializer(Serializer.BYTE_ARRAY)
                .makeOrGet();
        //z
    }
}
