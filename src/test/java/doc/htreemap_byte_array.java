package doc;

import org.mapdb20.DB;
import org.mapdb20.DBMaker;
import org.mapdb20.HTreeMap;
import org.mapdb20.Serializer;


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
