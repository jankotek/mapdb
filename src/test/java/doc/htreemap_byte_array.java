package doc;

import org.mapdb10.DB;
import org.mapdb10.DBMaker;
import org.mapdb10.HTreeMap;
import org.mapdb10.Serializer;


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
