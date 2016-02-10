package doc;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

public class btreemap_byte_array {

    public static void main(String[] args) {
        DB db = DBMaker.memoryDB().make();
        //a
        BTreeMap<byte[], Long> map = db.treeMap("map")
                .keySerializer(Serializer.BYTE_ARRAY)
                .valueSerializer(Serializer.LONG)
                .createOrOpen();
        //z
    }
}
