package doc;

import org.mapdb.*;

public class btreemap_byte_array {

    public static void main(String[] args) {
        DB db = DBMaker.memoryDB().make();
        //a
        BTreeMap<byte[], Long> map = db.treeMapCreate("map")
                .keySerializer(Serializer.BYTE_ARRAY)
                .makeOrGet();
        //z
    }
}
