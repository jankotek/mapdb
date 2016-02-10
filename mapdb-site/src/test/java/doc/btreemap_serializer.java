package doc;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

public class btreemap_serializer {

    public static void main(String[] args) {
        DB db = DBMaker.memoryDB().make();
        //a
        BTreeMap<Long, String> map = db.treeMap("map")
                .keySerializer(Serializer.LONG)
                .valueSerializer(Serializer.STRING)
                .createOrOpen();
        //z
    }
}
