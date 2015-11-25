package doc;

import org.mapdb.*;

public class btreemap_serializer {

    public static void main(String[] args) {
        DB db = DBMaker.memoryDB().make();
        //a
        BTreeMap<Long, String> map = db.treeMapCreate("map")
                .keySerializer(Serializer.LONG)
                .valueSerializer(Serializer.STRING)
                .makeOrGet();
        //z
    }
}
