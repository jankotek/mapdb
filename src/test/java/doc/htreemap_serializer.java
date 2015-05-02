package doc;

import org.mapdb.*;


public class htreemap_serializer {

    public static void main(String[] args) {
        DB db = DBMaker.memoryDB().make();
        //a
        HTreeMap<String, Long> map = db.hashMapCreate("map")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.LONG)
                .makeOrGet();
        //z
    }
}
