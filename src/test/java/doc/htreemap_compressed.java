package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;


public class htreemap_compressed {

    public static void main(String[] args) {
        DB db = DBMaker.memoryDB().make();
        //a
        HTreeMap<Long, String> map = db.hashMapCreate("map")
                .valueSerializer(new Serializer.CompressionWrapper(Serializer.STRING))
                .makeOrGet();
        //z
        //TODO add Serializer.compressed() method?
    }
}
