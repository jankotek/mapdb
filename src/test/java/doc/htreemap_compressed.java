package doc;

import org.mapdb10.DB;
import org.mapdb10.DBMaker;
import org.mapdb10.HTreeMap;
import org.mapdb10.Serializer;


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
