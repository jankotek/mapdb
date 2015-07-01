package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;


public class htreemap_counter {

    public static void main(String[] args) {
        DB db = DBMaker.memoryDB().make();
        //a
        HTreeMap<String, Long> map = db.hashMapCreate("map")
                .counterEnable()
                .makeOrGet();
        //z
    }
}
