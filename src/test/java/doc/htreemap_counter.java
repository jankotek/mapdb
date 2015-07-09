package doc;

import org.mapdb10.DB;
import org.mapdb10.DBMaker;
import org.mapdb10.HTreeMap;


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
