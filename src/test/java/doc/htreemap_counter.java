package doc;

import org.mapdb20.DB;
import org.mapdb20.DBMaker;
import org.mapdb20.HTreeMap;


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
