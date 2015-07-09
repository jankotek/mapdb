package doc;

import org.mapdb20.BTreeMap;
import org.mapdb20.DB;
import org.mapdb20.DBMaker;


public class btreemap_counter {

    public static void main(String[] args) {
        DB db = DBMaker.memoryDB().make();
        //a
        BTreeMap<Long, String> map = db.treeMapCreate("map")
                .counterEnable()
                .makeOrGet();
        //z
    }
}
