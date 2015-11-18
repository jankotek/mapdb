package doc;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;


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
