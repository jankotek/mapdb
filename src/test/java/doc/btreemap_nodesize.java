package doc;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;

public class btreemap_nodesize {

    public static void main(String[] args) {
        DB db = DBMaker.memoryDB().make();
        //a
        BTreeMap<Long, String> map = db.treeMapCreate("map")
                .nodeSize(64)
                .makeOrGet();
        //z
    }
}
