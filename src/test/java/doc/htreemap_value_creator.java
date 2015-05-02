package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.HTreeMap;


public class htreemap_value_creator {

    public static void main(String[] args) {
        DB db = DBMaker.memoryDB().make();
        //a
        HTreeMap<String,Long> map = db.hashMapCreate("map")
                .valueCreator(new Fun.Function1<Long, String>() {
                    @Override
                    public Long run(String o) {
                        return 1111L;
                    }
                })
                .makeOrGet();
        //z
    }
}
