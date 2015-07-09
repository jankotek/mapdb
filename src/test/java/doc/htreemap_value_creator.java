package doc;

import org.mapdb10.DB;
import org.mapdb10.DBMaker;
import org.mapdb10.Fun;
import org.mapdb10.HTreeMap;


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
