package doc;

import org.mapdb10.BTreeMap;
import org.mapdb10.DB;
import org.mapdb10.DBMaker;
import org.mapdb10.Serializer;


public class btreemap_object_array {

    public static void main(String[] args) {
        DB db = DBMaker.memoryDB().make();
        //a
        BTreeMap<Object[], Long> map = db.treeMapCreate("map")
                // use array serializer for unknown objects
                .keySerializer(new Serializer.Array(db.getDefaultSerializer()))
                // or use serializer for specific objects such as String
                .keySerializer(new Serializer.Array(Serializer.STRING))
                .makeOrGet();
        //z
    }
}
