package doc;

import org.mapdb.*;


public class btreemap_compressed {

    public static void main(String[] args) {
        DB db = DBMaker.memoryDB().make();
        //a
        BTreeMap<Long, String> map = db.treeMapCreate("map")
                .valuesOutsideNodesEnable()
                .valueSerializer(new Serializer.CompressionWrapper(Serializer.STRING))
                .makeOrGet();
        //z
    }
}
