package doc;

import org.mapdb20.DBMaker;

import java.util.concurrent.ConcurrentNavigableMap;

public class start_hello_world {
    public static void main(String[] args) {
        //a
        // import org.mapdb20.*;
        ConcurrentNavigableMap treeMap = DBMaker.tempTreeMap();

        // and now use disk based Map as any other Map
        treeMap.put(111,"some value");
        //z
    }
}
