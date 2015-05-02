package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TxMaker;

import java.util.Map;


public class dbmaker_txmaker_basic {

    public static void main(String[] args) {
        TxMaker txMaker = DBMaker
                .memoryDB()
                .makeTxMaker();
        //a
        DB tx0 = txMaker.makeTx();
        Map map0 = tx0.treeMap("testMap");
        map0.put(0,"zero");

        DB tx1 = txMaker.makeTx();
        Map map1 = tx1.treeMap("testMap");

        DB tx2 = txMaker.makeTx();
        Map map2 = tx1.treeMap("testMap");

        map1.put(1,"one");
        map2.put(2,"two");

        //each map sees only its modifications,
        //map1.keySet() contains [0,1]
        //map2.keySet() contains [0,2]

        //persist changes
        tx1.commit();
        tx2.commit();
        // second commit fails  with write conflict, both maps share single BTree node,
        // this does not happen on large maps with sufficient number of BTree nodes.
        //z
    }
}
