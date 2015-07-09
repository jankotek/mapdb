package doc;

import org.mapdb10.DBMaker;
import org.mapdb10.TxMaker;


public class dbmaker_txmaker_create {

    public static void main(String[] args) {
        //a
        TxMaker txMaker = DBMaker
                .memoryDB()
                .makeTxMaker();
        //z
    }
}
