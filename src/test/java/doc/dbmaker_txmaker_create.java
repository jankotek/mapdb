package doc;

import org.mapdb20.DBMaker;
import org.mapdb20.TxMaker;


public class dbmaker_txmaker_create {

    public static void main(String[] args) {
        //a
        TxMaker txMaker = DBMaker
                .memoryDB()
                .makeTxMaker();
        //z
    }
}
