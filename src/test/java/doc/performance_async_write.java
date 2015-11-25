package doc;

import org.mapdb20.DB;
import org.mapdb20.DBMaker;


public class performance_async_write {

    public static void main(String[] args) {
        //a
        DB db = DBMaker
            .memoryDB()
            .asyncWriteEnable()
            .asyncWriteQueueSize(10000) //optionally change queue size
            .executorEnable()   //enable background threads to flush data
            .make();
        //z
    }
}
