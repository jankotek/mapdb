package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;


public class concurrency_executor_global {

    public static void main(String[] args) {
        //a
        DB db = DBMaker
                .memoryDB()
                //enable executors globally
                .executorEnable()
                .make();
        //z
    }
}
