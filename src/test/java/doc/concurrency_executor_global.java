package doc;

import org.mapdb10.DB;
import org.mapdb10.DBMaker;


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
