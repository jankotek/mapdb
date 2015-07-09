package doc;

import org.mapdb10.DB;
import org.mapdb10.DBMaker;


public class concurrency_executor_async_write {

    public static void main(String[] args) {
        //a
        DB db = DBMaker.memoryDB()
                //TODO specific executor for async write


                .make();
        //z
    }
}
