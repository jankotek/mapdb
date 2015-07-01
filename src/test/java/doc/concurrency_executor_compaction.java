package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.util.concurrent.Executors;


public class concurrency_executor_compaction {

    public static void main(String[] args) {
        //a
        DB db = DBMaker
                .memoryDB()

                //enable executor used for compaction
                .storeExecutorEnable()
                //or use your own executor
                .storeExecutorEnable(
                        Executors.newSingleThreadScheduledExecutor()
                )
                .make();
        //perform compaction
        db.compact();

        //z
    }
}
