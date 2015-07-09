package doc;

import org.mapdb20.DB;
import org.mapdb20.DBMaker;

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
