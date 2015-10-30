package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;


public class concurrency_executor_custom {

    public static void main(String[] args) {
        //a
        DB db = DBMaker
                .memoryDB()
                //this would just enable global executor with default value
                // .executorEnable()
                //this will enable global executor supplied by user
                .executorEnable(
                        //TODO Executors.newSingleThreadScheduledExecutor()
                )
                .make();

        //remember that executor gets closed on shutdown
        db.close();
        //z
    }
}
