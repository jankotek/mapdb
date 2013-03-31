package examples;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Utils;

import java.io.File;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Jan Kotek
 */
public class Huge_Insert {

    public static void main(String[] args){
        DB db = DBMaker
                //.newFileDB(new File("/mnt/big/db/aa"))
                .newFileDB(new File("/mnt/big/db/aa" + System.currentTimeMillis()))
                .writeAheadLogDisable()
                .make();

        Map map = db
                .getTreeMap("map");
                //.getHashMap("map");

        long time = System.currentTimeMillis();
        long max = (int) 1e8;
        AtomicLong progress = new AtomicLong(0);
        Utils.printProgress(progress);

        while(progress.incrementAndGet()<max){
            Long val = Utils.RANDOM.nextLong();
            map.put(val, "test"+val);
        }
        progress.set(-1);

        System.out.println("Closing");
        db.close();

        System.out.println(System.currentTimeMillis() - time);

    }
}
