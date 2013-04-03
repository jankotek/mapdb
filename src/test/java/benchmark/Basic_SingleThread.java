package benchmark;

import org.mapdb.*;

import java.io.File;
import java.util.Map;

/**
 * Test basic CRUD operations with single thread
 */
public class Basic_SingleThread {
    static final int MAX = (int) 1e8;

    static final String path = "/home/plain/db"+System.currentTimeMillis();


    public static void main(String[] args) {
        DB db = DBMaker.newFileDB(new File(path))
                .writeAheadLogDisable()
                .make();
        Map m = db.createTreeMap("test",32,true,false, BTreeKeySerializer.ZERO_OR_POSITIVE_INT, Serializer.STRING_SERIALIZER,null);
        long time = System.currentTimeMillis();
        //insert
        for(Integer i=0;i<MAX;i++){
            m.put(i, "aaadqw "+i);
        }
        long time2 = System.currentTimeMillis();
        System.out.println("INSERT: "+(time2 - time));
        time = time2;

        //RANDOM READS
        for(int i=0;i<MAX;i++){
            m.get(Utils.RANDOM.nextInt(MAX));
        }
        time2 = System.currentTimeMillis();
        System.out.println("READS: "+(time2 - time));
        time = time2;

        //RANDOM UPDATES
        for(int i=0;i<MAX;i++){
            m.put(Utils.RANDOM.nextInt(MAX), "asdasdasdasd"+i);
        }
        time2 = System.currentTimeMillis();
        System.out.println("UPDATES: "+(time2 - time));
        time = time2;


        db.close();
    }
}
