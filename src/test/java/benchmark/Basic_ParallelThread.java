package benchmark;

import org.mapdb.*;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test basic CRUD operations with multiple threads
 */
public class Basic_ParallelThread {
    static final int MAX = (int) 1e8;

    static final String path = "/home/plain/db"+System.currentTimeMillis();

    static final int THREAD_NUM = 4;

    public static void main(String[] args) throws InterruptedException {
        DB db = DBMaker.newFileDB(new File(path))
                .writeAheadLogDisable()
                .asyncWriteDisable()
                .make();
        final Map m = db.createTreeMap("test",32,true,false, BTreeKeySerializer.ZERO_OR_POSITIVE_INT, Serializer.STRING_SERIALIZER,null);
        long time = System.currentTimeMillis();

        //insert
        for(Integer ii=0;ii<MAX;ii++){
            m.put(ii, "aaadqw "+ii);
        }

        long time2 = System.currentTimeMillis();
        System.out.println("INSERT: "+(time2 - time));
        time = time2;

        //RANDOM READS
        final AtomicInteger i = new AtomicInteger(0);
        ExecutorService exec = Executors.newFixedThreadPool(THREAD_NUM);
        for(int j=0;j<THREAD_NUM;j++){
            exec.submit(new Runnable() {
                @Override
                public void run() {
                    while(i.incrementAndGet()<MAX){
                        m.get(Utils.RANDOM.nextInt(MAX));
                    }
                }
            });
        }

        exec.shutdown();
        exec.awaitTermination(1, TimeUnit.DAYS);


        time2 = System.currentTimeMillis();
        System.out.println("READS: "+(time2 - time));
        time = time2;

        //RANDOM UPDATES
        i.set(0);
        exec = Executors.newFixedThreadPool(THREAD_NUM);
        for(int j=0;j<THREAD_NUM;j++){
            exec.submit(new Runnable() {
                @Override
                public void run() {
                    while(i.incrementAndGet()<MAX){
                        m.put(Utils.RANDOM.nextInt(MAX), "asdasdasdasd"+i);
                    }
                }
            });
        }

        exec.shutdown();
        exec.awaitTermination(1, TimeUnit.DAYS);


        time2 = System.currentTimeMillis();
        System.out.println("UPDATES: "+(time2 - time));
        time = time2;


        db.close();
    }
}
