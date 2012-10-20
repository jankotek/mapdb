package benchmark;

import net.kotek.jdbm.DB;
import net.kotek.jdbm.DBMaker;
import net.kotek.jdbm.JdbmUtil;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Tests massive concurrent insert
 */
public class MassiveInsert {

    final static int threads = 4 ;
    final static long max = (long) 1e9;



    public static void main(String[] args) throws InterruptedException {

        long t = System.currentTimeMillis();

        final DB db = DBMaker
                .newFileDB(new File("/media/big/db"))
                //.newMemoryDB()
                .closeOnJvmShutdown()
                .deleteFilesAfterClose()
//                .appendOnlyEnable()
                .transactionDisable()
//                .asyncSerializationDisable()
                .make();

        //final Map m = db.getTreeMap("treeMap");
        final Map m = db.getHashMap("hashMap");

        final ExecutorService exec = Executors.newFixedThreadPool(threads);
        final AtomicLong counter = new AtomicLong(0);

        final Logger log = Logger.getLogger("test");

        for(int i=0;i<threads;i++){
            final int threadNum = i;
            exec.execute(new Runnable() {
                @Override public void run() {
                    for(long j=threadNum;j<max;j+=threads){
                        //log.fine(""+Thread.currentThread().getId()+" - "+j);
                        m.put(j, Long.toHexString(j));
                        counter.incrementAndGet();
                    }
                }
            });
        }


        exec.shutdown();

        while(!exec.awaitTermination(10, TimeUnit.SECONDS)){
            long t2 = System.currentTimeMillis()-t;
            System.out.println(counter.get()+" - "+t2/1000+ " - "+(1000*counter.get()/t2)+" rec/sec");
        }
        db.close();
        System.out.println("DONE");
        long t2 = System.currentTimeMillis()-t;
        System.out.println(counter.get()+" - "+t2/1000+ " - "+(1000*counter.get()/t2)+" rec/sec");




    }
}
