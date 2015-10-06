package org.mapdb;

import java.util.List;
import java.util.concurrent.*;

/*
 *
 */
public class Exec {

    public static void execNTimes(int n, final Callable r){
        ExecutorService s = Executors.newFixedThreadPool(n);
        final CountDownLatch wait = new CountDownLatch(n);

        List<Future> f = new CopyOnWriteArrayList<Future>();

        Runnable r2 = new Runnable(){

            @Override
            public void run() {
                wait.countDown();
                try {
                    wait.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                try {
                    r.call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        for(int i=0;i<n;i++){
            f.add(s.submit(r2));
        }

        s.shutdown();

        while(!f.isEmpty()) {
            for (Future ff : f) {
                try {
                    ff.get(1, TimeUnit.SECONDS);
                    f.remove(ff);
                } catch (TimeoutException e) {
                    //ignored
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }
        }
    }
}
