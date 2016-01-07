package org.mapdb.benchmark;


import org.mapdb.DataIO;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class BU {

    public static final String RESULT_PROPERTIES = "res/result.properties";

    public final static String TMPDIR = System.getProperty("java.io.tmpdir");

    private static final char[] chars = "0123456789abcdefghijklmnopqrstuvwxyz !@#$%^&*()_+=-{}[]:\",./<>?|\\".toCharArray();

    public static void resultAdd(String key, String value) throws IOException {
        Properties p = new Properties(){
            public synchronized Enumeration<Object> keys() {
                return Collections.enumeration(new TreeSet<Object>(super.keySet()));
            }
        };

        File f = new File(BU.RESULT_PROPERTIES);
        if(f.exists())
            p.load(new FileReader(f));
        p.put(key, value);
        p.store(new FileWriter(f),"mapdb benchmarks");
    }

    public static String resultGet(String key) throws IOException {
        Properties p = new Properties();
        File f = new File(BU.RESULT_PROPERTIES);
        if(f.exists())
            p.load(new FileReader(f));

        return (String) p.get(key);
    }


    public static String randomString(int size) {
        return randomString(size, (int) (100000 * Math.random()));
    }

    public static String randomString(int size, int seed) {
        StringBuilder b = new StringBuilder(size);
        for(int i=0;i<size;i++){
            b.append(chars[Math.abs(seed)%chars.length]);
            seed = 31*seed+ DataIO.intHash(seed);

        }
        return b.toString();
    }

    /* faster version of Random.nextBytes() */
    public static byte[] randomByteArray(int size){
        return randomByteArray(size, (int) (100000 * Math.random()));
    }
    /* faster version of Random.nextBytes() */
    public static byte[] randomByteArray(int size, int randomSeed){
        byte[] ret = new byte[size];
        for(int i=0;i<ret.length;i++){
            ret[i] = (byte) randomSeed;
            randomSeed = 31*randomSeed+DataIO.intHash(randomSeed);
        }
        return ret;
    }



    public static void mkdir(String dir){
        new File(dir).mkdirs();
    }

    public static long randomLong(long maxVal) {
        return Math.abs(new Random().nextLong()%maxVal);
    }

    public static void shutdown(ExecutorService t){
        t.shutdown();
        try {
            t.awaitTermination(9999, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            throw new Error(e);
        }
    }

    static private AtomicLong printCounter = new AtomicLong();
    static private volatile long printTime;
    static private volatile String printTitle;
    static private volatile String printTaskName;
    static private volatile int printThreadNum;

    static public void printStart(String title, String taskName, int threadNum){
        printTitle = title;
        printTaskName = taskName;
        printThreadNum = threadNum;
        printTime = System.currentTimeMillis();
        printCounter.set(0);
    }

    static public void printEnd(){
        long t = System.currentTimeMillis()-printTime;
        if(t==0)
            t=1;
        long c = printCounter.get();
        System.out.printf("%-30s  -  %8s  -  %2d  -  %,10d \n",
                printTitle,
                printTaskName,
                printThreadNum,
                (1000L * c) / t);


        try{
            File f = new File(BU.RESULT_PROPERTIES);
            if(f.getParentFile().exists()) {
                Properties props = new Properties();
                if(f.exists())
                    props.load(new FileReader(f));
                props.put(printTitle + "_" + printTaskName + "_" + printThreadNum, ""+c);
                OutputStream out = new FileOutputStream(f);
                props.store(out, "");
                out.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }

    }

    static public void printIncrement(){
        printCounter.incrementAndGet();
    }

    static boolean printIncrementTime(long time){
        printCounter.incrementAndGet();
        return printTime + time > System.currentTimeMillis();
    }


    static public void gc(){
        for(int i=0;i<10;i++){
            System.gc();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException();
            }
        }

    }


    static public Iterator<Long> reverseLongIterator(final long start, final long end){

        return new Iterator<Long>() {

            long counter = end;

            @Override
            public boolean hasNext() {
                return counter>=start;
            }

            @Override
            public Long next() {
                long ret = counter--;
                if(ret<start)
                    throw new NoSuchElementException();
                return ret;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static void execNTimes(int n, final Callable r){
        ExecutorService s = Executors.newFixedThreadPool(n);
        final CountDownLatch wait = new CountDownLatch(n);

        List<Future> f = new ArrayList();

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

        for(Future ff:f){
            try {
                ff.get();
            } catch (Exception e) {
                throw new Error(e);
            }
        }

    }

    static String outStreamToString(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for(int b=in.read();b!=-1;b=in.read()){
            out.write(b);
        }
        return new String(out.toByteArray());
    }

}
