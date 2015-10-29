package org.mapdb;

import junit.framework.AssertionFailedError;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.*;
import java.util.*;

import static org.junit.Assert.*;

/*
 * Runs WAL and crashes JVM to test it
 *
 * This test start new JVM and kills it (kill PID -9) after random interval  (up to 1 minute).
 * Than it checks content of the file, starts new JVM and repeats.
 *
 * Forked JVM inserts random value based on random seed. Seed file is created before and after each commit,
 * so we know what seed value was.
 *
 */
@RunWith(Parameterized.class)
public class CrashTest {

    static final int MIN_RUNTIME = 3000;
    static final int MAX_RUNTIME = 10000;

    public static final class Params implements Serializable{

        final int index;
        final DBMaker.Maker dbMaker;
        final boolean clearMap;
        final boolean hashMap;
        final boolean largeVals;
        final int mapSize;

        public Params(int index, DBMaker.Maker dbMaker, boolean clearMap, boolean hashMap, boolean largeVals, int mapSize) throws IOException {
            this.index = index;
            this.dbMaker = dbMaker;
            this.clearMap = clearMap;
            this.hashMap = hashMap;
            this.largeVals = largeVals;
            this.mapSize = mapSize;
        }
    }

    static final File nonExistent = TT.tempDbFile();

    File dir;

    final Params p;

    public CrashTest(Params p) {
        this.p = p;
    }

    @Parameterized.Parameters
    public static List<Object[]> params() throws IOException {
        List ret = new ArrayList();

        int index=0;

        for( boolean notAppend:TT.BOOLS)
        for( boolean mmap:TT.boolsOrFalseIfQuick())
        for( boolean cache : TT.boolsOrFalseIfQuick())
        for( boolean largeVals : TT.boolsOrFalseIfQuick())
        for( boolean clearMap : TT.boolsOrFalseIfQuick())
        for( boolean hashMap : TT.BOOLS)
        for( int mapSize : TT.shortTest()? new int[]{100}:new int[]{10,0,1000})
        {

            DBMaker.Maker maker = notAppend ?
                    DBMaker.fileDB(nonExistent) :
                    DBMaker.appendFileDB(nonExistent);

            maker.fileLockDisable();
            maker.checksumEnable();

            if (mmap)
                maker.fileMmapEnableIfSupported().fileMmapCleanerHackEnable();

            if (cache)
                maker.cacheHashTableEnable();

            ret.add(new Object[]{
                    new Params(index++, maker, clearMap,
                            hashMap, largeVals, mapSize)});

        }

        return ret;
    }

    @Test
    public void test() throws IOException, InterruptedException {
        if(TT.shortTest())
            return;

        dir =
                new File(System.getProperty("java.io.tmpdir")
                        +"/mapdbTest"+System.currentTimeMillis()+Math.random());


        //create folders
        dir.mkdirs();

        File seedStartDir = new File(dir,"seedStart");
        File seedEndDir = new File(dir,"seedEnd");


        long end = TT.nowPlusMinutes(0.5+TT.scale()*9);
        if(dir.getFreeSpace()<10e9)
            fail("not enough free disk space, at least 10GB needed: "+dir.getFreeSpace());

        assertTrue(dir.exists() && dir.isDirectory() && dir.canWrite());


        long oldSeed=0;
        long crashCount = 0;

        while(end>System.currentTimeMillis()) {
            //fork JVM, pass current dir and config index as param
            {
                ProcessBuilder b = new ProcessBuilder(
                        jvmExecutable(),
                        "-classpath",
                        System.getProperty("java.class.path"),
                        "-Dmdbtest=" + TT.scale(),
                        this.getClass().getName(),
                        dir.getAbsolutePath(),
                        "" + this.p.index);
                Process pr = b.start();
                pr.waitFor(); //it should kill itself after some time

                Thread.sleep(100);// just in case

                //handle output streams
                String out = outStreamToString(pr.getInputStream());
                System.err.print(outStreamToString(pr.getErrorStream()));
                assertTrue(out, out.startsWith("started_"));
                assertTrue(out, out.endsWith("_killed"));
                assertEquals(137, pr.exitValue());

            }

            //now reopen file and check its content
            p.dbMaker.props.put(DBMaker.Keys.file,dir.getPath()+"/store");
            DB db = p.dbMaker.make();
            Atomic.Long dbSeed = db.atomicLong("seed");

            assertTrue(dbSeed.get()>=oldSeed);

            seedEndDir.mkdirs();
            seedStartDir.mkdirs();

            File[] seedStartFiles = seedStartDir.listFiles();
            File[] seedEndFiles = seedEndDir.listFiles();


            if(seedStartFiles.length==0) {
                // JVM interrupted before creating any seed files
                // in that case seed should not change
                if(oldSeed!=0)
                    assertEquals(oldSeed, dbSeed.get());
            }else if(seedEndFiles.length== seedStartFiles.length ){
                //commit finished fine,
                assertEquals(getSeed(seedStartDir,0), getSeed(seedEndDir,0));
                //content of database should be applied
                assertEquals(dbSeed.get(),getSeed(seedStartDir,0));
            }else if(seedStartFiles.length==1){
                //only single commit started, in that case it did not succeeded, or it did succeeded
                assertTrue(dbSeed.get()==oldSeed || dbSeed.get()==getSeed(seedStartDir, 0));
            }else{
                long minimalSeed =
                        seedEndFiles.length>0?
                                getSeed(seedEndDir,0):
                                oldSeed;
                assertTrue(""+minimalSeed+"<=" +dbSeed.get(), minimalSeed<=dbSeed.get());

                //either last started commit succeeded or commit before that succeeded
                assertTrue(" "+dbSeed.get(), dbSeed.get()==getSeed(seedStartDir, 0) || dbSeed.get()==getSeed(seedStartDir, 1));
            }

            if(dbSeed.get()!=oldSeed)
                crashCount++;

            Map<Long,byte[]> m = map(p,db);
            //check content of map
            Random r = new Random(dbSeed.get());
            for (long i = 0; i < p.mapSize; i++) {
                byte[] b = getBytes(p, r);
                if (!Arrays.equals(b, m.get(i))) {
                    throw new AssertionFailedError("Wrong arrays");
                }
            }
            oldSeed = dbSeed.get();
            db.close();

            //cleanup seeds
            TT.dirDelete(seedEndDir);
            TT.dirDelete(seedStartDir);

            if(dir.getFreeSpace()<1e9){
                System.out.println("Not enough free space, delete store and start over");
                TT.dirDelete(dir);
                dir.mkdirs();
                assertTrue(dir.exists() && dir.isDirectory() && dir.canWrite());
            }

        }
        assertTrue("no commits were made",crashCount>0);
        System.out.println("Finished after " + crashCount + " crashes");
    }

    @After
    public void clean(){
        if(dir!=null)
            TT.dirDelete(dir);
    }

    public static void main(String[] args) throws IOException {
        File dir = new File(args[0]);
        try {
            //start kill timer
            killThisJVM(MIN_RUNTIME + new Random().nextInt(MAX_RUNTIME - MIN_RUNTIME));

            System.out.print("started_");
            //collect all parameters

             int index = Integer.valueOf(args[1]);
            Params p = (Params) params().get(index)[0];

            File seedStartDir = new File(dir,"seedStart");
            File seedEndDir = new File(dir,"seedEnd");
            seedStartDir.mkdirs();
            seedEndDir.mkdirs();

            p.dbMaker.props.put(DBMaker.Keys.file,dir.getPath()+"/store");
            DB db = p.dbMaker.make();
            Atomic.Long dbSeed = db.atomicLong("seed");

            Map<Long, byte[]> m = map(p, db);

            long seed;

            while (true) {
                seed = System.currentTimeMillis();
                dbSeed.set(seed);

                Random r = new Random(seed);
                for (long i = 0; i < p.mapSize; i++) {
                    byte[] b = getBytes(p, r);
                    m.put(i, b);
                }

                //create seed file before commit
                assertTrue(new File(seedStartDir, "" + seed).createNewFile());

                db.commit();

                //create seed file after commit
                assertTrue(new File(seedEndDir, "" + seed).createNewFile());

                //wait until clock increases
                while(seed==System.currentTimeMillis()) {
                    Thread.sleep(1);
                }

                //randomly delete content of map
                if (p.clearMap && r.nextInt(10) <= 1)
                    m.clear();
            }
        }catch(Throwable e){
            if(dir !=null)
                System.err.println("Free space: "+ dir.getFreeSpace());
            e.printStackTrace();
            System.exit(-1111);
        }
    }

    private static byte[] getBytes(Params p, Random r) {
        int size = r.nextInt(p.largeVals ? 10000 : 10);
        return TT.randomByteArray(size, r.nextInt());
    }

    private static Map map(Params p, DB db) {
        return (Map) (
                p.hashMap ?
                        db.hashMapCreate("hash")
                                .keySerializer(Serializer.LONG)
                                .valueSerializer(Serializer.BYTE_ARRAY)
                                .makeOrGet() :
                        db.treeMapCreate("hash")
                                .keySerializer(Serializer.LONG)
                                .valueSerializer(Serializer.BYTE_ARRAY)
                                .valuesOutsideNodesEnable()
                                .makeOrGet());
    }


    static void killThisJVM(final long delay){
       Thread t = new Thread(){
           @Override
           public void run() {
               try {
                   Thread.sleep(delay);
               } catch (InterruptedException e) {
                   e.printStackTrace();
               }
               try {
                   killThisJVM();
               } catch (IOException e) {
                   e.printStackTrace();
               } catch (InterruptedException e) {
                   e.printStackTrace();
               }
           }
       };
        t.setDaemon(true);
        t.start();
    }

    static void killThisJVM() throws IOException, InterruptedException {
        String pid = new File("/proc/self").getCanonicalFile().getName();

        Long.valueOf(pid);
        System.out.print("killed");
        ProcessBuilder b = new ProcessBuilder("kill", "-9", pid);
        b.start();
        while(true){
            Thread.sleep(10000);
            System.out.println("KILL - Still alive");
        }
    }

    static String outStreamToString(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for(int b=in.read();b!=-1;b=in.read()){
            out.write(b);
        }
        return new String(out.toByteArray());
    }

    static long getSeed(File seedDir, int indexFromEnd){
        File[] f = seedDir.listFiles();
        Arrays.sort(f);
        return Long.valueOf(f[f.length-1-indexFromEnd].getName());
    }

    static String jvmExecutable(){
        String exec = System.getProperty("os.name").startsWith("Win") ? "java.exe":"java";
        String javaHome = System.getProperty("java.home");
        if(javaHome==null ||"".equals(javaHome))
            return exec;
        return javaHome+ File.separator + "bin" + File.separator + exec;
    }
}
