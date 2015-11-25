package org.mapdb;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;
import static org.mapdb.CrashTest.*;

public class WALCrash {

    static final int MIN_RUNTIME = 1000;
    static final int MAX_RUNTIME = 2000;

    File dir;

    @Test
    public void crash() throws InterruptedException, IOException {
        if(TT.shortTest())
            return;

        dir = TT.tempDbDir();

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
                        dir.getAbsolutePath()
                );
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
            final AtomicLong dbSeed = new AtomicLong();
            WriteAheadLog wal = new WriteAheadLog(dir.getPath()+"/mapdbWal");
            wal.open(new WriteAheadLog.WALReplay() {
                @Override
                public void beforeReplayStart() {

                }

                @Override
                public void afterReplayFinished() {

                }

                @Override
                public void writeLong(long offset, long value) {
                    fail();
                }

                @Override
                public void writeRecord(long recid, long walId, Volume vol, long volOffset, int length) {
                    long old = dbSeed.getAndSet(recid);
                    //System.err.println("aa "+old+" < "+recid+ " - "+volOffset);
                    assertTrue(old<recid);
                    byte[] b = new byte[31];
                    vol.getData(volOffset, b,0,length);

                    byte[] b2 = TT.randomByteArray(31, (int) recid);
                    assertTrue(Arrays.equals(b2,b));
                }

                @Override
                public void writeByteArray(long offset, long walId, Volume vol, long volOffset, int length) {
                    fail();
                }

                @Override
                public void commit() {

                }

                @Override
                public void rollback() {
                    fail();
                }

                @Override
                public void writeTombstone(long recid) {
                    fail();
                }

                @Override
                public void writePreallocate(long recid) {
                    fail();
                }
            });
            assertTrue(dbSeed.get()>=oldSeed);

            File seedStartDir = new File(dir,"seedStart");
            File seedEndDir = new File(dir,"seedEnd");

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

            oldSeed = dbSeed.get();
            wal.close();

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


    public static void main(String[] args) throws IOException, InterruptedException {
        try {
            //start kill timer
            killThisJVM(MIN_RUNTIME + new Random().nextInt(MAX_RUNTIME - MIN_RUNTIME));

            System.out.print("started_");
            //collect all parameters
            File dir = new File(args[0]);

            File seedStartDir = new File(dir, "seedStart");
            File seedEndDir = new File(dir, "seedEnd");
            seedStartDir.mkdirs();
            seedEndDir.mkdirs();

            WriteAheadLog wal = new WriteAheadLog(dir.getPath() + "/mapdbWal");
            wal.open(WriteAheadLog.NOREPLAY);

            long seed;

            while (true) {
                seed = System.currentTimeMillis();

                byte[] b = TT.randomByteArray(31, (int) seed);

                wal.walPutRecord(seed, b, 0, b.length);

                //create seed file before commit
                assertTrue(new File(seedStartDir, "" + seed).createNewFile());

                wal.commit();

                //create seed file after commit
                assertTrue(new File(seedEndDir, "" + seed).createNewFile());

                //wait until clock increases
                while (seed == System.currentTimeMillis()) {
                    Thread.sleep(1);
                }

            }
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1111);
        }
    }


}
