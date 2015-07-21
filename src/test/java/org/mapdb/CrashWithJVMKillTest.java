package org.mapdb;

import org.junit.Test;

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Runs WAL and crashes JVM to test it
 */
public class CrashWithJVMKillTest {

    @Test
    public void test() throws IOException, InterruptedException {
        if(UtilsTest.scale()==0)
            return;

        long end = System.currentTimeMillis()+1000*60*10*UtilsTest.scale();

        String tmpDir = System.getProperty("java.io.tmpdir");
        String wal = tmpDir+"/mapdbTest"+Math.random();
        String props = wal+"props";
        while(end>System.currentTimeMillis()) {
            ProcessBuilder b = new ProcessBuilder("java",
                    "-classpath", System.getProperty("java.class.path"),
                    this.getClass().getName(),
                    wal,props);
            Process p = b.start();
            p.waitFor();
            String out = outStreamToString(p.getInputStream());
            assertTrue(out.startsWith("started_"));
            assertTrue(out.endsWith("_killed"));
            assertEquals(137, p.exitValue());
            assertEquals("", outStreamToString(p.getErrorStream()));
        }
    }

    public static void main(String[] args) throws IOException {
        killThisJVM(10000);
        System.out.print("started_");
        File wal = new File(args[0]);
        wal.mkdir();
        File props = new File(args[1]);
        props.mkdir();

        DB db = DBMaker.fileDB(new File(wal, "store"))
                .make();

        Map<Long,byte[]> m = db.treeMapCreate("hash")
                .keySerializer(Serializer.LONG)
                .valueSerializer(Serializer.BYTE_ARRAY)
                .valuesOutsideNodesEnable()
                .makeOrGet();

        long seed = System.currentTimeMillis();

        //find last sucessfull commmit
        if(props.exists() && props.listFiles().length>0){
            //list all files, find latest one
            File[] ff = props.listFiles();
            Arrays.sort(ff);
            seed = Long.valueOf(ff[ff.length-1].getName());

            //check content of map
            Random r = new Random(seed);
            for(long i=0;i<1000;i++){
                byte[] b = new byte[r.nextInt(100000)];
                r.nextBytes(b);
                if(!Arrays.equals(b,m.get(i))){
                    System.out.println("Wrong");
;                    System.exit(0xFFFFF);
                }
            }
        }


        while(true){
            seed = System.currentTimeMillis();
            Random r = new Random(seed);
            for(long i=0;i<1000;i++){
                byte[] b = new byte[r.nextInt(100000)];
                r.nextBytes(b);
                m.put(i,b);
            }
            db.commit();

            if(!new File(props,""+seed).createNewFile())
                throw new RuntimeException("could not create props file");
        }

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
}
