package org.mapdb;

import junit.framework.AssertionFailedError;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.*;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Runs WAL and crashes JVM to test it
 */
@RunWith(Parameterized.class)
public class CrashTest {

    public static File FILE;

    public static final class Params implements Serializable{

        final int index;
        final File file;
        final DBMaker.Maker dbMaker;
        final boolean clearMap;
        final boolean hashMap;
        final boolean largeVals;

        public Params(int index, File file, DBMaker.Maker dbMaker, boolean clearMap, boolean hashMap, boolean largeVals) throws IOException {
            this.index = index;
            this.file = file;
            this.dbMaker = dbMaker;
            this.clearMap = clearMap;
            this.hashMap = hashMap;
            this.largeVals = largeVals;
        }

    }
    final Params p;

    public CrashTest(Params p) {
        this.p = p;
    }

    @Parameterized.Parameters
    public static List<Object[]> params() throws IOException {
        List ret = new ArrayList();
        if(TT.shortTest())
            return ret;

        int index=0;

        for(boolean notAppend:TT.BOOLS){
            for(boolean tx:TT.BOOLS){
                for(boolean mmap:TT.BOOLS) {
                    for (boolean cache : TT.BOOLS) {
                        for (boolean largeVals : TT.BOOLS) {
                            for (boolean clearMap : TT.BOOLS) {
                                for (boolean hashMap : TT.BOOLS) {
                                    File f = FILE!=null? FILE :
                                            File.createTempFile("mapdbTest", "mapdb");
                                    DBMaker.Maker maker = !notAppend ?
                                            DBMaker.appendFileDB(f) :
                                            DBMaker.fileDB(f);

                                    maker.fileLockDisable();

                                    if (mmap)
                                        maker.fileMmapEnableIfSupported();

                                    if (!tx)
                                        maker.transactionDisable();

                                    if (cache)
                                        maker.cacheHashTableEnable();

                                    ret.add(new Object[]{
                                            new Params(index++, f, maker, clearMap,
                                                    hashMap, largeVals)});
                                }
                            }
                        }
                    }
                }
            }
        }

        return ret; }

    @Test
    public void test() throws IOException, InterruptedException {
        if(TT.scale()==0)
            return;

        long end = System.currentTimeMillis()+1000*60*10* TT.scale();

        String tmpDir = System.getProperty("java.io.tmpdir");
        if(new File(tmpDir).getFreeSpace()<20e9)
            fail("not enough free disk space");

        String props = tmpDir+"/mapdbTestProps"+Math.random();
        while(end>System.currentTimeMillis()) {
            ProcessBuilder b = new ProcessBuilder("java",
                    "-classpath", System.getProperty("java.class.path"),
                    "-Dmdbtest="+TT.scale(),
                    this.getClass().getName(),
                    props,this.p.file.getAbsolutePath(),""+this.p.index);
            Process p = b.start();
            p.waitFor();
            String out = outStreamToString(p.getInputStream());
            System.err.println(outStreamToString(p.getErrorStream()));
            assertTrue(out,out.startsWith("started_"));
            assertTrue(out, out.endsWith("_killed"));
            assertEquals(137, p.exitValue());

//            assertEquals(out,"", outStreamToString(p.getErrorStream()));
        }
    }

    public static void main(String[] args) throws IOException {
        try {
            killThisJVM(10000);
            System.out.print("started_");
            File props = new File(args[0]);
            props.mkdir();

            FILE = new File(args[1]);

            int index = Integer.valueOf(args[2]);

            Params p = (Params) params().get(index)[0];

            DB db = p.dbMaker.make();

            Map<Long, byte[]> m = (Map) (
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

            long seed;

            //find last sucessfull commmit
            if (props.exists() && props.listFiles().length > 0) {
                //list all files, find latest one
                final File[] ff = props.listFiles();
                Arrays.sort(ff);
                seed = Long.valueOf(ff[ff.length - 1].getName());
                for (int i = 0; i < ff.length - 1; i++) {
                    ff[i].delete();
                }

                //check content of map
                Random r = new Random(seed);
                for (long i = 0; i < 1000; i++) {
                    int size = r.nextInt(p.largeVals ? 100000 : 100);
                    byte[] b = TT.randomByteArray(size, r.nextInt());
                    if (!Arrays.equals(b, m.get(i))) {
                       throw new AssertionFailedError("Wrong arrays");
                    }
                }
            }


            while (true) {
                seed = System.currentTimeMillis();
                Random r = new Random(seed);
                for (long i = 0; i < 1000; i++) {
                    int size = r.nextInt(p.largeVals ? 100000 : 100);
                    byte[] b = TT.randomByteArray(size, r.nextInt());
                    m.put(i, b);
                }
                db.commit();
                if (p.clearMap && r.nextInt(10) <= 1)
                    m.clear();

                if (!new File(props, "" + seed).createNewFile())
                    throw new RuntimeException("could not create props file");
            }
        }catch(Throwable e){
            if(FILE!=null)
                System.err.println("Free space: "+FILE.getFreeSpace());
            e.printStackTrace();
            System.exit(-1111);
        }finally {
            if(FILE!=null)
                FILE.delete();
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
