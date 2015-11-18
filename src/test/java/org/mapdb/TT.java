package org.mapdb;

import com.sun.management.UnixOperatingSystemMXBean;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class TT {

    private static int SCALE;
    static{
        String prop = System.getProperty("mdbtest");
        try {
            SCALE = prop==null?0:Integer.valueOf(prop);
        }catch(NumberFormatException e){
            SCALE = 0;
        }

    }

    /** how many hours should unit tests run? Controlled by:
     *
     * {@code mvn test -Dmdbtest=2}
     *
     * @return test scale
     */
    public static int scale() {
        return SCALE;
    }

    public static long nowPlusMinutes(double minutes){
        return System.currentTimeMillis() + (long)(scale()+1000*60*minutes);
    }


    public static boolean shortTest() {
        return scale()==0;
    }

    public static final boolean[] BOOLS = {true, false};

    public static boolean[] boolsOrTrueIfQuick(){
        return shortTest()? new boolean[]{true}:BOOLS;
    }

    public static boolean[] boolsOrFalseIfQuick(){
        return shortTest()? new boolean[]{false}:BOOLS;
    }

    @Test public void testPackInt() throws Exception {

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        DataIO.DataInputByteBuffer in = new DataIO.DataInputByteBuffer(ByteBuffer.wrap(out.buf,0, out.pos),0);
        for(int i = 0;i>-1; i = i + 1 + i/1111){  //overflow is expected
            out.pos = 0;

            DataIO.packInt(out, i);
            in.pos = 0;
            in.buf.clear();

            int i2 = DataIO.unpackInt(in);

            Assert.assertEquals(i, i2);
        }
    }


    @Test public void testPackIntBigger() throws Exception {

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        DataIO.DataInputByteBuffer in = new DataIO.DataInputByteBuffer(ByteBuffer.wrap(out.buf,0, out.pos),0);
        for(int i = 0;i>-1; i = i + 1 + i/1111){  //overflow is expected
            out.pos = 0;

            DataIO.packIntBigger(out, i);
            in.pos = 0;
            in.buf.clear();

            int i2 = DataIO.unpackInt(in);

            Assert.assertEquals(i, i2);
        }
    }


    @Test public void testPackLong() throws Exception {

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        DataIO.DataInputByteBuffer in = new DataIO.DataInputByteBuffer(ByteBuffer.wrap(out.buf,0, out.pos),0);
        for(long i = 0;i>-1L  ; i=i+1 + i/111){  //overflow is expected
            out.pos = 0;

            DataIO.packLong((DataOutput) out, i);
            in.pos = 0;
            in.buf.clear();

            long i2 = DataIO.unpackLong(in);
            Assert.assertEquals(i, i2);

        }
    }

    @Test public void testArrayPut(){
        assertEquals(asList(1,2,3,4,5), asList(BTreeMap.arrayPut(new Integer[]{1, 2, 4, 5}, 2, 3)));
        assertEquals(asList(1,2,3,4,5), asList(BTreeMap.arrayPut(new Integer[]{2, 3, 4, 5}, 0, 1)));
        assertEquals(asList(1,2,3,4,5), asList(BTreeMap.arrayPut(new Integer[]{1, 2, 3, 4}, 4, 5)));
    }

    @Test
    public void testNextPowTwo() throws Exception {
        int val=9;
        assertEquals(16, 1 << (32 - Integer.numberOfLeadingZeros(val - 1)));
        val = 8;
        assertEquals(8, 1 << (32 - Integer.numberOfLeadingZeros(val - 1)));
    }



    /* clone value using serialization */
    public static <E> E clone(E value, Serializer<E> serializer){
        try{
            DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
            serializer.serialize(out, value);
            DataIO.DataInputByteBuffer in = new DataIO.DataInputByteBuffer(ByteBuffer.wrap(out.copyBytes()), 0);

            return serializer.deserialize(in,out.pos);
        }catch(IOException ee){
            throw new IOError(ee);
        }
    }

    /* clone value using java serialization */
    public static <E> E cloneJavaSerialization(E value) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream out2 = new ObjectOutputStream(out);
        out2.writeObject(value);
        out2.flush();

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        return (E) new ObjectInputStream(in).readObject();
    }




    public static Serializer FAIL = new Serializer() {
        @Override
        public void serialize(DataOutput out, Object value) throws IOException {
            throw new RuntimeException();
        }

        @Override
        public Object deserialize(DataInput in, int available) throws IOException {
            throw new RuntimeException();
        }

        @Override
        public int fixedSize() {
            return -1;
        }

    };


    /*
     * Create temporary file in temp folder. All associated db files will be deleted on JVM exit.
     */
    public static File tempDbFile() {
        try{
            File index = File.createTempFile("mapdbTest","db");
            index.deleteOnExit();

            return index;
        }catch(IOException e){
            throw new IOError(e);
        }
    }

    public static File tempDbDir() {
        String tmpDir = System.getProperty("java.io.tmpdir");
        File ret =  new File(tmpDir+File.separator+"mapdbTest"+System.currentTimeMillis()+"-"+Math.random());
        ret.mkdir();
        return ret;
    }

    private static final char[] chars = "0123456789abcdefghijklmnopqrstuvwxyz !@#$%^&*()_+=-{}[]:\",./<>?|\\".toCharArray();


    public static String randomString(int size) {
        return randomString(size, (int) (100000 * Math.random()));
    }

    public static String randomString(int size, int seed) {
        StringBuilder b = new StringBuilder(size);
        for(int i=0;i<size;i++){
            b.append(chars[Math.abs(seed)%chars.length]);
            seed = 31*seed+DataIO.intHash(seed);

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
    public static int randomInt() {
        return new Random().nextInt();
    }

    public static long fileHandles(){
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        if(os instanceof UnixOperatingSystemMXBean){
            return ((UnixOperatingSystemMXBean) os)
                    //TODO log warning if needed
            .getOpenFileDescriptorCount();

            //TODO max file descriptors in doc
//                    /etc/security/limits.conf
//                    * soft nofile 4096
//                    * hard nofile 65535 <<<

        }
        return -1;
    }

    public static <E> Future<E> fork(Callable<E> callable) {
        ExecutorService s = Executors.newSingleThreadExecutor();
        Future<E> f = s.submit(callable);
        s.shutdown();
        return f;
    }

    public static List<Future> fork(int count, Callable callable) {
        ArrayList<Future> ret = new ArrayList();
        for(int i=0;i<count;i++){
            ret.add(fork(callable));
        }
        return ret;
    }

    public static void forkAwait(List<Future> futures) throws ExecutionException, InterruptedException {
        futures = new ArrayList(futures);

        while(!futures.isEmpty()){
            for(int i=0; i<futures.size();i++){
                Future f = futures.get(i);
                try {
                    f.get(100, TimeUnit.MILLISECONDS);
                    //get was fine, so remove from list and continue
                    futures.remove(i);
                } catch (TimeoutException e) {
                    //thats fine, not yet finished, continue
                }
            }
        }
    }

    public static String serializeToString(Object o) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream out2 = new ObjectOutputStream(out);
        out2.writeObject(o);
        out2.close();
        byte[] b = out.toByteArray();
        return DataIO.toHexa(b);
    }

    public static <A> A deserializeFromString(String s) throws IOException, ClassNotFoundException {
        ByteArrayInputStream in = new ByteArrayInputStream(DataIO.fromHexa(s));
        return (A) new ObjectInputStream(in).readObject();
    }

    /** recursive delete directory */
    public static void dirDelete(File dir) {
        String tempDir = System.getProperty("java.io.tmpdir");
        assertTrue(dir.getAbsolutePath().startsWith(tempDir));
        dirDelete2(dir);
    }

    private static void dirDelete2(File dir){
        if(dir.isDirectory()) {
            for (File f : dir.listFiles()) {
                dirDelete2(f);
            }
        }
        dir.delete();
    }

    public static void sortAndEquals(long[] longs, long[] longs1) {
        Arrays.sort(longs);
        Arrays.sort(longs1);
        assertArrayEquals(longs,longs1);
    }

    public static void assertZeroes(Volume vol, long start, long end) {
        for(long offset = start; offset<end;offset++){
            if(0!=vol.getUnsignedByte(offset))
                throw new AssertionError("Not zero at offset: "+offset);
        }
    }
}
