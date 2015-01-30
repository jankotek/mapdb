package org.mapdb;

import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Random;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class UtilsTest {


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

    @Test public void testPackLong() throws Exception {

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        DataIO.DataInputByteBuffer in = new DataIO.DataInputByteBuffer(ByteBuffer.wrap(out.buf,0, out.pos),0);
        for(long i = 0;i>-1L  ; i=i+1 + i/111){  //overflow is expected
            out.pos = 0;

            DataIO.packLong(out, i);
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



    /** clone value using serialization */
    public static <E> E clone(E value, Serializer<E> serializer){
        try{
            DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
            serializer.serialize(out,value);
            DataIO.DataInputByteBuffer in = new DataIO.DataInputByteBuffer(ByteBuffer.wrap(out.copyBytes()), 0);

            return serializer.deserialize(in,out.pos);
        }catch(IOException ee){
            throw new IOError(ee);
        }
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


    @Test public void testHexaConversion(){
        byte[] b = new byte[]{11,112,11,0,39,90};
        assertArrayEquals(b, DBMaker.fromHexa(DBMaker.toHexa(b)));
    }

    /**
     * Create temporary file in temp folder. All associated db files will be deleted on JVM exit.
     */
    public static File tempDbFile() {
        try{
            File index = File.createTempFile("mapdb","db");
            index.deleteOnExit();
            new File(index.getPath()+ StoreWAL.TRANS_LOG_FILE_EXT).deleteOnExit();

            return index;
        }catch(IOException e){
            throw new IOError(e);
        }
    }


    public static String randomString(int size) {
        String chars = "0123456789abcdefghijklmnopqrstuvwxyz !@#$%^&*()_+=-{}[]:\",./<>?|\\";
        StringBuilder b = new StringBuilder(size);
        Random r = new Random();
        for(int i=0;i<size;i++){
            b.append(chars.charAt(r.nextInt(chars.length())));
        }
        return b.toString();
    }

    /** faster version of Random.nextBytes() */
    public static byte[] randomByteArray(int size){
        int seed = (int) (100000*Math.random());
        byte[] ret = new byte[size];
        for(int i=0;i<ret.length;i++){
            ret[i] = (byte) seed;
            seed = DataIO.intHash(seed);
        }
        return ret;
    }

    public static int randomInt() {
        return new Random().nextInt();
    }
}
