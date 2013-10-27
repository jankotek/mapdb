package org.mapdb;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class UtilsTest {


    @Test public void testPackInt() throws Exception {

        DataOutput2 out = new DataOutput2();
        DataInput2 in = new DataInput2(ByteBuffer.wrap(out.buf,0, out.pos),0);
        for(int i = 0;i>-1; i = i + 1 + i/1111){  //overflow is expected
            out.pos = 0;

            Utils.packInt(out, i);
            in.pos = 0;
            in.buf.clear();

            int i2 = Utils.unpackInt(in);

            Assert.assertEquals(i, i2);

        }

    }

    @Test public void testPackLong() throws Exception {

        DataOutput2 out = new DataOutput2();
        DataInput2 in = new DataInput2(ByteBuffer.wrap(out.buf,0, out.pos),0);
        for(long i = 0;i>-1L  ; i=i+1 + i/111){  //overflow is expected
            out.pos = 0;

            Utils.packLong(out, i);
            in.pos = 0;
            in.buf.clear();

            long i2 = Utils.unpackLong(in);
            Assert.assertEquals(i, i2);

        }
    }

    @Test public void testArrayPut(){
        assertEquals(asList(1,2,3,4,5), asList(Utils.arrayPut(new Integer[]{1, 2, 4, 5}, 2, 3)));
        assertEquals(asList(1,2,3,4,5), asList(Utils.arrayPut(new Integer[]{2, 3, 4, 5}, 0, 1)));
        assertEquals(asList(1,2,3,4,5), asList(Utils.arrayPut(new Integer[]{1, 2, 3, 4}, 4, 5)));
    }

    @Test
    public void testNextPowTwo() throws Exception {
        assertEquals(16, Utils.nextPowTwo(9));
        assertEquals(8, Utils.nextPowTwo(8));
    }



    /** clone value using serialization */
    public static <E> E clone(E value, Serializer<E> serializer){
        try{
            DataOutput2 out = new DataOutput2();
            serializer.serialize(out,value);
            DataInput2 in = new DataInput2(ByteBuffer.wrap(out.copyBytes()), 0);

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
    };

    @Test
    public void identity_hash_set(){
        Set a = Utils.identityHashSet(Serializer.BASIC, Serializer.BOOLEAN);
        assertTrue(a.contains(Serializer.BASIC));
        assertTrue(a.contains(Serializer.BOOLEAN));
        assertFalse(a.contains(Serializer.LONG));
        a.clear();
    }

    @Test public void testHexaConversion(){
        byte[] b = new byte[]{11,112,11,0,39,90};
        assertArrayEquals(b,Utils.fromHexa(Utils.toHexa(b)));
    }
}
