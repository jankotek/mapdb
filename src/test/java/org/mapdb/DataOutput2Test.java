package org.mapdb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class DataOutput2Test {

    //TODO more tests here for compability between DataIO.ByteArrayDataOutput and other DataInputs

    DataOutput2 out = new DataOutput2();

    DataInput2.ByteArray in(){
        return new DataInput2.ByteArray(out.buf);
    }

    @Test
    public void testWriteFloat() throws Exception {
        float f = 12.1239012093e-19F;
        out.writeFloat(f);
        DataInput2.ByteArray in = in();
        assertEquals(Float.floatToIntBits(f),Float.floatToIntBits(in.readFloat()));
        assertEquals(4,in.pos);
    }

    @Test
    public void testWriteDouble() throws Exception {
        double f = 12.123933423523012093e-199;
        out.writeDouble(f);
        DataInput2.ByteArray in = in();
        assertEquals(Double.doubleToLongBits(f),Double.doubleToLongBits(in.readDouble()));
        assertEquals(8,in.pos);
    }
}
