package net.kotek.jdbm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.logging.Logger;

/**
 * Various IO related utilities
 */
final public class JdbmUtil {

    static final Logger LOG = Logger.getLogger("JDBM");

    public static final Comparator<Comparable> COMPARABLE_COMPARATOR = new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
            return o1.compareTo(o2);
        }
    };

    public static final Comparator<Comparable> COMPARABLE_COMPARATOR_WITH_NULLS = new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
            return o1 == null && o2 != null ? -1 : (o1 != null && o2 == null ? 1 : o1.compareTo(o2));
        }
    };


    public static final String EMPTY_STRING = "";
    public static final String UTF8 = "UTF8";


    /**
     * Pack  non-negative long into output stream.
     * It will occupy 1-10 bytes depending on value (lower values occupy smaller space)
     *
     * @param os
     * @param value
     * @throws java.io.IOException
     */
    static public void packLong(DataOutput os, long value) throws IOException {

        if (CC.ASSERT && value < 0) {
            throw new IllegalArgumentException("negative value: keys=" + value);
        }

        while ((value & ~0x7FL) != 0) {
            os.write((((int) value & 0x7F) | 0x80));
            value >>>= 7;
        }
        os.write((byte) value);
    }


    /**
     * Unpack positive long value from the input stream.
     *
     * @param is The input stream.
     * @return The long value.
     * @throws java.io.IOException
     */
    static public long unpackLong(DataInput is) throws IOException {

        long result = 0;
        for (int offset = 0; offset < 64; offset += 7) {
            long b = is.readUnsignedByte();
            result |= (b & 0x7F) << offset;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        if(CC.ASSERT) throw new Error("Malformed long.");
        else return Long.MIN_VALUE;
    }


    /**
     * Pack  non-negative long into output stream.
     * It will occupy 1-5 bytes depending on value (lower values occupy smaller space)
     *
     * @param os
     * @param value
     * @throws IOException
     */

    static public void packInt(DataOutput os, int value) throws IOException {
        if (CC.ASSERT && value < 0) {
            throw new IllegalArgumentException("negative value: keys=" + value);
        }

        while ((value & ~0x7F) != 0) {
            os.write(((value & 0x7F) | 0x80));
            value >>>= 7;
        }

        os.write((byte) value);
    }

    static public int unpackInt(DataInput is) throws IOException {
        for (int offset = 0, result = 0; offset < 32; offset += 7) {
            int b = is.readUnsignedByte();
            result |= (b & 0x7F) << offset;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        if(CC.ASSERT) throw new Error("Malformed int.");
        else return Integer.MIN_VALUE;

    }


    public static int longHash(final long key) {
        int h = (int)(key ^ (key >>> 32));
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h <<  15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h <<   3);
        h ^= (h >>>  6);
        h += (h <<   2) + (h << 14);
        return h ^ (h >>> 16);

    }

    /** clone value using serialization */
    public static <E> E clone(E value, Serializer<E> serializer){
        try{
            DataOutput2 out = new DataOutput2();
            serializer.serialize(out,value);
            DataInput2 in = new DataInput2(ByteBuffer.wrap(out.copyBytes()), 0);

            return serializer.deserialize(in,-1);
        }catch(IOException ee){
            throw new IOError(ee);
        }
    }

    /** expand array size by 1, and put value at given position. No items from original array are lost*/
    public static Object[] arrayPut(final Object[] array, final int pos, final Object value){
        final Object[] ret = Arrays.copyOf(array, array.length+1);
        if(pos<array.length){
            System.arraycopy(array, pos, ret, pos+1, array.length-pos);
        }
        ret[pos] = value;
        return ret;
    }

    public static long[] arrayLongPut(final long[] array, final int pos, final long value) {
        final long[] ret = Arrays.copyOf(array,array.length+1);
        if(pos<array.length){
            System.arraycopy(array,pos,ret,pos+1,array.length-pos);
        }
        ret[pos] = value;
        return ret;
    }

}
