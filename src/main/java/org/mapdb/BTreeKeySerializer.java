package org.mapdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Custom serializer for BTreeMap keys.
 * Is used to take advantage of Delta Compression.
 *
 * @param <K>
 */
public abstract class BTreeKeySerializer<K>{
    public abstract void serialize(DataOutput out, int start, int end, Object[] keys) throws IOException;

    public abstract Object[] deserialize(DataInput in, int start, int end, int size) throws IOException;

    static final class BasicKeySerializer extends BTreeKeySerializer<Object> {

        protected final Serializer defaultSerializer;

        BasicKeySerializer(Serializer defaultSerializer) {
            this.defaultSerializer = defaultSerializer;
        }

        @Override
        public void serialize(DataOutput out, int start, int end, Object[] keys) throws IOException {
            for(int i = start;i<end;i++){
                defaultSerializer.serialize(out,keys[i]);
            }
        }

        @Override
        public Object[] deserialize(DataInput in, int start, int end, int size) throws IOException{
            Object[] ret = new Object[size];
            for(int i=start; i<end; i++){
                ret[i] = defaultSerializer.deserialize(in,-1);
            }
            return ret;
        }
    }


    public static final  BTreeKeySerializer<Long> ZERO_OR_POSITIVE_LONG = new BTreeKeySerializer<Long>() {
        @Override
        public void serialize(DataOutput out, int start, int end, Object[] keys) throws IOException {
            if(start>=end) return;
//            System.out.println(start+" - "+end+" - "+Arrays.toString(keys));
            long prev = (Long)keys[start];
            Utils.packLong(out,prev);
            for(int i=start+1;i<end;i++){
                long curr = (Long)keys[i];
                Utils.packLong(out, curr-prev);
                prev = curr;
            }
        }

        @Override
        public Object[] deserialize(DataInput in, int start, int end, int size) throws IOException {
            Object[] ret = new Long[size];
            long prev = 0 ;
            for(int i = start; i<end; i++){
                ret[i] = prev = prev + Utils.unpackLong(in);
            }
            return ret;
        }
    };

    public static final  BTreeKeySerializer<Integer> ZERO_OR_POSITIVE_INT = new BTreeKeySerializer<Integer>() {
        @Override
        public void serialize(DataOutput out, int start, int end, Object[] keys) throws IOException {
            if(start>=end) return;
//            System.out.println(start+" - "+end+" - "+Arrays.toString(keys));
            int prev = (Integer)keys[start];
            Utils.packLong(out,prev);
            for(int i=start+1;i<end;i++){
                int curr = (Integer)keys[i];
                Utils.packInt(out, curr-prev);
                prev = curr;
            }
        }

        @Override
        public Object[] deserialize(DataInput in, int start, int end, int size) throws IOException {
            Object[] ret = new Integer[size];
            int prev = 0 ;
            for(int i = start; i<end; i++){
                ret[i] = prev = prev + Utils.unpackInt(in);
            }
            return ret;
        }
    };


    public static final  BTreeKeySerializer<String> STRING = new BTreeKeySerializer<String>() {

        @Override
        public void serialize(DataOutput out, int start, int end, Object[] keys) throws IOException {
            byte[] previous = null;
            for (int i = start; i < end; i++) {
                byte[] b = ((String) keys[i]).getBytes(Utils.UTF8);
                leadingValuePackWrite(out, b, previous, 0);
                previous = b;
            }
        }

        @Override
        public Object[] deserialize(DataInput in, int start, int end, int size) throws IOException {
            Object[] ret = new Object[size];
            byte[] previous = null;
            for (int i = start; i < end; i++) {
                byte[] b = leadingValuePackRead(in, previous, 0);
                if (b == null) continue;
                ret[i] = new String(b,Utils.UTF8);
                previous = b;
            }
            return ret;
        }
    };

    /**
     * Read previously written data
     *
     * @author Kevin Day
     */
    public static byte[] leadingValuePackRead(DataInput in, byte[] previous, int ignoreLeadingCount) throws IOException {
        int len = Utils.unpackInt(in) - 1;  // 0 indicates null
        if (len == -1)
            return null;

        int actualCommon = Utils.unpackInt(in);

        byte[] buf = new byte[len];

        if (previous == null) {
            actualCommon = 0;
        }


        if (actualCommon > 0) {
            in.readFully(buf, 0, ignoreLeadingCount);
            System.arraycopy(previous, ignoreLeadingCount, buf, ignoreLeadingCount, actualCommon - ignoreLeadingCount);
        }
        in.readFully(buf, actualCommon, len - actualCommon);
        return buf;
    }

    /**
     * This method is used for delta compression for keys.
     * Writes the contents of buf to the DataOutput out, with special encoding if
     * there are common leading bytes in the previous group stored by this compressor.
     *
     * @author Kevin Day
     */
    public static void leadingValuePackWrite(DataOutput out, byte[] buf, byte[] previous, int ignoreLeadingCount) throws IOException {
        if (buf == null) {
            Utils.packInt(out, 0);
            return;
        }

        int actualCommon = ignoreLeadingCount;

        if (previous != null) {
            int maxCommon = buf.length > previous.length ? previous.length : buf.length;

            if (maxCommon > Short.MAX_VALUE) maxCommon = Short.MAX_VALUE;

            for (; actualCommon < maxCommon; actualCommon++) {
                if (buf[actualCommon] != previous[actualCommon])
                    break;
            }
        }


        // there are enough common bytes to justify compression
        Utils.packInt(out, buf.length + 1);// store as +1, 0 indicates null
        Utils.packInt(out, actualCommon);
        out.write(buf, 0, ignoreLeadingCount);
        out.write(buf, actualCommon, buf.length - actualCommon);

    }
}
