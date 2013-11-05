package org.mapdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Custom serializer for BTreeMap keys which enables <a href='https://en.wikipedia.org/wiki/Delta_encoding'>Delta encoding</a>.
 * <p/>
 * Keys in BTree Nodes are sorted, this enables number of tricks to save disk space.
 * For example for numbers we may store only difference between subsequent numbers, for string we can only take suffix, etc...
 *
 * @param <K> type of key
 */
public abstract class BTreeKeySerializer<K>{

    /**
     * Serialize keys from single BTree Node.
     *
     * @param out output stream where to put ata
     * @param start where data start in array. Before this index all keys are null
     * @param end where data ends in array (exclusive). From this index all keys are null
     * @param keys array of keys for single BTree Node
     *
     * @throws IOException
     */
    public abstract void serialize(DataOutput out, int start, int end, Object[] keys) throws IOException;

    /**
     * Deserializes keys for single BTree Node. To
     *
     * @param in input stream to read data from
     * @param start where data start in array. Before this index all keys are null
     * @param end where data ends in array (exclusive). From this index all keys are null
     * @param size size of array which should be returned
     * @return array of keys for single BTree Node
     *
     * @throws IOException
     */
    public abstract Object[] deserialize(DataInput in, int start, int end, int size) throws IOException;


    public static final BTreeKeySerializer BASIC = new BTreeKeySerializer.BasicKeySerializer(Serializer.BASIC);

    /**
     * Basic Key Serializer which just writes data without applying any compression.
     * Is used by default if no other Key Serializer is specified.
     */
    public static final class BasicKeySerializer extends BTreeKeySerializer<Object> implements Serializable {

        protected final Serializer defaultSerializer;

        public BasicKeySerializer(Serializer defaultSerializer) {
            this.defaultSerializer = defaultSerializer;
        }

        /** used for deserialization*/
        protected BasicKeySerializer(SerializerBase serializerBase, DataInput is, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            defaultSerializer = (Serializer) serializerBase.deserialize(is,objectStack);
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


    /**
     * Applies delta packing on {@code java.lang.Long}. All keys must be non negative.
     * Difference between consequential numbers is also packed itself, so for small diffs it takes only single byte per
     * number.
     */
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

    /**
     * Applies delta packing on {@code java.lang.Integer}. All keys must be non negative.
     * Difference between consequential numbers is also packed itself, so for small diffs it takes only single byte per
     * number.
     */
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


    /**
     * Applies delta packing on {@code java.lang.String}. This serializer splits consequent strings
     * to two parts: shared prefix and different suffix. Only suffix is than stored.
     */
    public static final  BTreeKeySerializer<String> STRING = new BTreeKeySerializer<String>() {

        @Override
        public void serialize(DataOutput out, int start, int end, Object[] keys) throws IOException {
            byte[] previous = null;
            for (int i = start; i < end; i++) {
                byte[] b = ((String) keys[i]).getBytes(Utils.UTF8_CHARSET);
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
                ret[i] = new String(b,Utils.UTF8_CHARSET);
                previous = b;
            }
            return ret;
        }
    };

    /**
     * Read previously written data from {@code leadingValuePackWrite()} method.
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

    /**
     * Tuple2 Serializer which uses Default Serializer from DB and expect values to implement {@code Comparable} interface.
     */
    public static final Tuple2KeySerializer TUPLE2 = new Tuple2KeySerializer(null, null, null);

    /**
     * Applies delta compression on array of tuple. First tuple value may be shared between consequentive tuples, so only
     * first occurrence is serialized. An example:
     *
     * <pre>
     *     Value            Serialized as
     *     -------------------------
     *     Tuple(1, 1)       1, 1
     *     Tuple(1, 2)          2
     *     Tuple(1, 3)          3
     *     Tuple(1, 4)          4
     * </pre>
     *
     * @param <A> first tuple value
     * @param <B> second tuple value
     */
    public final  static class Tuple2KeySerializer<A,B> extends  BTreeKeySerializer<Fun.Tuple2<A,B>> implements Serializable {

        protected final Comparator<A> aComparator;
        protected final Serializer<A> aSerializer;
        protected final Serializer<B> bSerializer;

        /**
         * Construct new Tuple2 Key Serializer. You may pass null for some value,
         * In that case 'default' value will be used, Comparable comparator and Default Serializer from DB.
         *
         * @param aComparator comparator used for first tuple value
         * @param aSerializer serializer used for first tuple value
         * @param bSerializer serializer used for second tuple value
         */
        public Tuple2KeySerializer(Comparator<A> aComparator,Serializer aSerializer, Serializer bSerializer){
            this.aComparator = aComparator;
            this.aSerializer = aSerializer;
            this.bSerializer = bSerializer;
        }

        /** used for deserialization, `extra` is to avoid argument collision */
        Tuple2KeySerializer(SerializerBase serializerBase, DataInput is, SerializerBase.FastArrayList<Object> objectStack, int extra) throws IOException {
            objectStack.add(this);
            aComparator = (Comparator<A>) serializerBase.deserialize(is,objectStack);
            aSerializer = (Serializer<A>) serializerBase.deserialize(is,objectStack);
            bSerializer = (Serializer<B>) serializerBase.deserialize(is,objectStack);
        }

        @Override
        public void serialize(DataOutput out, int start, int end, Object[] keys) throws IOException {
            int acount=0;
            for(int i=start;i<end;i++){
                Fun.Tuple2<A,B> t = (Fun.Tuple2<A, B>) keys[i];
                if(acount==0){
                    //write new A
                    aSerializer.serialize(out,t.a);
                    //count how many A are following
                    acount=1;
                    while(i+acount<end && aComparator.compare(t.a, ((Fun.Tuple2<A, B>) keys[i+acount]).a)==0){
                        acount++;
                    }
                    Utils.packInt(out,acount);
                }
                bSerializer.serialize(out,t.b);

                acount--;
            }
        }

        @Override
        public Object[] deserialize(DataInput in, int start, int end, int size) throws IOException {
            Object[] ret = new Object[size];
            A a = null;
            int acount = 0;

            for(int i=start;i<end;i++){
                if(acount==0){
                    //read new A
                    a = aSerializer.deserialize(in,-1);
                    acount = Utils.unpackInt(in);
                }
                B b = bSerializer.deserialize(in,-1);
                ret[i]= Fun.t2(a,b);
                acount--;
            }
            if(acount!=0) throw new InternalError();

            return ret;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tuple2KeySerializer that = (Tuple2KeySerializer) o;

            if (aComparator != null ? !aComparator.equals(that.aComparator) : that.aComparator != null) return false;
            if (aSerializer != null ? !aSerializer.equals(that.aSerializer) : that.aSerializer != null) return false;
            if (bSerializer != null ? !bSerializer.equals(that.bSerializer) : that.bSerializer != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = aComparator != null ? aComparator.hashCode() : 0;
            result = 31 * result + (aSerializer != null ? aSerializer.hashCode() : 0);
            result = 31 * result + (bSerializer != null ? bSerializer.hashCode() : 0);
            return result;
        }
    }

    /**
     * Tuple3 Serializer which uses Default Serializer from DB and expect values to implement {@code Comparable} interface.
     */
    public static final Tuple3KeySerializer TUPLE3 = new Tuple3KeySerializer(null, null, null, null, null);

    /**
     * Applies delta compression on array of tuple. First and second tuple value may be shared between consequentive tuples, so only
     * first occurrence is serialized. An example:
     *
     * <pre>
     *     Value            Serialized as
     *     ----------------------------
     *     Tuple(1, 2, 1)       1, 2, 1
     *     Tuple(1, 2, 2)             2
     *     Tuple(1, 3, 3)          3, 3
     *     Tuple(1, 3, 4)             4
     * </pre>
     *
     * @param <A> first tuple value
     * @param <B> second tuple value
     * @param <C> third tuple value
     */
    public static class Tuple3KeySerializer<A,B,C> extends  BTreeKeySerializer<Fun.Tuple3<A,B,C>> implements Serializable {

        protected final Comparator<A> aComparator;
        protected final Comparator<B> bComparator;
        protected final Serializer<A> aSerializer;
        protected final Serializer<B> bSerializer;
        protected final Serializer<C> cSerializer;

        /**
         * Construct new Tuple3 Key Serializer. You may pass null for some value,
         * In that case 'default' value will be used, Comparable comparator and Default Serializer from DB.
         *
         * @param aComparator comparator used for first tuple value
         * @param bComparator comparator used for second tuple value
         * @param aSerializer serializer used for first tuple value
         * @param bSerializer serializer used for second tuple value
         * @param cSerializer serializer used for third tuple value
         */
        public Tuple3KeySerializer(Comparator<A> aComparator, Comparator<B> bComparator,  Serializer aSerializer,
                                   Serializer bSerializer, Serializer cSerializer){
            this.aComparator = aComparator;
            this.bComparator = bComparator;
            this.aSerializer = aSerializer;
            this.bSerializer = bSerializer;
            this.cSerializer = cSerializer;
        }

        /** used for deserialization */
        Tuple3KeySerializer(SerializerBase serializerBase, DataInput is, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            aComparator = (Comparator<A>) serializerBase.deserialize(is,objectStack);
            bComparator = (Comparator<B>) serializerBase.deserialize(is,objectStack);
            aSerializer = (Serializer<A>) serializerBase.deserialize(is,objectStack);
            bSerializer = (Serializer<B>) serializerBase.deserialize(is,objectStack);
            cSerializer = (Serializer<C>) serializerBase.deserialize(is,objectStack);
        }


        @Override
        public void serialize(DataOutput out, int start, int end, Object[] keys) throws IOException {
            int acount=0;
            int bcount=0;
            for(int i=start;i<end;i++){
                Fun.Tuple3<A,B,C> t = (Fun.Tuple3<A, B,C>) keys[i];
                if(acount==0){
                    //write new A
                    aSerializer.serialize(out,t.a);
                    //count how many A are following
                    acount=1;
                    while(i+acount<end && aComparator.compare(t.a, ((Fun.Tuple3<A, B, C>) keys[i+acount]).a)==0){
                        acount++;
                    }
                    Utils.packInt(out,acount);
                }
                if(bcount==0){
                    //write new B
                    bSerializer.serialize(out,t.b);
                    //count how many B are following
                    bcount=1;
                    while(i+bcount<end && bComparator.compare(t.b, ((Fun.Tuple3<A, B,C>) keys[i+bcount]).b)==0){
                        bcount++;
                    }
                    Utils.packInt(out,bcount);
                }


                cSerializer.serialize(out,t.c);

                acount--;
                bcount--;
            }


        }

        @Override
        public Object[] deserialize(DataInput in, int start, int end, int size) throws IOException {
            Object[] ret = new Object[size];
            A a = null;
            int acount = 0;
            B b = null;
            int bcount = 0;

            for(int i=start;i<end;i++){
                if(acount==0){
                    //read new A
                    a = aSerializer.deserialize(in,-1);
                    acount = Utils.unpackInt(in);
                }
                if(bcount==0){
                    //read new B
                    b = bSerializer.deserialize(in,-1);
                    bcount = Utils.unpackInt(in);
                }
                C c = cSerializer.deserialize(in,-1);
                ret[i]= Fun.t3(a, b, c);
                acount--;
                bcount--;
            }
            if(acount!=0) throw new InternalError();
            if(bcount!=0) throw new InternalError();

            return ret;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tuple3KeySerializer that = (Tuple3KeySerializer) o;

            if (aComparator != null ? !aComparator.equals(that.aComparator) : that.aComparator != null) return false;
            if (aSerializer != null ? !aSerializer.equals(that.aSerializer) : that.aSerializer != null) return false;
            if (bComparator != null ? !bComparator.equals(that.bComparator) : that.bComparator != null) return false;
            if (bSerializer != null ? !bSerializer.equals(that.bSerializer) : that.bSerializer != null) return false;
            if (cSerializer != null ? !cSerializer.equals(that.cSerializer) : that.cSerializer != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = aComparator != null ? aComparator.hashCode() : 0;
            result = 31 * result + (bComparator != null ? bComparator.hashCode() : 0);
            result = 31 * result + (aSerializer != null ? aSerializer.hashCode() : 0);
            result = 31 * result + (bSerializer != null ? bSerializer.hashCode() : 0);
            result = 31 * result + (cSerializer != null ? cSerializer.hashCode() : 0);
            return result;
        }
    }

    /**
     * Tuple4 Serializer which uses Default Serializer from DB and expect values to implement {@code Comparable} interface.
     */
    public static final Tuple4KeySerializer TUPLE4 = new Tuple4KeySerializer(null, null, null, null, null, null, null);


    /**
     * Applies delta compression on array of tuple. First, second and third tuple value may be shared between consequential tuples,
     * so only first occurrence is serialized. An example:
     *
     * <pre>
     *     Value                Serialized as
     *     ----------------------------------
     *     Tuple(1, 2, 1, 1)       1, 2, 1, 1
     *     Tuple(1, 2, 1, 2)                2
     *     Tuple(1, 3, 3, 3)          3, 3, 3
     *     Tuple(1, 3, 4, 4)             4, 4
     * </pre>
     *
     * @param <A> first tuple value
     * @param <B> second tuple value
     * @param <C> third tuple value
     */
    public static class Tuple4KeySerializer<A,B,C,D> extends  BTreeKeySerializer<Fun.Tuple4<A,B,C,D>> implements Serializable {

        protected final Comparator<A> aComparator;
        protected final Comparator<B> bComparator;
        protected final Comparator<C> cComparator;
        protected final Serializer<A> aSerializer;
        protected final Serializer<B> bSerializer;
        protected final Serializer<C> cSerializer;
        protected final Serializer<D> dSerializer;

        /**
         * Construct new Tuple4 Key Serializer. You may pass null for some value,
         * In that case 'default' value will be used, Comparable comparator and Default Serializer from DB.
         *
         * @param aComparator comparator used for first tuple value
         * @param bComparator comparator used for second tuple value
         * @param cComparator comparator used for third tuple value*
         * @param aSerializer serializer used for first tuple value
         * @param bSerializer serializer used for second tuple value
         * @param cSerializer serializer used for third tuple value
         * @param dSerializer serializer used for fourth tuple value
         */
        public Tuple4KeySerializer(Comparator<A> aComparator, Comparator<B> bComparator, Comparator<C> cComparator,
                                   Serializer aSerializer, Serializer bSerializer, Serializer cSerializer, Serializer dSerializer){
            this.aComparator = aComparator;
            this.bComparator = bComparator;
            this.cComparator = cComparator;
            this.aSerializer = aSerializer;
            this.bSerializer = bSerializer;
            this.cSerializer = cSerializer;
            this.dSerializer = dSerializer;
        }

        /** used for deserialization */
        Tuple4KeySerializer(SerializerBase serializerBase, DataInput is, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            aComparator = (Comparator<A>) serializerBase.deserialize(is,objectStack);
            bComparator = (Comparator<B>) serializerBase.deserialize(is,objectStack);
            cComparator = (Comparator<C>) serializerBase.deserialize(is,objectStack);
            aSerializer = (Serializer<A>) serializerBase.deserialize(is,objectStack);
            bSerializer = (Serializer<B>) serializerBase.deserialize(is,objectStack);
            cSerializer = (Serializer<C>) serializerBase.deserialize(is,objectStack);
            dSerializer = (Serializer<D>) serializerBase.deserialize(is,objectStack);
        }


        @Override
        public void serialize(DataOutput out, int start, int end, Object[] keys) throws IOException {
            int acount=0;
            int bcount=0;
            int ccount=0;
            for(int i=start;i<end;i++){
                Fun.Tuple4<A,B,C,D> t = (Fun.Tuple4<A, B,C,D>) keys[i];
                if(acount==0){
                    //write new A
                    aSerializer.serialize(out,t.a);
                    //count how many A are following
                    acount=1;
                    while(i+acount<end && aComparator.compare(t.a, ((Fun.Tuple4<A, B, C,D>) keys[i+acount]).a)==0){
                        acount++;
                    }
                    Utils.packInt(out,acount);
                }
                if(bcount==0){
                    //write new B
                    bSerializer.serialize(out,t.b);
                    //count how many B are following
                    bcount=1;
                    while(i+bcount<end && bComparator.compare(t.b, ((Fun.Tuple4<A, B,C,D>) keys[i+bcount]).b)==0){
                        bcount++;
                    }
                    Utils.packInt(out,bcount);
                }
                if(ccount==0){
                    //write new C
                    cSerializer.serialize(out,t.c);
                    //count how many C are following
                    ccount=1;
                    while(i+ccount<end && cComparator.compare(t.c, ((Fun.Tuple4<A, B,C,D>) keys[i+ccount]).c)==0){
                        ccount++;
                    }
                    Utils.packInt(out,ccount);
                }


                dSerializer.serialize(out,t.d);

                acount--;
                bcount--;
                ccount--;
            }
        }

        @Override
        public Object[] deserialize(DataInput in, int start, int end, int size) throws IOException {
            Object[] ret = new Object[size];
            A a = null;
            int acount = 0;
            B b = null;
            int bcount = 0;
            C c = null;
            int ccount = 0;


            for(int i=start;i<end;i++){
                if(acount==0){
                    //read new A
                    a = aSerializer.deserialize(in,-1);
                    acount = Utils.unpackInt(in);
                }
                if(bcount==0){
                    //read new B
                    b = bSerializer.deserialize(in,-1);
                    bcount = Utils.unpackInt(in);
                }
                if(ccount==0){
                    //read new C
                    c = cSerializer.deserialize(in,-1);
                    ccount = Utils.unpackInt(in);
                }

                D d = dSerializer.deserialize(in,-1);
                ret[i]= Fun.t4(a, b, c, d);
                acount--;
                bcount--;
                ccount--;
            }
            if(acount!=0) throw new InternalError();
            if(bcount!=0) throw new InternalError();
            if(ccount!=0) throw new InternalError();

            return ret;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tuple4KeySerializer that = (Tuple4KeySerializer) o;

            if (aComparator != null ? !aComparator.equals(that.aComparator) : that.aComparator != null) return false;
            if (aSerializer != null ? !aSerializer.equals(that.aSerializer) : that.aSerializer != null) return false;
            if (bComparator != null ? !bComparator.equals(that.bComparator) : that.bComparator != null) return false;
            if (bSerializer != null ? !bSerializer.equals(that.bSerializer) : that.bSerializer != null) return false;
            if (cComparator != null ? !cComparator.equals(that.cComparator) : that.cComparator != null) return false;
            if (cSerializer != null ? !cSerializer.equals(that.cSerializer) : that.cSerializer != null) return false;
            if (dSerializer != null ? !dSerializer.equals(that.dSerializer) : that.dSerializer != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = aComparator != null ? aComparator.hashCode() : 0;
            result = 31 * result + (bComparator != null ? bComparator.hashCode() : 0);
            result = 31 * result + (cComparator != null ? cComparator.hashCode() : 0);
            result = 31 * result + (aSerializer != null ? aSerializer.hashCode() : 0);
            result = 31 * result + (bSerializer != null ? bSerializer.hashCode() : 0);
            result = 31 * result + (cSerializer != null ? cSerializer.hashCode() : 0);
            result = 31 * result + (dSerializer != null ? dSerializer.hashCode() : 0);
            return result;
        }
    }

}