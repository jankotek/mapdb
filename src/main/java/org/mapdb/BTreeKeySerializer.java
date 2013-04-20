package org.mapdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

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

    public static final Tuple2KeySerializer TUPLE2 = new Tuple2KeySerializer(null, null, null);

    public final  static class Tuple2KeySerializer<A,B> extends  BTreeKeySerializer<Fun.Tuple2<A,B>>{

        protected final Comparator<A> aComparator;
        protected final Serializer<A> aSerializer;
        protected final Serializer<B> bSerializer;

        public Tuple2KeySerializer(Comparator<A> aComparator,Serializer aSerializer, Serializer bSerializer){
            this.aComparator = aComparator;
            this.aSerializer = aSerializer;
            this.bSerializer = bSerializer;
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
    }

    public static final Tuple3KeySerializer TUPLE3 = new Tuple3KeySerializer(null, null, null, null, null);

    public static class Tuple3KeySerializer<A,B,C> extends  BTreeKeySerializer<Fun.Tuple3<A,B,C>>{

        protected final Comparator<A> aComparator;
        protected final Comparator<B> bComparator;
        protected final Serializer<A> aSerializer;
        protected final Serializer<B> bSerializer;
        protected final Serializer<C> cSerializer;

        public Tuple3KeySerializer(Comparator<A> aComparator, Comparator<B> bComparator,  Serializer aSerializer,
                                   Serializer bSerializer, Serializer cSerializer){
            this.aComparator = aComparator;
            this.bComparator = bComparator;
            this.aSerializer = aSerializer;
            this.bSerializer = bSerializer;
            this.cSerializer = cSerializer;
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
    }

    public static final Tuple4KeySerializer TUPLE4 = new Tuple4KeySerializer(null, null, null, null, null, null, null);

    public static class Tuple4KeySerializer<A,B,C,D> extends  BTreeKeySerializer<Fun.Tuple4<A,B,C,D>>{

        protected final Comparator<A> aComparator;
        protected final Comparator<B> bComparator;
        protected final Comparator<C> cComparator;
        protected final Serializer<A> aSerializer;
        protected final Serializer<B> bSerializer;
        protected final Serializer<C> cSerializer;
        protected final Serializer<D> dSerializer;

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
    }
}