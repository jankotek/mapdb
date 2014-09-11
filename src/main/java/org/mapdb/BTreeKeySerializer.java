package org.mapdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.UUID;

/**
 * Custom serializer for BTreeMap keys which enables [Delta encoding](https://en.wikipedia.org/wiki/Delta_encoding).
 *
 * Keys in BTree Nodes are sorted, this enables number of tricks to save disk space.
 * For example for numbers we may store only difference between subsequent numbers, for string we can only take suffix, etc...
 *
 * @param <KEY> type of key
 * @param <KEYS> type of object which holds multiple keys (
 */
public abstract class BTreeKeySerializer<KEY,KEYS>{


    /**
     * Serialize keys from single BTree Node.
     *
     * @param out output stream where to put ata
     * @param keys An object which represents keys
     *
     * @throws IOException
     */
    public abstract void serialize(DataOutput out, KEYS keys) throws IOException;

    /**
     * Deserializes keys for single BTree Node. To
     *
     * @param in input stream to read data from
     * @return an object which represents keys
     *
     * @throws IOException
     */
    public abstract KEYS deserialize(DataInput in, int nodeSize) throws IOException;


    public abstract int compare(KEYS keys, int pos1, int pos2);


    public abstract int compare(KEYS keys, int pos, KEY key);

    public boolean compareIsSmaller(KEYS keys, int pos, KEY key) {
        //TODO override in Strings and other implementations
        return compare(keys,pos,key)<0;
    }


    public abstract KEY getKey(KEYS keys, int pos);


    public static final BTreeKeySerializer BASIC = new BTreeKeySerializer.BasicKeySerializer(Serializer.BASIC, Fun.COMPARATOR);

    public abstract Comparator comparator();

    public abstract KEYS emptyKeys();

    public abstract int length(KEYS keys);

    /** expand keys array by one and put `newKey` at position `pos` */
    public abstract KEYS putKey(KEYS keys, int pos, KEY newKey);


    public abstract KEYS copyOfRange(KEYS keys, int from, int to);

    public abstract KEYS deleteKey(KEYS keys, int pos);

    /**
     * Find the first children node with a key equal or greater than the given key.
     * If all items are smaller it returns `keyser.length(keys)`
     */
    public int findChildren(final BTreeMap.BNode node, final Object key) {
        KEYS keys = (KEYS) node.keys;
        int keylen = length(keys);
        int left = 0;
        int right = keylen;

        int middle;

        // binary search
        for(;;) {
            middle = (left + right) / 2;
            if(middle==keylen)
                return middle+node.leftEdgeInc(); //null is positive infinitive
            if (compareIsSmaller(keys,middle, (KEY) key)) {
                left = middle + 1;
            } else {
                right = middle;
            }
            if (left >= right) {
                return  right+node.leftEdgeInc();
            }
        }
    }

    public int findChildren2(final BTreeMap.BNode node, final Object key) {
        KEYS keys = (KEYS) node.keys;
        int keylen = length(keys);

        int left = 0;
        int right = keylen;
        int comp;
        int middle;

        // binary search
        while (true) {
            middle = (left + right) / 2;
            if(middle==keylen)
                return -1-(middle+node.leftEdgeInc()); //null is positive infinitive
            comp = compare(keys, middle, (KEY) key);
            if(comp==0){
                //try one before last, in some cases it might be duplicate of last
                if(!node.isRightEdge() && middle==keylen-1 && middle>0
                        && compare(keys,middle-1,(KEY)key)==0){
                    middle--;
                }
                return middle+node.leftEdgeInc();
            } else if ( comp< 0) {
                left = middle +1;
            } else {
                right = middle;
            }
            if (left >= right) {
                return  -1-(right+node.leftEdgeInc());
            }
        }

    }


    public abstract KEYS arrayToKeys(Object[] keys);

    public Object[] keysToArray(KEYS keys) {
        Object[] ret = new Object[length(keys)];
        for (int i = 0; i <ret.length; i++) {
            ret[i] = getKey(keys,i);
        }
        return ret;
    }


    /**
     * Basic Key Serializer which just writes data without applying any compression.
     * Is used by default if no other Key Serializer is specified.
     */
    public static final class BasicKeySerializer extends BTreeKeySerializer<Object, Object[]> implements Serializable {

        private static final long serialVersionUID = 1654710710946309279L;

        protected final Serializer serializer;
        protected final Comparator comparator;

        public BasicKeySerializer(Serializer serializer, Comparator comparator) {
            this.serializer = serializer;
            this.comparator = comparator;
        }

        /** used for deserialization*/
        BasicKeySerializer(SerializerBase serializerBase, DataInput is, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            serializer = (Serializer) serializerBase.deserialize(is,objectStack);
            comparator = (Comparator) serializerBase.deserialize(is,objectStack);
        }

        @Override
        public void serialize(DataOutput out, Object[] keys) throws IOException {
            for(Object o:keys){
                serializer.serialize(out, o);
            }
        }

        @Override
        public Object[] deserialize(DataInput in, int nodeSize) throws IOException {
            Object[] keys = new Object[nodeSize];
            for(int i=0;i<keys.length;i++)
                keys[i] = serializer.deserialize(in,-1);
            return keys;
        }

        @Override
        public int compare(Object[] keys, int pos1, int pos2) {
            return comparator.compare(keys[pos1], keys[pos2]);

        }

        @Override
        public int compare(Object[] keys, int pos, Object key) {
            return comparator.compare(keys[pos],key);
        }


        @Override
        public Object getKey(Object[] keys, int pos) {
            return keys[pos];
        }

        @Override
        public Comparator comparator() {
            return comparator;
        }

        @Override
        public Object[] emptyKeys() {
            return new Object[0];
        }

        @Override
        public int length(Object[] keys) {
            return keys.length;
        }

        @Override
        public Object[] putKey(Object[] keys, int pos, Object newKey) {
            return BTreeMap.arrayPut(keys, pos, newKey);
        }

        @Override
        public Object[] arrayToKeys(Object[] keys) {
            return keys;
        }

        @Override
        public Object[] copyOfRange(Object[] keys, int from, int to) {
            return Arrays.copyOfRange(keys,from,to);
        }

        @Override
        public Object[] deleteKey(Object[] keys, int pos) {
            Object[] keys2 = new Object[keys.length-1];
            System.arraycopy(keys,0,keys2, 0, pos);
            System.arraycopy(keys, pos+1, keys2, pos, keys2.length-pos);
            return keys2;
        }
    }


    /**
     * Applies delta packing on {@code java.lang.Long}.
     * Difference between consequential numbers is also packed itself, so for small diffs it takes only single byte per
     * number.
     */
    public static final  BTreeKeySerializer LONG = new BTreeKeySerializer<Long,long[]>() {

        @Override
        public void serialize(DataOutput out, long[] keys) throws IOException {
            long prev = keys[0];
            DataIO.packLong(out, prev);
            for(int i=1;i<keys.length;i++){
                long curr = keys[i];
                DataIO.packLong(out, curr - prev);
                prev = curr;
            }
        }

        @Override
        public long[] deserialize(DataInput in, int nodeSize) throws IOException {
            long[] ret = new long[nodeSize];
            long prev = 0 ;
            for(int i = 0; i<nodeSize; i++){
                prev += DataIO.unpackLong(in);
                ret[i] = prev;
            }
            return ret;
        }

        @Override
        public int compare(long[] keys, int pos1, int pos2) {
            return Fun.compareLong(keys[pos1], keys[pos2]);
        }

        @Override
        public int compare(long[] keys, int pos, Long preDigestedKey) {
            return Fun.compareLong(keys[pos], preDigestedKey);
        }

        @Override
        public boolean compareIsSmaller(long[] keys, int pos, Long key) {
            return  keys[pos]<key;
        }

        @Override
        public Long getKey(long[] keys, int pos) {
            return new Long(keys[pos]);
        }

        @Override
        public Comparator comparator() {
            return Fun.COMPARATOR;
        }

        @Override
        public long[] emptyKeys() {
            return new long[0];
        }

        @Override
        public int length(long[] keys) {
            return keys.length;
        }

        @Override
        public long[] putKey(long[] keys, int pos, Long newKey) {
            return BTreeMap.arrayLongPut(keys, pos, newKey);
        }

        @Override
        public long[] copyOfRange(long[] keys, int from, int to) {
            return Arrays.copyOfRange(keys,from,to);
        }


        @Override
        public long[] arrayToKeys(Object[] keys) {
            long[] ret = new long[keys.length];
            for(int i=keys.length-1;i>=0;i--)
                ret[i] = (Long)keys[i];
            return ret;
        }


        @Override
        public long[] deleteKey(long[] keys, int pos) {
            long[] keys2 = new long[keys.length-1];
            System.arraycopy(keys,0,keys2, 0, pos);
            System.arraycopy(keys, pos+1, keys2, pos, keys2.length-pos);
            return keys2;
        }

        @Override
        public final int findChildren(final BTreeMap.BNode node, final Object key) {
            long[] keys = (long[]) node.keys;
            long key2 = (Long)key;

            int left = 0;
            int right = keys.length;

            int middle;

            // binary search
            for(;;) {
                middle = (left + right) / 2;
                if(middle==keys.length)
                    return middle+node.leftEdgeInc(); //null is positive infinitive
                if (keys[middle]<key2) {
                    left = middle + 1;
                } else {
                    right = middle;
                }
                if (left >= right) {
                    return  right+node.leftEdgeInc();
                }
            }
        }

        @Override
        public final int findChildren2(final BTreeMap.BNode node, final Object key) {
            long[] keys = (long[]) node.keys;
            long key2 = (Long)key;

            int left = 0;
            int right = keys.length;
            int middle;

            // binary search
            while (true) {
                middle = (left + right) / 2;
                if(middle==keys.length)
                    return -1-(middle+node.leftEdgeInc()); //null is positive infinitive

                if(keys[middle]==key2){
                    //try one before last, in some cases it might be duplicate of last
                    if(!node.isRightEdge() && middle==keys.length-1 && middle>0
                            && keys[middle-1]==key2){
                        middle--;
                    }
                    return middle+node.leftEdgeInc();
                } else if ( keys[middle]<key2) {
                    left = middle +1;
                } else {
                    right = middle;
                }
                if (left >= right) {
                    return  -1-(right+node.leftEdgeInc());
                }
            }
        }

    };

    /**
     * @deprecated use {@link org.mapdb.BTreeKeySerializer#LONG}
     */
    public static final BTreeKeySerializer ZERO_OR_POSITIVE_LONG = LONG;

    /**
     * Applies delta packing on {@code java.lang.Integer}.
     * Difference between consequential numbers is also packed itself, so for small diffs it takes only single byte per
     * number.
     */
    public static final  BTreeKeySerializer INTEGER = new BTreeKeySerializer<Integer,int[]>() {
        @Override
        public void serialize(DataOutput out, int[] keys) throws IOException {
            int prev = keys[0];
            DataIO.packInt(out, prev);
            for(int i=1;i<keys.length;i++){
                int curr = keys[i];
                DataIO.packInt(out, curr - prev);
                prev = curr;
            }
        }

        @Override
        public int[] deserialize(DataInput in, int nodeSize) throws IOException {
            int[] ret = new int[nodeSize];
            int prev = 0 ;
            for(int i = 0; i<nodeSize; i++){
                prev += DataIO.unpackInt(in);
                ret[i] = prev;
            }
            return ret;
        }

        @Override
        public int compare(int[] keys, int pos1, int pos2) {
            return Fun.compareInt(keys[pos1], keys[pos2]);
        }

        @Override
        public int compare(int[] keys, int pos, Integer preDigestedKey) {
            return Fun.compareInt(keys[pos], preDigestedKey);
        }

        @Override
        public boolean compareIsSmaller(int[] keys, int pos, Integer key) {
            return  keys[pos]<key;
        }


        @Override
        public Integer getKey(int[] keys, int pos) {
            return new Integer(keys[pos]);
        }

        @Override
        public Comparator comparator() {
            return Fun.COMPARATOR;
        }

        @Override
        public int[] emptyKeys() {
            return new int[0];
        }

        @Override
        public int length(int[] keys) {
            return keys.length;
        }

        @Override
        public int[] putKey(int[] keys, int pos, Integer newKey) {
            final int[] ret = Arrays.copyOf(keys,keys.length+1);
            if(pos<keys.length){
                System.arraycopy(keys,pos,ret,pos+1,keys.length-pos);
            }
            ret[pos] = newKey;
            return ret;
        }

        @Override
        public int[] copyOfRange(int[] keys, int from, int to) {
            return Arrays.copyOfRange(keys,from,to);
        }


        @Override
        public int[] arrayToKeys(Object[] keys) {
            int[] ret = new int[keys.length];
            for(int i=keys.length-1;i>=0;i--)
                ret[i] = (Integer)keys[i];
            return ret;
        }

        @Override
        public int[] deleteKey(int[] keys, int pos) {
            int[] keys2 = new int[keys.length-1];
            System.arraycopy(keys,0,keys2, 0, pos);
            System.arraycopy(keys, pos+1, keys2, pos, keys2.length-pos);
            return keys2;
        }

        @Override
        public final int findChildren(final BTreeMap.BNode node, final Object key) {
            int[] keys = (int[]) node.keys;
            int key2 = (Integer)key;
            int left = 0;
            int right = keys.length;

            int middle;

            // binary search
            for(;;) {
                middle = (left + right) / 2;
                if(middle==keys.length)
                    return middle+node.leftEdgeInc(); //null is positive infinitive
                if (keys[middle]<key2) {
                    left = middle + 1;
                } else {
                    right = middle;
                }
                if (left >= right) {
                    return  right+node.leftEdgeInc();
                }
            }
        }

        @Override
        public final int findChildren2(final BTreeMap.BNode node, final Object key) {
            int[] keys = (int[]) node.keys;
            int key2 = (Integer)key;

            int left = 0;
            int right = keys.length;
            int middle;

            // binary search
            while (true) {
                middle = (left + right) / 2;
                if(middle==keys.length)
                    return -1-(middle+node.leftEdgeInc()); //null is positive infinitive

                if(keys[middle]==key2){
                    //try one before last, in some cases it might be duplicate of last
                    if(!node.isRightEdge() && middle==keys.length-1 && middle>0
                            && keys[middle-1]==key2){
                        middle--;
                    }
                    return middle+node.leftEdgeInc();
                } else if ( keys[middle]<key2) {
                    left = middle +1;
                } else {
                    right = middle;
                }
                if (left >= right) {
                    return  -1-(right+node.leftEdgeInc());
                }
            }
        }
    };

    /**
     * @deprecated use {@link org.mapdb.BTreeKeySerializer#INTEGER}
     */
    public static final  BTreeKeySerializer ZERO_OR_POSITIVE_INT = INTEGER;

    /**
     * Applies delta packing on {@code java.lang.String}. This serializer splits consequent strings
     * to two parts: shared prefix and different suffix. Only suffix is than stored.
     */
    public static final  BTreeKeySerializer<String, byte[][]> STRING2 = new BTreeKeySerializer<String, byte[][]>() {

        @Override
        public void serialize(DataOutput out, byte[][] chars) throws IOException {
            byte[] previous = null;
            for (byte[] b:chars) {
                leadingValuePackWrite(out, b, previous, 0);
                previous = b;
            }
        }

        @Override
        public byte[][] deserialize(DataInput in, int nodeSize) throws IOException {
            byte[][] ret = new byte[nodeSize][];
            byte[] previous = null;
            for (int i = 0; i < nodeSize; i++) {
                byte[] b = leadingValuePackRead(in, previous, 0);
                if (b == null) continue;
                ret[i] = b;
                previous = b;
            }
            return ret;
        }

        /** compares two char arrays, has same contract as {@link String#compareTo(String)} */
        int compare(byte[] c1, byte[] c2){
            int end = (c1.length <= c2.length) ? c1.length : c2.length;
            int ret;
            for(int i=0;i<end;i++){
                if ((ret = c1[i] - c2[i]) != 0) {
                    return ret;
                }
            }
            return c1.length - c2.length;
        }


        /** compares char array and string, has same contract as {@link String#compareTo(String)} */
        int compare(byte[] c1, String c2){
            int end = Math.min(c1.length,c2.length());
            int ret;
            for(int i=0;i<end;i++){
                if ((ret = c1[i] - (byte)c2.charAt(i)) != 0) {
                    return ret;
                }
            }
            return c1.length - c2.length();
        }

        @Override
        public int compare(byte[][] chars, int pos1, int pos2) {
            return compare(chars[pos1],chars[pos2]);
        }

        @Override
        public int compare(byte[][] chars, int pos, String key) {
            return compare(chars[pos],key);
        }

        @Override
        public String getKey(byte[][] chars, int pos) {
            return new String(chars[pos]); //TODO call private constr, so no copy is made
        }

        @Override
        public Comparator comparator() {
            return Fun.COMPARATOR;
        }

        @Override
        public byte[][] emptyKeys() {
            return new byte[0][];
        }

        @Override
        public int length(byte[][] chars) {
            return chars.length;
        }

        @Override
        public byte[][] putKey(byte[][] keys, int pos, String newKey) {
            return (byte[][]) BTreeMap.arrayPut(keys, pos, newKey.getBytes());
        }

        @Override
        public byte[][] copyOfRange(byte[][] keys, int from, int to) {
            return Arrays.copyOfRange( keys,from,to);
        }


        @Override
        public byte[][] arrayToKeys(Object[] keys) {
            byte[][] ret = new byte[keys.length][];
            for(int i=keys.length-1;i>=0;i--)
                ret[i] = ((String)keys[i]).getBytes();
            return ret;
        }

        @Override
        public byte[][] deleteKey(byte[][] keys, int pos) {
            byte[][] keys2 = new byte[keys.length-1][];
            System.arraycopy(keys,0,keys2, 0, pos);
            System.arraycopy(keys, pos+1, keys2, pos, keys2.length-pos);
            return keys2;
        }
    };

    /**
     * Read previously written data from {@code leadingValuePackWrite()} method.
     *
     * author: Kevin Day
     */
    public static byte[] leadingValuePackRead(DataInput in, byte[] previous, int ignoreLeadingCount) throws IOException {
        int len = DataIO.unpackInt(in) - 1;  // 0 indicates null
        if (len == -1)
            return null;

        int actualCommon = DataIO.unpackInt(in);

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
     * author: Kevin Day
     */
    public static void leadingValuePackWrite(DataOutput out, byte[] buf, byte[] previous, int ignoreLeadingCount) throws IOException {
        if (buf == null) {
            DataIO.packInt(out, 0);
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
        DataIO.packInt(out, buf.length + 1);// store as +1, 0 indicates null
        DataIO.packInt(out, actualCommon);
        out.write(buf, 0, ignoreLeadingCount);
        out.write(buf, actualCommon, buf.length - actualCommon);

    }


    public final static class TupleKeySerializer extends BTreeKeySerializer<Fun.Tuple,Object[]> implements Serializable{

        private static final long serialVersionUID = 998929894238939892L;

        protected final int tsize;
        protected final Comparator[] comparators;
        protected final Serializer[] serializers;

        protected Comparator comparator;

        public TupleKeySerializer(int tsize, Comparator[] comparators, Serializer[] serializers) {
            if(tsize!=comparators.length ||tsize!=serializers.length){
                throw new IllegalArgumentException("array sizes do not match");
            }
            this.tsize = tsize;
            this.comparators = comparators;
            this.serializers = serializers;

            initComparator();
        }

        private void initComparator() {
            switch(tsize){
                case 2:comparator=new Fun.Tuple2Comparator(
                        comparators[0],comparators[1]);
                    break;
                case 3:comparator=new Fun.Tuple3Comparator(
                        comparators[0],comparators[1],comparators[2]);
                    break;
                case 4:comparator=new Fun.Tuple4Comparator(
                        comparators[0],comparators[1],comparators[2],comparators[3]);
                    break;
                case 5:comparator=new Fun.Tuple5Comparator(
                        comparators[0],comparators[1],comparators[2],comparators[3],comparators[4]);
                    break;
                case 6:comparator=new Fun.Tuple6Comparator(
                        comparators[0],comparators[1],comparators[2],comparators[3],comparators[4],comparators[5]);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported tuple size: "+tsize);
            }
        }

        /** used for deserialization, `extra` is to avoid argument collision */
        public TupleKeySerializer(SerializerBase serializerBase, DataInput is,
                                  SerializerBase.FastArrayList<Object>  objectStack) throws IOException {
            objectStack.add(this);
            tsize = DataIO.unpackInt(is);
            comparators = new Comparator[tsize];
            for(int i=0;i<tsize;i++){
                comparators[i] = (Comparator) serializerBase.deserialize(is,objectStack);
            }
            serializers = new Serializer[tsize];
            for(int i=0;i<tsize;i++){
                serializers[i] = (Serializer) serializerBase.deserialize(is,objectStack);
            }
            initComparator();
        }

        @Override
        public void serialize(DataOutput out, Object[] keys) throws IOException {

            int[] counts = new int[tsize-1];

            for(int i=0;i<keys.length;i+=tsize){
                for(int j=0;j<tsize-1;j++){
                    if(counts[j]==0){
                        Object orig = keys[i+j];
                        serializers[j].serialize(out,orig);
                        counts[j]=1;
                        while(i+j+counts[j]*tsize<keys.length &&
                                comparators[j].compare(orig,keys[i+j+counts[j]*tsize])==0){
                            counts[j]++;
                        }
                        DataIO.packInt(out,counts[j]);
                    }
                }
                //write last value from tuple
                serializers[serializers.length-1].serialize(out,keys[i+tsize-1]);
                //decrement all
                for(int j=counts.length-1;j>=0;j--){
                    counts[j]--;
                }
            }
        }

        @Override
        public Object[] deserialize(DataInput in, int nodeSize) throws IOException {
            Object[] ret = new Object[nodeSize*tsize];
            Object[] curr = new Object[tsize];
            int[] counts = new int[tsize-1];

            for(int i=0;i<ret.length;i+=tsize){
                for(int j=0;j<tsize-1;j++){
                    if(counts[j]==0){
                        curr[j] = serializers[j].deserialize(in,-1);
                        counts[j] = DataIO.unpackInt(in);
                    }
                }
                curr[tsize-1] = serializers[tsize-1].deserialize(in,-1);
                System.arraycopy(curr,0,ret,i,tsize);
                for(int j=counts.length-1;j>=0;j--){
                    counts[j]--;
                }
            }

            if(CC.PARANOID){
                for(int j:counts){
                    if(j!=0)
                        throw new AssertionError();
                }
            }
            return ret;

        }

        @Override
        public int compare(Object[] keys, int pos1, int pos2) {
            pos1 *=tsize;
            pos2 *=tsize;
            int res;
            for(Comparator c:comparators){
                res = c.compare(keys[pos1++],keys[pos2++]);
                if(res!=0)
                    return res;
            }
            return 0;
        }

        @Override
        public int compare(Object[] keys, int pos, Fun.Tuple tuple) {
            return -tuple.compare(comparators,keys,pos*tsize);
        }

        @Override
        public Fun.Tuple getKey(Object[] keys, int pos) {
            pos*=tsize;
            switch (tsize) {
                case 2: return new Fun.Tuple2(keys[pos++],keys[pos]);
                case 3: return new Fun.Tuple3(keys[pos++],keys[pos++],keys[pos]);
                case 4: return new Fun.Tuple4(keys[pos++],keys[pos++],keys[pos++],keys[pos]);
                case 5: return new Fun.Tuple5(keys[pos++],keys[pos++],keys[pos++],keys[pos++],keys[pos]);
                default: return new Fun.Tuple6(keys[pos++],keys[pos++],keys[pos++],keys[pos++],keys[pos++],keys[pos]);
            }
        }

        @Override
        public Comparator comparator() {
            return comparator;
        }

        @Override
        public Object[] emptyKeys() {
            return new Object[0];
        }

        @Override
        public int length(Object[] objects) {
            return objects.length/tsize;
        }

        @Override
        public Object[] putKey(Object[] keys, int pos, Fun.Tuple newKey) {
            pos*=tsize;
            Object[] ret = new Object[keys.length+tsize];
            System.arraycopy(keys, 0, ret, 0, pos);
            newKey.copyIntoArray(ret,pos);
            System.arraycopy(keys,pos,ret,pos+tsize,keys.length-pos);
            return ret;
        }

        @Override
        public Object[] arrayToKeys(Object[] keys) {
            Object[] ret = new Object[keys.length*tsize];
            int pos=0;
            for(Object o:keys){
                ((Fun.Tuple)o).copyIntoArray(ret,pos);
                pos+=tsize;
            }
            return ret;
        }

        @Override
        public Object[] copyOfRange(Object[] keys, int from, int to) {
            from*=tsize;
            to*=tsize;
            return Arrays.copyOfRange(keys,from,to);
        }

        @Override
        public Object[] deleteKey(Object[] keys, int pos) {
            pos*=tsize;
            Object[] ret = new Object[keys.length-tsize];
            System.arraycopy(keys,0,ret,0,pos);
            System.arraycopy(keys,pos+tsize,ret,pos,ret.length-pos);
            return ret;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TupleKeySerializer that = (TupleKeySerializer) o;

            if (tsize != that.tsize) return false;
            if (!Arrays.equals(comparators, that.comparators)) return false;
            if (!Arrays.equals(serializers, that.serializers)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = tsize;
            result = 31 * result + Arrays.hashCode(comparators);
            result = 31 * result + Arrays.hashCode(serializers);
            return result;
        }
    }

    /**
     * Tuple2 Serializer which uses Default Serializer from DB and expect values to implement {@code Comparable} interface.
     */
    public static final BTreeKeySerializer<Fun.Tuple2,Fun.Tuple2[]> TUPLE2 = new Tuple2KeySerializer(
            Fun.COMPARATOR_NULLABLE, Fun.COMPARATOR_NULLABLE,
            Serializer.BASIC,Serializer.BASIC);


    /**
     * Tuple3 Serializer which uses Default Serializer from DB and expect values to implement {@code Comparable} interface.
     */
    public static final BTreeKeySerializer TUPLE3 = new TupleKeySerializer(3,
            new Comparator[]{Fun.COMPARATOR_NULLABLE, Fun.COMPARATOR_NULLABLE, Fun.COMPARATOR_NULLABLE},
            new Serializer[]{Serializer.BASIC,Serializer.BASIC,Serializer.BASIC});
    /**
     * Tuple4 Serializer which uses Default Serializer from DB and expect values to implement {@code Comparable} interface.
     */
    public static final BTreeKeySerializer TUPLE4 = new TupleKeySerializer(4,
            new Comparator[]{Fun.COMPARATOR_NULLABLE, Fun.COMPARATOR_NULLABLE, Fun.COMPARATOR_NULLABLE,
                    Fun.COMPARATOR_NULLABLE},
            new Serializer[]{Serializer.BASIC,Serializer.BASIC,Serializer.BASIC,Serializer.BASIC});

    /**
     * Tuple5 Serializer which uses Default Serializer from DB and expect values to implement {@code Comparable} interface.
     */
    public static final BTreeKeySerializer TUPLE5 = new TupleKeySerializer(5,
            new Comparator[]{Fun.COMPARATOR_NULLABLE, Fun.COMPARATOR_NULLABLE, Fun.COMPARATOR_NULLABLE,
                    Fun.COMPARATOR_NULLABLE, Fun.COMPARATOR_NULLABLE},
            new Serializer[]{Serializer.BASIC,Serializer.BASIC,Serializer.BASIC,Serializer.BASIC,Serializer.BASIC});
    /**
     * Tuple6 Serializer which uses Default Serializer from DB and expect values to implement {@code Comparable} interface.
     */
    public static final BTreeKeySerializer TUPLE6 = new TupleKeySerializer(6,
            new Comparator[]{Fun.COMPARATOR_NULLABLE, Fun.COMPARATOR_NULLABLE, Fun.COMPARATOR_NULLABLE,
                    Fun.COMPARATOR_NULLABLE, Fun.COMPARATOR_NULLABLE, Fun.COMPARATOR_NULLABLE},
            new Serializer[]{Serializer.BASIC,Serializer.BASIC,Serializer.BASIC,Serializer.BASIC,Serializer.BASIC,Serializer.BASIC});

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
    public final  static class Tuple2KeySerializer<A,B> extends  BTreeKeySerializer<Fun.Tuple2<A,B>,Fun.Tuple2<A,B>[]> implements Serializable {

        private static final long serialVersionUID = 2183804367032891772L;
        protected final Comparator<A> aComparator;
        protected final Comparator<B> bComparator;
        protected final Serializer<A> aSerializer;
        protected final Serializer<B> bSerializer;
        protected final Comparator<Fun.Tuple2<A, B>> comparator;

        /**
         * Construct new Tuple2 Key Serializer. You may pass null for some value,
         * In that case 'default' value will be used, Comparable comparator and Default Serializer from DB.
         *
         * @param aComparator comparator used for first tuple value
         * @param bComparator comparator used for second tuple value
         * @param aSerializer serializer used for first tuple value
         * @param bSerializer serializer used for second tuple value
         */
        public Tuple2KeySerializer(Comparator<A> aComparator,Comparator<B> bComparator,Serializer<A> aSerializer, Serializer<B> bSerializer){
            this.aComparator = aComparator;
            this.bComparator = bComparator;
            this.aSerializer = aSerializer;
            this.bSerializer = bSerializer;

            this.comparator = new Fun.Tuple2Comparator<A,B>(aComparator,bComparator);
        }

        /** used for deserialization, `extra` is to avoid argument collision */
        Tuple2KeySerializer(SerializerBase serializerBase, DataInput is, SerializerBase.FastArrayList<Object> objectStack, int extra) throws IOException {
            objectStack.add(this);
            aComparator = (Comparator<A>) serializerBase.deserialize(is,objectStack);
            bComparator = (Comparator<B>) serializerBase.deserialize(is,objectStack);
            aSerializer = (Serializer<A>) serializerBase.deserialize(is,objectStack);
            bSerializer = (Serializer<B>) serializerBase.deserialize(is,objectStack);
            comparator = new Fun.Tuple2Comparator<A,B>(aComparator,bComparator);
        }


        @Override
        public void serialize(DataOutput out,  Fun.Tuple2<A,B>[] keys) throws IOException {
            int acount=0;
            for(int i=0;i<keys.length;i++){
                Fun.Tuple2<A,B> t = keys[i];
                if(acount==0){
                    //write new A
                    aSerializer.serialize(out,t.a);
                    //count how many A are following
                    acount=1;
                    while(i+acount<keys.length && aComparator.compare(t.a, ((Fun.Tuple2<A, B>) keys[i+acount]).a)==0){
                        acount++;
                    }
                    DataIO.packInt(out, acount);
                }
                bSerializer.serialize(out,t.b);

                acount--;
            }
        }

        @Override
        public Fun.Tuple2<A,B>[] deserialize(DataInput in, int nodeSize) throws IOException {
            Fun.Tuple2<A,B>[] ret = new Fun.Tuple2[nodeSize];
            A a = null;
            int acount = 0;

            for(int i=0;i<nodeSize;i++){
                if(acount==0){
                    //read new A
                    a = aSerializer.deserialize(in,-1);
                    acount = DataIO.unpackInt(in);
                }
                B b = bSerializer.deserialize(in,-1);
                ret[i]= Fun.t2(a,b);
                acount--;
            }
            assert(acount==0);

            return ret;
        }

        @Override
        public int compare(Fun.Tuple2<A, B>[] keys, int pos1, int pos2) {
            return comparator.compare(keys[pos1],keys[pos2]);
        }

        @Override
        public int compare(Fun.Tuple2<A, B>[] keys, int pos, Fun.Tuple2<A, B> preDigestedKey) {
            return comparator.compare(keys[pos], preDigestedKey);
        }

        @Override
        public Fun.Tuple2<A, B> getKey(Fun.Tuple2<A, B>[] keys, int pos) {
            return keys[pos];
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tuple2KeySerializer t = (Tuple2KeySerializer) o;

            return
                    Fun.eq(aComparator, t.aComparator) &&
                            Fun.eq(bComparator, t.bComparator) &&
                            Fun.eq(aSerializer, t.aSerializer) &&
                            Fun.eq(bSerializer, t.bSerializer);
        }

        @Override
        public int hashCode() {
            return aComparator.hashCode() + bComparator.hashCode() + aSerializer.hashCode() + bSerializer.hashCode();
        }

        @Override
        public Comparator comparator() {
            return comparator;
        }

        @Override
        public Fun.Tuple2[] emptyKeys() {
            return new Fun.Tuple2[0];
        }

        @Override
        public int length(Fun.Tuple2[] keys) {
            return keys.length;
        }

        @Override
        public Fun.Tuple2[] putKey(Fun.Tuple2[] keys, int pos, Fun.Tuple2 newKey) {
            final Fun.Tuple2[] ret = Arrays.copyOf(keys, keys.length+1);
            if(pos<keys.length){
                System.arraycopy(keys, pos, ret, pos+1, keys.length-pos);
            }
            ret[pos] = newKey;
            return ret;
        }

        @Override
        public Fun.Tuple2[] copyOfRange(Fun.Tuple2[] keys, int from, int to) {
            return Arrays.copyOfRange(keys,from,to);
        }


        @Override
        public Fun.Tuple2[] arrayToKeys(Object[] keys) {
            Fun.Tuple2[] ret = new Fun.Tuple2[keys.length];
            for(int i=ret.length-1;i>=0;i--){
                ret[i] = (Fun.Tuple2) keys[i];
            }
            return ret;
        }

        @Override
        public Fun.Tuple2<A, B>[] deleteKey(Fun.Tuple2<A, B>[] keys, int pos) {
            Fun.Tuple2<A, B>[] keys2 = new Fun.Tuple2[keys.length-1];
            System.arraycopy(keys,0,keys2, 0, pos);
            System.arraycopy(keys, pos+1, keys2, pos, keys2.length-pos);
            return keys2;
        }


    }



    public static final BTreeKeySerializer<java.util.UUID,long[]> UUID = new BTreeKeySerializer<java.util.UUID,long[]>() {

        @Override
        public void serialize(DataOutput out, long[] longs) throws IOException {
            for(long l:longs){
                out.writeLong(l);
            }
        }

        @Override
        public long[] deserialize(DataInput in, int nodeSize) throws IOException {
            long[] ret= new long[nodeSize<<1];
            for(int i=0;i<ret.length;i++){
                ret[i]=in.readLong();
            }
            return ret;
        }

        @Override
        public int compare(long[] longs, int pos1, int pos2) {
            pos1<<=1;
            pos2<<=1;
            int r = Fun.compareLong(longs[pos1++],longs[pos2++]);
            if(r!=0)
                return r;
            return Fun.compareLong(longs[pos1],longs[pos2]);
        }

        @Override
        public int compare(long[] longs, int pos, java.util.UUID uuid) {
            pos<<=1;
            int r = Fun.compareLong(longs[pos++],uuid.getMostSignificantBits());
            if(r!=0)
                return r;
            return Fun.compareLong(longs[pos],uuid.getLeastSignificantBits());
        }

        @Override
        public UUID getKey(long[] longs, int pos) {
            pos<<=1;
            return new UUID(longs[pos++],longs[pos]);
        }

        @Override
        public Comparator comparator() {
            return Fun.COMPARATOR;
        }

        @Override
        public long[] emptyKeys() {
            return new long[0];
        }

        @Override
        public int length(long[] longs) {
            return longs.length/2;
        }

        @Override
        public long[] putKey(long[] keys, int pos, UUID newKey) {
            pos <<= 1; //*2
            long[] ret = new long[keys.length+2];
            System.arraycopy(keys, 0, ret, 0, pos);
            ret[pos++] = newKey.getMostSignificantBits();
            ret[pos++] = newKey.getLeastSignificantBits();
            System.arraycopy(keys,pos-2,ret,pos,ret.length-pos);
            return ret;

        }

        @Override
        public long[] arrayToKeys(Object[] keys) {
            long[] ret = new long[keys.length<<1]; //*2
            int i=0;
            for(Object o:keys){
                java.util.UUID u = (java.util.UUID) o;
                ret[i++]=u.getMostSignificantBits();
                ret[i++]=u.getLeastSignificantBits();
            }
            return ret;
        }

        @Override
        public long[] copyOfRange(long[] longs, int from, int to) {
            return Arrays.copyOfRange(longs,from<<1,to<<1);
        }

        @Override
        public long[] deleteKey(long[] keys, int pos) {
            pos <<= 1; //*2
            long[] ret = new long[keys.length-2];
            System.arraycopy(keys,0,ret,0,pos);
            System.arraycopy(keys,pos+2,ret,pos,ret.length-pos);
            return ret;
        }
    };

    public static final class ByteArrayKeys{
        final int[] offset;
        final byte[] array;

        ByteArrayKeys(int[] offset, byte[] array) {
            this.offset = offset;
            this.array = array;

            assert(array.length==0 || array.length == offset[offset.length-1]);
        }
    }


    public static final BTreeKeySerializer<String,ByteArrayKeys> STRING_BYTE_ARRAY = new BTreeKeySerializer<String,ByteArrayKeys>() {
        @Override
        public void serialize(DataOutput out, ByteArrayKeys keys) throws IOException {
            int offset = 0;
            for(int o:keys.offset){
                DataIO.packInt(out,o-offset);
                offset = o;
            }

            out.write(keys.array);
        }

        @Override
        public ByteArrayKeys deserialize(DataInput in, int nodeSize) throws IOException {
            int[] offsets = new int[nodeSize];
            int old=0;
            for(int i=0;i<nodeSize;i++){
                old+= DataIO.unpackInt(in);
                offsets[i]=old;
            }

            byte[] bb = new byte[old];
            in.readFully(bb);
            return new ByteArrayKeys(offsets,bb);
        }

        @Override
        public int compare(ByteArrayKeys byteArrayKeys, int pos1, int pos2) {
            int start1 = pos1==0 ? 0 : byteArrayKeys.offset[pos1-1];
            int start2 = pos2==0 ? 0 : byteArrayKeys.offset[pos2-1];
            int len1 = byteArrayKeys.offset[pos1] - start1;
            int len2 = byteArrayKeys.offset[pos2] - start2;
            int len = Math.min(len1,len2);

            while(len-- != 0){
                byte b1 = byteArrayKeys.array[start1++];
                byte b2 = byteArrayKeys.array[start2++];
                if(b1!=b2){
                    return b1-b2;
                }
            }
            return len1 - len2;
        }

        @Override
        public int compare(ByteArrayKeys byteArrayKeys, int pos1, String string) {
            int strLen = string.length();
            int start1 = pos1==0 ? 0 : byteArrayKeys.offset[pos1-1];
            int start2 = 0;
            int len1 = byteArrayKeys.offset[pos1] - start1;
            int len = Math.min(len1,strLen);

            while(len-- != 0){
                byte b1 = byteArrayKeys.array[start1++];
                byte b2 = (byte) string.charAt(start2++);
                if(b1!=b2){
                    return b1-b2;
                }
            }
            return len1 - strLen;
        }



        private byte[] toBytes(String string) {
            byte[] ret = new byte[string.length()];
            for(int i=ret.length-1;i!=-1;i--){
                ret[i] = (byte) string.charAt(i);
            }
            return ret;
        }

        @Override
        public String getKey(ByteArrayKeys byteArrayKeys, int pos) {
            int from =  pos==0 ? 0 : byteArrayKeys.offset[pos-1];
            int to =  byteArrayKeys.offset[pos];
            byte[] ret =  Arrays.copyOfRange(byteArrayKeys.array, from, to);
            return toString(ret);
        }

        private String toString(byte[] ret) {
            StringBuilder sb = new StringBuilder(ret.length);
            for(byte b:ret){
                sb.append((char)b);
            }
            return sb.toString();
        }

        @Override
        public Comparator comparator() {
            return Fun.COMPARATOR;
        }

        @Override
        public ByteArrayKeys emptyKeys() {
            return new ByteArrayKeys(new int[0], new byte[0]);
        }

        @Override
        public int length(ByteArrayKeys byteArrayKeys) {
            return byteArrayKeys.offset.length;
        }

        @Override
        public ByteArrayKeys putKey(ByteArrayKeys byteArrayKeys, int pos, String string) {
            byte[] newKey = toBytes(string);
            byte[] bb = new byte[byteArrayKeys.array.length+ newKey.length];
            int split1 = pos==0? 0: byteArrayKeys.offset[pos-1];
            System.arraycopy(byteArrayKeys.array,0,bb,0,split1);
            System.arraycopy(newKey,0,bb,split1,newKey.length);
            System.arraycopy(byteArrayKeys.array,split1,bb,split1+newKey.length,byteArrayKeys.array.length-split1);

            int[] offsets = new int[byteArrayKeys.offset.length+1];

            int plus = 0;
            int plusI = 0;
            for(int i=0;i<byteArrayKeys.offset.length;i++){
                if(i==pos){
                    //skip one item and increase space
                    plus = newKey.length;
                    plusI = 1;

                }
                offsets[i+plusI] = byteArrayKeys.offset[i] + plus;
            }
            offsets[pos] = split1+newKey.length;

            return new ByteArrayKeys(offsets,bb);
        }

        @Override
        public ByteArrayKeys arrayToKeys(Object[] keys) {
            //fill offsets
            int[] offsets = new int[keys.length];

            int old=0;
            for(int i=0;i<keys.length;i++){
                String b = (String) keys[i];
                old+=b.length();
                offsets[i]=old;
            }

            //fill large array
            byte[] bb = new byte[old];
            old=0;
            for(int i=0;i<keys.length;i++){
                int curr = offsets[i];
                String str = (String) keys[i];
                System.arraycopy(toBytes(str), 0, bb, old, curr - old);
                old=curr;
            }

            return new ByteArrayKeys(offsets,bb);
        }

        @Override
        public ByteArrayKeys copyOfRange(ByteArrayKeys byteArrayKeys, int from, int to) {
            int start = from==0? 0: byteArrayKeys.offset[from-1];
            int end = to==0? 0: byteArrayKeys.offset[to-1];
            byte[] bb = Arrays.copyOfRange(byteArrayKeys.array,start,end);

            int[] offsets = new int[to-from];
            for(int i=0;i<offsets.length;i++){
                offsets[i] = byteArrayKeys.offset[i+from] - start;
            }

            return new ByteArrayKeys(offsets,bb);
        }

        @Override
        public ByteArrayKeys deleteKey(ByteArrayKeys byteArrayKeys, int pos) {
            int split = pos==0? 0: byteArrayKeys.offset[pos-1];
            int next = byteArrayKeys.offset[pos];

            byte[] bb = new byte[byteArrayKeys.array.length - (next-split)];
            int[] offsets = new  int[byteArrayKeys.offset.length - 1];

            System.arraycopy(byteArrayKeys.array,0,bb,0,split);
            System.arraycopy(byteArrayKeys.array,next,bb,split,byteArrayKeys.array.length-next);

            int minus=0;
            int plusI=0;
            for(int i=0;i<offsets.length;i++){
                if(i==pos){
                    //skip current item and normalize offsets
                    plusI=1;
                    minus = next-split;
                }
                offsets[i] = byteArrayKeys.offset[i+plusI] - minus;
            }
            return new ByteArrayKeys(offsets,bb);
        }
    };

    public static final BTreeKeySerializer<String,ByteArrayKeys> STRING = STRING_BYTE_ARRAY;

}