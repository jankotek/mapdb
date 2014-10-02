package org.mapdb;

import java.io.*;
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

    /** expand keys array by one and put {@code newKey} at position {@code pos} */
    public abstract KEYS putKey(KEYS keys, int pos, KEY newKey);


    public abstract KEYS copyOfRange(KEYS keys, int from, int to);

    public abstract KEYS deleteKey(KEYS keys, int pos);

    /**
     * Find the first children node with a key equal or greater than the given key.
     * If all items are smaller it returns {@code keyser.length(keys)}
     */
    public int findChildren(final BTreeMap.BNode node, final Object key) {
        KEYS keys = (KEYS) node.keys;
        int keylen = length(keys);
        int left = 0;
        int right = keylen;

        int middle;
        //$DELAY$
        // binary search
        for(;;) {
            //$DELAY$
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
        //$DELAY$
        // binary search
        while (true) {
            //$DELAY$
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
        //$DELAY$
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
            if(serializer == null || comparator == null)
                throw new  NullPointerException();
            this.serializer = serializer;
            this.comparator = comparator;
        }

        /** used for deserialization*/
        BasicKeySerializer(SerializerBase serializerBase, DataInput is, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            serializer = (Serializer) serializerBase.deserialize(is,objectStack);
            comparator = (Comparator) serializerBase.deserialize(is,objectStack);
            if(serializer == null || comparator == null)
                throw new  NullPointerException();
        }

        @Override
        public void serialize(DataOutput out, Object[] keys) throws IOException {
            for(Object o:keys){
                //$DELAY$
                serializer.serialize(out, o);
            }
        }

        @Override
        public Object[] deserialize(DataInput in, int nodeSize) throws IOException {
            //$DELAY$
            Object[] keys = new Object[nodeSize];
            for(int i=0;i<keys.length;i++) {
                //$DELAY$
                keys[i] = serializer.deserialize(in, -1);
            }
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
            //$DELAY$
            System.arraycopy(keys, pos+1, keys2, pos, keys2.length-pos);
            //$DELAY$
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
                //$DELAY$
                DataIO.packLong(out, curr - prev);
                prev = curr;
            }
        }

        @Override
        public long[] deserialize(DataInput in, int nodeSize) throws IOException {
            long[] ret = new long[nodeSize];
            long prev = 0 ;
            for(int i = 0; i<nodeSize; i++){
                //$DELAY$
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
            for(int i=keys.length-1;i>=0;i--) {
                //$DELAY$
                ret[i] = (Long) keys[i];
            }
            return ret;
        }


        @Override
        public long[] deleteKey(long[] keys, int pos) {
            long[] keys2 = new long[keys.length-1];
            System.arraycopy(keys,0,keys2, 0, pos);
            //$DELAY$
            System.arraycopy(keys, pos+1, keys2, pos, keys2.length-pos);
            //$DELAY$
            return keys2;
        }

        @Override
        public final int findChildren(final BTreeMap.BNode node, final Object key) {
            long[] keys = (long[]) node.keys;
            long key2 = (Long)key;

            int left = 0;
            int right = keys.length;

            int middle;
            //$DELAY$
            // binary search
            for(;;) {
                //$DELAY$
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
            //$DELAY$
            // binary search
            while (true) {
                //$DELAY$
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
            //$DELAY$
            for(int i=1;i<keys.length;i++){
                int curr = keys[i];
                //$DELAY$
                DataIO.packInt(out, curr - prev);
                prev = curr;
            }
        }

        @Override
        public int[] deserialize(DataInput in, int nodeSize) throws IOException {
            int[] ret = new int[nodeSize];
            int prev = 0 ;
            for(int i = 0; i<nodeSize; i++){
                //$DELAY$
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
            //$DELAY$
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
                //$DELAY$
                ret[i] = (Integer)keys[i];
            return ret;
        }

        @Override
        public int[] deleteKey(int[] keys, int pos) {
            int[] keys2 = new int[keys.length-1];
            System.arraycopy(keys,0,keys2, 0, pos);
            //$DELAY$
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
            //$DELAY$
            // binary search
            for(;;) {
                //$DELAY$
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
            //$DELAY$
            // binary search
            while (true) {
                //$DELAY$
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


    public static final BTreeKeySerializer ARRAY2 = new ArrayKeySerializer(
            new Comparator[]{Fun.COMPARATOR,Fun.COMPARATOR},
            new Serializer[]{Serializer.BASIC, Serializer.BASIC}
    );

    public static final BTreeKeySerializer ARRAY3 = new ArrayKeySerializer(
            new Comparator[]{Fun.COMPARATOR,Fun.COMPARATOR,Fun.COMPARATOR},
            new Serializer[]{Serializer.BASIC, Serializer.BASIC, Serializer.BASIC}
    );

    public static final BTreeKeySerializer ARRAY4 = new ArrayKeySerializer(
            new Comparator[]{Fun.COMPARATOR,Fun.COMPARATOR,Fun.COMPARATOR,Fun.COMPARATOR},
            new Serializer[]{Serializer.BASIC, Serializer.BASIC, Serializer.BASIC, Serializer.BASIC}
    );


    public final static class ArrayKeySerializer extends BTreeKeySerializer<Object[],Object[]> implements Serializable{

        private static final long serialVersionUID = 998929894238939892L;

        protected final int tsize;
        protected final Comparator[] comparators;
        protected final Serializer[] serializers;

        protected final Comparator comparator;

        public ArrayKeySerializer(Comparator[] comparators, Serializer[] serializers) {
            if(comparators.length!=serializers.length){
                throw new IllegalArgumentException("array sizes do not match");
            }

            this.tsize = comparators.length;
            this.comparators = comparators;
            this.serializers = serializers;

            this.comparator = new Fun.ArrayComparator(comparators);
        }

        /** used for deserialization, extra is to avoid argument collision */
        public ArrayKeySerializer(SerializerBase serializerBase, DataInput is,
                                  SerializerBase.FastArrayList<Object> objectStack) throws IOException {
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

            this.comparator = new Fun.ArrayComparator(comparators);
        }

        @Override
        public void serialize(DataOutput out, Object[] keys) throws IOException {

            int[] counts = new int[tsize-1];
                //$DELAY$
            for(int i=0;i<keys.length;i+=tsize){
                for(int j=0;j<tsize-1;j++){
                    //$DELAY$
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
                //$DELAY$
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
            //$DELAY$
            for(int i=0;i<ret.length;i+=tsize){
                for(int j=0;j<tsize-1;j++){
                    if(counts[j]==0){
                        //$DELAY$
                        curr[j] = serializers[j].deserialize(in,-1);
                        counts[j] = DataIO.unpackInt(in);
                    }
                }
                curr[tsize-1] = serializers[tsize-1].deserialize(in,-1);
                System.arraycopy(curr,0,ret,i,tsize);
                //$DELAY$
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
            //$DELAY$
            for(Comparator c:comparators){
                //$DELAY$
                res = c.compare(keys[pos1++],keys[pos2++]);
                if(res!=0) {
                    return res;
                }
            }
            return 0;
        }

        @Override
        public int compare(Object[] keys, int pos, Object[] tuple) {
            pos*=tsize;
            int len = Math.min(tuple.length, tsize);
            int r;
            //$DELAY$
            for(int i=0;i<len;i++){
                //$DELAY$
                r = comparators[i].compare(keys[pos++],tuple[i]);
                if(r!=0)
                    return r;
            }
            return Fun.compareInt(tsize, tuple.length);
        }

        @Override
        public Object[] getKey(Object[] keys, int pos) {
            pos*=tsize;
            return Arrays.copyOfRange(keys,pos,pos+tsize);
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
        public Object[] putKey(Object[] keys, int pos, Object[] newKey) {
            if(CC.PARANOID && newKey.length!=tsize)
                throw new AssertionError();
            pos*=tsize;
            Object[] ret = new Object[keys.length+tsize];
            System.arraycopy(keys, 0, ret, 0, pos);
            //$DELAY$
            System.arraycopy(newKey,0,ret,pos,tsize);
            //$DELAY$
            System.arraycopy(keys,pos,ret,pos+tsize,keys.length-pos);
            return ret;
        }

        @Override
        public Object[] arrayToKeys(Object[] keys) {
            Object[] ret = new Object[keys.length*tsize];
            int pos=0;
            //$DELAY$
            for(Object o:keys){
                if(CC.PARANOID && ((Object[])o).length!=tsize)
                    throw new AssertionError();
                System.arraycopy(o,0,ret,pos,tsize);
                //$DELAY$
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
            //$DELAY$
            System.arraycopy(keys,pos+tsize,ret,pos,ret.length-pos);
            return ret;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ArrayKeySerializer that = (ArrayKeySerializer) o;
            //$DELAY$
            if (tsize != that.tsize) return false;
            if (!Arrays.equals(comparators, that.comparators)) return false;
            //$DELAY$
            return Arrays.equals(serializers, that.serializers);
        }

        @Override
        public int hashCode() {
            int result = tsize;
            result = 31 * result + Arrays.hashCode(comparators);
            result = 31 * result + Arrays.hashCode(serializers);
            return result;
        }
    }

    public static final BTreeKeySerializer<java.util.UUID,long[]> UUID = new BTreeKeySerializer<java.util.UUID,long[]>() {

        @Override
        public void serialize(DataOutput out, long[] longs) throws IOException {
            //$DELAY$
            for(long l:longs){
                out.writeLong(l);
            }
        }

        @Override
        public long[] deserialize(DataInput in, int nodeSize) throws IOException {
            long[] ret= new long[nodeSize<<1];
            //$DELAY$
            for(int i=0;i<ret.length;i++){
                ret[i]=in.readLong();
            }
            return ret;
        }

        @Override
        public int compare(long[] longs, int pos1, int pos2) {
            pos1<<=1;
            pos2<<=1;
            //$DELAY$
            int r = Fun.compareLong(longs[pos1++],longs[pos2++]);
            if(r!=0)
                return r;
            return Fun.compareLong(longs[pos1],longs[pos2]);
        }

        @Override
        public int compare(long[] longs, int pos, java.util.UUID uuid) {
            pos<<=1;
            //$DELAY$
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
            //$DELAY$
            ret[pos++] = newKey.getMostSignificantBits();
            ret[pos++] = newKey.getLeastSignificantBits();
            System.arraycopy(keys,pos-2,ret,pos,ret.length-pos);
            return ret;

        }

        @Override
        public long[] arrayToKeys(Object[] keys) {
            long[] ret = new long[keys.length<<1]; //*2
            int i=0;
            //$DELAY$
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
            //$DELAY$
            System.arraycopy(keys,pos+2,ret,pos,ret.length-pos);
            return ret;
        }
    };

    public interface StringArrayKeys {

        int commonPrefixLen();

        int length();

        BTreeKeySerializer.StringArrayKeys deleteKey(int pos);

        BTreeKeySerializer.StringArrayKeys copyOfRange(int from, int to);

        BTreeKeySerializer.StringArrayKeys putKey(int pos, String newKey);

        int compare(int pos1, String string);

        int compare(int pos1, int pos2);

        String getKeyString(int pos);

        boolean hasUnicodeChars();

        void serialize(DataOutput out, int prefixLen) throws IOException;
    }

    //TODO right now byte[] contains 7 bit characters, but it should be expandable to 8bit.
    public static final class ByteArrayKeys implements StringArrayKeys {
        final int[] offset;
        final byte[] array;

        ByteArrayKeys(int[] offset, byte[] array) {
            this.offset = offset;
            this.array = array;

            if(CC.PARANOID && ! (array.length==0 || array.length == offset[offset.length-1]))
                throw new AssertionError();
        }

        ByteArrayKeys(DataInput in, int[] offsets, int prefixLen) throws IOException {
            this.offset = offsets;
            array = new byte[offsets[offsets.length-1]];

            in.readFully(array, 0, prefixLen);
            for(int i=0; i<offsets.length-1;i++){
                System.arraycopy(array,0,array,offsets[i],prefixLen);
            }
            //$DELAY$
            //read suffixes
            int offset = prefixLen;
            for(int o:offsets){
                in.readFully(array,offset,o-offset);
                offset = o+prefixLen;
            }

        }

        ByteArrayKeys(int[] offsets, Object[] keys) {
            this.offset = offsets;
            //fill large array
            array = new byte[offsets[offsets.length-1]];
            int bbOffset = 0;
            //$DELAY$
            for (Object key : keys) {
                String str = (String) key;
                for (int j = 0; j < str.length(); j++) {
                    array[bbOffset++] = (byte) str.charAt(j);
                }
            }
        }

        @Override
        public int commonPrefixLen() {
            int lenMinus1 = offset.length-1;
            //$DELAY$
            for(int ret = 0;; ret++){
                if(offset[0]==ret)
                    return ret;
                byte byt = array[ret];
                for(int i=0;i<lenMinus1;i++){
                    int o = offset[i]+ret;
                    if( o==offset[i+1]  || //too long
                            array[o]!=byt //other character
                            )
                        return ret;
                }
            }
        }

        @Override
        public int length() {
            return offset.length;
        }

        @Override
        public ByteArrayKeys deleteKey(int pos) {
            int split = pos==0? 0: offset[pos-1];
            int next = offset[pos];

            byte[] bb = new byte[array.length - (next-split)];
            int[] offsets = new  int[offset.length - 1];

            System.arraycopy(array,0,bb,0,split);
            //$DELAY$
            System.arraycopy(array,next,bb,split,array.length-next);

            int minus=0;
            int plusI=0;
            for(int i=0;i<offsets.length;i++){
                if(i==pos){
                    //skip current item and normalize offsets
                    plusI=1;
                    minus = next-split;
                }
                offsets[i] = offset[i+plusI] - minus;
            }
            return new ByteArrayKeys(offsets,bb);
        }

        @Override
        public ByteArrayKeys copyOfRange(int from, int to) {
            int start = from==0? 0: offset[from-1];
            int end = to==0? 0: offset[to-1];
            byte[] bb = Arrays.copyOfRange(array,start,end);
            //$DELAY$
            int[] offsets = new int[to-from];
            for(int i=0;i<offsets.length;i++){
                offsets[i] = offset[i+from] - start;
            }

            return new ByteArrayKeys(offsets,bb);
        }

        @Override
        public StringArrayKeys putKey(int pos, String newKey) {
            if(containsUnicode(newKey)){
                return CharArrayKeys.putKey(this,pos,newKey);
            }
            return putKey(pos,newKey.getBytes());
        }

        static final boolean containsUnicode(String str){
            int strLen = str.length();
            //$DELAY$
            for(int i=0;i<strLen;i++){
                if(str.charAt(i)>127)
                    return true;
            }
            return false;
        }

        public ByteArrayKeys putKey(int pos, byte[] newKey) {
            byte[] bb = new byte[array.length+ newKey.length];
            int split1 = pos==0? 0: offset[pos-1];
            System.arraycopy(array,0,bb,0,split1);
            //$DELAY$
            System.arraycopy(newKey,0,bb,split1,newKey.length);
            System.arraycopy(array,split1,bb,split1+newKey.length,array.length-split1);

            int[] offsets = new int[offset.length+1];

            int plus = 0;
            int plusI = 0;
            for(int i=0;i<offset.length;i++){
                if(i==pos){
                    //skip one item and increase space
                    plus = newKey.length;
                    plusI = 1;

                }
                offsets[i+plusI] = offset[i] + plus;
            }
            offsets[pos] = split1+newKey.length;

            return new ByteArrayKeys(offsets,bb);
        }

        public byte[] getKey(int pos) {
            int from =  pos==0 ? 0 : offset[pos-1];
            int to =  offset[pos];
            return Arrays.copyOfRange(array, from, to);
        }

        public int compare(int pos1, byte[] string) {
            int strLen = string.length;
            int start1 = pos1==0 ? 0 : offset[pos1-1];
            int start2 = 0;
            int len1 = offset[pos1] - start1;
            int len = Math.min(len1,strLen);
            //$DELAY$
            while(len-- != 0){
                byte b1 = array[start1++];
                byte b2 = string[start2++];
                if(b1!=b2){
                    return b1-b2;
                }
            }
            return len1 - strLen;
        }

        @Override
        public int compare(int pos1, String string) {
            int strLen = string.length();
            int start1 = pos1==0 ? 0 : offset[pos1-1];
            int start2 = 0;
            int len1 = offset[pos1] - start1;
            int len = Math.min(len1,strLen);
             //$DELAY$
            while(len-- != 0){
                byte b1 = array[start1++];
                byte b2 = (byte) string.charAt(start2++);
                if(b1!=b2){
                    return b1-b2;
                }
            }
            return len1 - strLen;
        }

        @Override
        public int compare(int pos1, int pos2) {
            int start1 = pos1==0 ? 0 : offset[pos1-1];
            int start2 = pos2==0 ? 0 : offset[pos2-1];
            int len1 = offset[pos1] - start1;
            int len2 = offset[pos2] - start2;
            int len = Math.min(len1,len2);
            //$DELAY$
            while(len-- != 0){
                byte b1 = array[start1++];
                byte b2 = array[start2++];
                if(b1!=b2){
                    return b1-b2;
                }
            }
            return len1 - len2;
        }

        @Override
        public String getKeyString(int pos) {
            byte[] ret = getKey(pos);
            StringBuilder sb = new StringBuilder(ret.length);
            for(byte b:ret){
                sb.append((char)b);
            }
            return sb.toString();
        }

        @Override
        public boolean hasUnicodeChars() {
            return false;
        }

        @Override
        public void serialize(DataOutput out, int prefixLen) throws IOException {
            //write rest of the suffix
            out.write(array,0,prefixLen);
            //$DELAY$
            //write suffixes
            int aa = prefixLen;
            for(int o:offset){
                out.write(array, aa, o-aa);
                aa = o+prefixLen;
            }
        }
    }

    public static final class CharArrayKeys implements StringArrayKeys {
        final int[] offset;
        final char[] array;

        CharArrayKeys(int[] offset, char[] array) {
            this.offset = offset;
            this.array = array;

            if(CC.PARANOID && ! (array.length==0 || array.length == offset[offset.length-1]))
                throw new AssertionError();
        }

        public CharArrayKeys(DataInput in, int[] offsets, int prefixLen) throws IOException {
            this.offset = offsets;
            array = new char[offsets[offsets.length-1]];

            inReadFully(in, 0, prefixLen);
            for(int i=0; i<offsets.length-1;i++){
                System.arraycopy(array,0,array,offsets[i],prefixLen);
            }

            //read suffixes
            int offset = prefixLen;
            for(int o:offsets){
                inReadFully(in, offset, o);
                offset = o+prefixLen;
            }
        }

        CharArrayKeys(int[] offsets, Object[] keys) {
            this.offset = offsets;
            //fill large array
            array = new char[offsets[offsets.length-1]];
            int bbOffset = 0;
            for (Object key : keys) {
                String str = (String) key;
                str.getChars(0, str.length(), array, bbOffset);
                bbOffset += str.length();
            }
        }



        private void inReadFully(DataInput in, int from, int to) throws IOException {
            for(int i=from;i<to;i++){
                array[i] = (char) DataIO.unpackInt(in);
            }
        }

        @Override
        public int commonPrefixLen() {
            int lenMinus1 = offset.length-1;
            for(int ret = 0;; ret++){
                if(offset[0]==ret)
                    return ret;
                char byt = array[ret];
                for(int i=0;i<lenMinus1;i++){
                    int o = offset[i]+ret;
                    if( o==offset[i+1]  || //too long
                            array[o]!=byt //other character
                            )
                        return ret;
                }
            }
        }

        @Override
        public int length() {
            return offset.length;
        }

        @Override
        public CharArrayKeys deleteKey(int pos) {
            int split = pos==0? 0: offset[pos-1];
            int next = offset[pos];
            //$DELAY$
            char[] bb = new char[array.length - (next-split)];
            int[] offsets = new  int[offset.length - 1];

            System.arraycopy(array,0,bb,0,split);
            //$DELAY$
            System.arraycopy(array,next,bb,split,array.length-next);

            int minus=0;
            int plusI=0;
            for(int i=0;i<offsets.length;i++){
                if(i==pos){
                    //skip current item and normalize offsets
                    plusI=1;
                    minus = next-split;
                }
                offsets[i] = offset[i+plusI] - minus;
            }
            return new CharArrayKeys(offsets,bb);
        }

        @Override
        public CharArrayKeys copyOfRange(int from, int to) {
            int start = from==0? 0: offset[from-1];
            int end = to==0? 0: offset[to-1];
            char[] bb = Arrays.copyOfRange(array,start,end);
            //$DELAY$
            int[] offsets = new int[to-from];
            for(int i=0;i<offsets.length;i++){
                offsets[i] = offset[i+from] - start;
            }

            return new CharArrayKeys(offsets,bb);
        }

        @Override
        public CharArrayKeys putKey(int pos, String newKey) {
            int strLen = newKey.length();
            char[] bb = new char[array.length+ strLen];
            int split1 = pos==0? 0: offset[pos-1];
            //$DELAY$
            System.arraycopy(array,0,bb,0,split1);
            newKey.getChars(0,strLen,bb,split1);
            System.arraycopy(array,split1,bb,split1+strLen,array.length-split1);

            int[] offsets = new int[offset.length+1];

            int plus = 0;
            int plusI = 0;
            for(int i=0;i<offset.length;i++){
                if(i==pos){
                    //skip one item and increase space
                    plus = strLen;
                    plusI = 1;
                }
                offsets[i+plusI] = offset[i] + plus;
            }
            offsets[pos] = split1+strLen;

            return new CharArrayKeys(offsets,bb);
        }

        public static StringArrayKeys putKey(ByteArrayKeys kk, int pos, String newKey) {
            int strLen = newKey.length();
            char[] bb = new char[kk.array.length+ strLen];
            int split1 = pos==0? 0: kk.offset[pos-1];
            for(int i=0;i<split1;i++){
                bb[i] = (char) kk.array[i];
            }
            newKey.getChars(0,strLen,bb,split1);
            for(int i=split1;i<kk.array.length;i++){
                bb[i+strLen] = (char) kk.array[i];
            }
            int[] offsets = new int[kk.offset.length+1];
            int plus = 0;
            int plusI = 0;
            //$DELAY$
            for(int i=0;i<kk.offset.length;i++){
                if(i==pos){
                    //skip one item and increase space
                    plus = strLen;
                    plusI = 1;
                }
                offsets[i+plusI] = kk.offset[i] + plus;
            }
            offsets[pos] = split1+strLen;

            return new CharArrayKeys(offsets,bb);

        }


        @Override
        public int compare(int pos1, String string) {
            int strLen = string.length();
            int start1 = pos1==0 ? 0 : offset[pos1-1];
            int start2 = 0;
            int len1 = offset[pos1] - start1;
            int len = Math.min(len1,strLen);
            //$DELAY$
            while(len-- != 0){
                char b1 = array[start1++];
                char b2 = string.charAt(start2++);
                if(b1!=b2){
                    return b1-b2;
                }
            }
            return len1 - strLen;
        }

        @Override
        public int compare(int pos1, int pos2) {
            int start1 = pos1==0 ? 0 : offset[pos1-1];
            int start2 = pos2==0 ? 0 : offset[pos2-1];
            int len1 = offset[pos1] - start1;
            int len2 = offset[pos2] - start2;
            int len = Math.min(len1,len2);
            //$DELAY$
            while(len-- != 0){
                char b1 = array[start1++];
                char b2 = array[start2++];
                if(b1!=b2){
                    return b1-b2;
                }
            }
            return len1 - len2;
        }

        @Override
        public String getKeyString(int pos) {
            int from =  pos==0 ? 0 : offset[pos-1];
            int len =  offset[pos]-from;
            return new String(array,from,len);
        }

        @Override
        public boolean hasUnicodeChars() {
            for(char c:array){
                if(c>127)
                    return true;
            }
            return false;
        }

        @Override
        public void serialize(DataOutput out, int prefixLen) throws IOException {
                //write rest of the suffix
                outWrite(out, 0, prefixLen);
                //$DELAY$
                //write suffixes
                int aa = prefixLen;
                for(int o:offset){
                    outWrite(out,  aa, o);
                    aa = o+prefixLen;
                }
            }

        private void outWrite(DataOutput out, int from, int to) throws IOException {
            for(int i=from;i<to;i++){
                DataIO.packInt(out,array[i]);
            }
        }

    }



    public static final  BTreeKeySerializer<String, char[][]> STRING2 = new BTreeKeySerializer<String, char[][]>() {

        @Override
        public void serialize(DataOutput out, char[][] chars) throws IOException {
            boolean unicode = false;
            //write lengths
            for(char[] b:chars){
                DataIO.packInt(out,b.length);
                //$DELAY$
                if(!unicode) {
                    for (char cc : b) {
                        if (cc>127)
                            unicode = true;
                    }
                }
            }


            //find common prefix
            int prefixLen = commonPrefixLen(chars);
            DataIO.packInt(out,(prefixLen<<1) | (unicode?1:0));
            for (int i = 0; i < prefixLen; i++) {
              DataIO.packInt(out, chars[0][i]);
            }
            //$DELAY$
            for(char[] b:chars){
                for (int i = prefixLen; i < b.length; i++) {
                    DataIO.packInt(out, b[i]);
                }
            }
        }

        @Override
        public char[][] deserialize(DataInput in, int nodeSize) throws IOException {
            char[][] ret = new char[nodeSize][];
            //$DELAY$
            //read lengths and init arrays
            for(int i=0;i<ret.length;i++){
                int len = DataIO.unpackInt(in);
                ret[i] = new char[len];
            }
            //$DELAY$
            //read and distribute common prefix
            int prefixLen = DataIO.unpackInt(in);
            boolean unicode = 1==(prefixLen & 1);
            prefixLen >>>=1;
            //$DELAY$
            for(int i=0;i<prefixLen;i++){
                ret[0][i] = (char) in.readByte();
            }

            for(int i=1;i<ret.length;i++){
                System.arraycopy(ret[0],0,ret[i],0,prefixLen);
            }
            //$DELAY$
            //read suffixes
            for(char[] b:ret){
                 for(int j=prefixLen;j<b.length;j++){
                     b[j] = (char) DataIO.unpackInt(in);
                 }
            }
            //$DELAY$
            return ret;
        }

        /** compares two char arrays, has same contract as {@link String#compareTo(String)} */
        int compare(char[] c1, char[] c2){
            int end = (c1.length <= c2.length) ? c1.length : c2.length;
            int ret;
            //$DELAY$
            for(int i=0;i<end;i++){
                if ((ret = c1[i] - c2[i]) != 0) {
                    return ret;
                }
            }
            //$DELAY$
            return c1.length - c2.length;
        }


        /** compares char array and string, has same contract as {@link String#compareTo(String)} */
        int compare(char[] c1, String c2){
            int end = Math.min(c1.length,c2.length());
            int ret;
            //$DELAY$
            for(int i=0;i<end;i++){
                if ((ret = c1[i] - c2.charAt(i)) != 0) {
                    return ret;
                }
            }
            //$DELAY$
            return c1.length - c2.length();
        }

        @Override
        public int compare(char[][] chars, int pos1, int pos2) {
            return compare(chars[pos1],chars[pos2]);
        }

        @Override
        public int compare(char[][] chars, int pos, String key) {
            return compare(chars[pos],key);
        }

        @Override
        public String getKey(char[][] chars, int pos) {
            return new String(chars[pos]);
        }

        @Override
        public Comparator comparator() {
            return Fun.COMPARATOR;
        }

        @Override
        public char[][] emptyKeys() {
            return new char[0][];
        }

        @Override
        public int length(char[][] chars) {
            return chars.length;
        }

        @Override
        public char[][] putKey(char[][] keys, int pos, String newKey) {
            return (char[][]) BTreeMap.arrayPut(keys, pos, newKey.toCharArray());
        }

        @Override
        public char[][] copyOfRange(char[][] keys, int from, int to) {
            return Arrays.copyOfRange( keys,from,to);
        }


        @Override
        public char[][] arrayToKeys(Object[] keys) {
            char[][] ret = new char[keys.length][];
            //$DELAY$
            for(int i=keys.length-1;i>=0;i--)
                ret[i] = ((String)keys[i]).toCharArray();
            return ret;
        }

        @Override
        public char[][] deleteKey(char[][] keys, int pos) {
            char[][] keys2 = new char[keys.length-1][];
            //$DELAY$
            System.arraycopy(keys,0,keys2, 0, pos);
            System.arraycopy(keys, pos+1, keys2, pos, keys2.length-pos);
            return keys2;
        }
    };

    protected static int commonPrefixLen(byte[][] bytes) {
        for(int ret=0;;ret++){
            if(bytes[0].length==ret) {
                return ret;
            }
            byte byt = bytes[0][ret];
            for(int i=1;i<bytes.length;i++){
                if(bytes[i].length==ret || byt!=bytes[i][ret])
                    return ret;
            }
        }
    }

    protected static int commonPrefixLen(char[][] chars) {
        //$DELAY$
        for(int ret=0;;ret++){
            if(chars[0].length==ret) {
                return ret;
            }
            char byt = chars[0][ret];
            for(int i=1;i<chars.length;i++){
                if(chars[i].length==ret || byt!=chars[i][ret])
                    return ret;
            }
        }
    }

    public static final BTreeKeySerializer<String,StringArrayKeys> STRING = new BTreeKeySerializer<String,StringArrayKeys>() {
        @Override
        public void serialize(DataOutput out, StringArrayKeys keys2) throws IOException {
            ByteArrayKeys keys = (ByteArrayKeys) keys2;
            int offset = 0;
            //write sizes
            for(int o:keys.offset){
                DataIO.packInt(out,(o-offset));
                offset = o;
            }
            //$DELAY$
            int unicode = keys2.hasUnicodeChars()?1:0;
            
            //find and write common prefix
            int prefixLen = keys.commonPrefixLen();
            DataIO.packInt(out,(prefixLen<<1) | unicode);
            keys2.serialize(out, prefixLen);
        }

        @Override
        public StringArrayKeys deserialize(DataInput in, int nodeSize) throws IOException {
            //read data sizes
            int[] offsets = new int[nodeSize];
            int old=0;
            for(int i=0;i<nodeSize;i++){
                old+= DataIO.unpackInt(in);
                offsets[i]=old;
            }
            //$DELAY$
            //read and distribute common prefix
            int prefixLen = DataIO.unpackInt(in);
            boolean useUnicode = (0!=(prefixLen & 1));
            prefixLen >>>=1;
            //$DELAY$
            return useUnicode?
                    new CharArrayKeys(in,offsets,prefixLen):
                    new ByteArrayKeys(in,offsets,prefixLen);
        }

        @Override
        public int compare(StringArrayKeys byteArrayKeys, int pos1, int pos2) {
            return byteArrayKeys.compare(pos1,pos2);
        }

        @Override
        public int compare(StringArrayKeys byteArrayKeys, int pos1, String string) {
            return byteArrayKeys.compare(pos1,string);
        }



        @Override
        public String getKey(StringArrayKeys byteArrayKeys, int pos) {
            return byteArrayKeys.getKeyString(pos);
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
        public int length(StringArrayKeys byteArrayKeys) {
            return byteArrayKeys.length();
        }

        @Override
        public StringArrayKeys putKey(StringArrayKeys byteArrayKeys, int pos, String string) {
            return byteArrayKeys.putKey(pos,string);
        }

        @Override
        public StringArrayKeys arrayToKeys(Object[] keys) {
            if(keys.length==0)
                return emptyKeys();
            //$DELAY$
            boolean unicode = false;

            //fill offsets
            int[] offsets = new int[keys.length];

            int old=0;
            for(int i=0;i<keys.length;i++){
                String b = (String) keys[i];

                if(!unicode && ByteArrayKeys.containsUnicode(b)) {
                    unicode = true;
                }

                old+=b.length();
                offsets[i]=old;
            }

            return unicode?
                    new CharArrayKeys(offsets, keys):
                    new ByteArrayKeys(offsets,keys);
        }

        @Override
        public StringArrayKeys copyOfRange(StringArrayKeys byteArrayKeys, int from, int to) {
            return byteArrayKeys.copyOfRange(from,to);
        }

        @Override
        public StringArrayKeys deleteKey(StringArrayKeys byteArrayKeys, int pos) {
            return byteArrayKeys.deleteKey(pos);
        }
    };

    public static final  BTreeKeySerializer<byte[], byte[][]> BYTE_ARRAY2 = new BTreeKeySerializer<byte[], byte[][]>() {

        @Override
        public void serialize(DataOutput out, byte[][] chars) throws IOException {
            //write lengths
            for(byte[] b:chars){
                DataIO.packInt(out,b.length);
            }
            //$DELAY$
            //find common prefix
            int prefixLen = commonPrefixLen(chars);
            DataIO.packInt(out,prefixLen);
            out.write(chars[0], 0, prefixLen);
            //$DELAY$
            for(byte[] b:chars){
                out.write(b,prefixLen,b.length-prefixLen);
            }
        }

        @Override
        public byte[][] deserialize(DataInput in, int nodeSize) throws IOException {
            byte[][] ret = new byte[nodeSize][];

            //read lengths and init arrays
            for(int i=0;i<ret.length;i++){
                ret[i] = new byte[DataIO.unpackInt(in)];
            }
            //$DELAY$
            //read and distribute common prefix
            int prefixLen = DataIO.unpackInt(in);
            in.readFully(ret[0],0,prefixLen);
            for(int i=1;i<ret.length;i++){
                System.arraycopy(ret[0],0,ret[i],0,prefixLen);
            }
            //$DELAY$
            //read suffixes
            for (byte[] aRet : ret) {
                in.readFully(aRet, prefixLen, aRet.length - prefixLen);
            }

            return ret;
        }

        /** compares two char arrays, has same contract as {@link String#compareTo(String)} */
        int compare(byte[] c1, byte[] c2){
            int end = (c1.length <= c2.length) ? c1.length : c2.length;
            int ret;
            //$DELAY$
            for(int i=0;i<end;i++){
                if ((ret = c1[i] - c2[i]) != 0) {
                    return ret;
                }
            }
            //$DELAY$
            return c1.length - c2.length;
        }



        @Override
        public int compare(byte[][] chars, int pos1, int pos2) {
            return compare(chars[pos1], chars[pos2]);
        }

        @Override
        public int compare(byte[][] chars, int pos, byte[] key) {
            return compare(chars[pos],key);
        }

        @Override
        public byte[] getKey(byte[][] chars, int pos) {
            return chars[pos];
        }

        @Override
        public Comparator comparator() {
            return Fun.BYTE_ARRAY_COMPARATOR;
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
        public byte[][] putKey(byte[][] keys, int pos, byte[] newKey) {
            return (byte[][]) BTreeMap.arrayPut(keys, pos, newKey);
        }

        @Override
        public byte[][] copyOfRange(byte[][] keys, int from, int to) {
            return Arrays.copyOfRange( keys,from,to);
        }


        @Override
        public byte[][] arrayToKeys(Object[] keys) {
            byte[][] ret = new byte[keys.length][];
            for(int i=keys.length-1;i>=0;i--)
                ret[i] = (byte[]) keys[i];
            return ret;
        }

        @Override
        public byte[][] deleteKey(byte[][] keys, int pos) {
            byte[][] keys2 = new byte[keys.length-1][];
            System.arraycopy(keys,0,keys2, 0, pos);
            //$DELAY$
            System.arraycopy(keys, pos+1, keys2, pos, keys2.length-pos);
            return keys2;
        }
    };

    public static final BTreeKeySerializer<byte[],ByteArrayKeys> BYTE_ARRAY = new BTreeKeySerializer<byte[],ByteArrayKeys>() {
        @Override
        public void serialize(DataOutput out, ByteArrayKeys keys) throws IOException {
            int offset = 0;
            //write sizes
            for(int o:keys.offset){
                DataIO.packInt(out,o-offset);
                offset = o;
            }
            //$DELAY$
            //find and write common prefix
            int prefixLen = keys.commonPrefixLen();
            DataIO.packInt(out, prefixLen);
            out.write(keys.array,0,prefixLen);
            //$DELAY$
            //write suffixes
            offset = prefixLen;
            for(int o:keys.offset){
                out.write(keys.array, offset, o-offset);
                offset = o+prefixLen;
            }
        }

        @Override
        public ByteArrayKeys deserialize(DataInput in, int nodeSize) throws IOException {
            //read data sizes
            int[] offsets = new int[nodeSize];
            int old=0;
            for(int i=0;i<nodeSize;i++){
                old+= DataIO.unpackInt(in);
                offsets[i]=old;
            }
            byte[] bb = new byte[old];
            //$DELAY$
            //read and distribute common prefix
            int prefixLen = DataIO.unpackInt(in);
            in.readFully(bb, 0, prefixLen);
            for(int i=0; i<offsets.length-1;i++){
                System.arraycopy(bb, 0, bb, offsets[i], prefixLen);
            }
            //$DELAY$
            //read suffixes
            int offset = prefixLen;
            for(int o:offsets){
                in.readFully(bb,offset,o-offset);
                offset = o+prefixLen;
            }

            return new ByteArrayKeys(offsets,bb);
        }

        @Override
        public int compare(ByteArrayKeys byteArrayKeys, int pos1, int pos2) {
            return byteArrayKeys.compare(pos1,pos2);
        }

        @Override
        public int compare(ByteArrayKeys byteArrayKeys, int pos1, byte[] string) {
            return byteArrayKeys.compare(pos1,string);
        }

        @Override
        public byte[] getKey(ByteArrayKeys byteArrayKeys, int pos) {
            return byteArrayKeys.getKey(pos);
        }

        @Override
        public Comparator comparator() {
            return Fun.BYTE_ARRAY_COMPARATOR;
        }

        @Override
        public ByteArrayKeys emptyKeys() {
            return new ByteArrayKeys(new int[0], new byte[0]);
        }

        @Override
        public int length(ByteArrayKeys byteArrayKeys) {
            return byteArrayKeys.length();
        }

        @Override
        public ByteArrayKeys putKey(ByteArrayKeys byteArrayKeys, int pos, byte[] newKey) {
            return byteArrayKeys.putKey(pos,newKey);
        }

        @Override
        public ByteArrayKeys arrayToKeys(Object[] keys) {
            //fill offsets
            int[] offsets = new int[keys.length];

            int old=0;
            for(int i=0;i<keys.length;i++){
                byte[] b = (byte[]) keys[i];
                old+=b.length;
                offsets[i]=old;
            }
            //$DELAY$
            //fill large array
            byte[] bb = new byte[old];
            old=0;
            for(int i=0;i<keys.length;i++){
                int curr = offsets[i];
                System.arraycopy(keys[i], 0, bb, old, curr - old);
                old=curr;
            }
            //$DELAY$
            return new ByteArrayKeys(offsets,bb);
        }

        @Override
        public ByteArrayKeys copyOfRange(ByteArrayKeys byteArrayKeys, int from, int to) {
            return byteArrayKeys.copyOfRange(from,to);
        }

        @Override
        public ByteArrayKeys deleteKey(ByteArrayKeys byteArrayKeys, int pos) {
            return byteArrayKeys.deleteKey(pos);
        }
    };

    public static class Compress extends BTreeKeySerializer {

        final BTreeKeySerializer wrapped;
        final CompressLZF lzf = new CompressLZF();

        public Compress(BTreeKeySerializer wrapped) {
            if(wrapped == null)
                throw new  NullPointerException();

            this.wrapped = wrapped;
        }

        protected Compress(SerializerBase serializerBase, DataInput in, SerializerBase.FastArrayList objectStack) throws IOException {
            objectStack.add(this);
            wrapped = (BTreeKeySerializer) serializerBase.deserialize(in,objectStack);
            if(wrapped == null)
                throw new  NullPointerException();
        }

        @Override
        public void serialize(DataOutput out, Object o) throws IOException {
            DataIO.DataOutputByteArray out2 = new DataIO.DataOutputByteArray();
            wrapped.serialize(out2, o);
            DataIO.packInt(out,out2.pos);
            //$DELAY$
            byte[] out3 = new byte[out2.pos+100];
            int compSize = lzf.compress(out2.buf,out2.pos,out3,0);
            out.write(out3,0,compSize);
        }

        @Override
        public Object deserialize(DataInput in, int nodeSize) throws IOException {
            int unpackSize = DataIO.unpackInt(in);
            byte[] in2 = new byte[unpackSize];
            //$DELAY$
            lzf.expand(in,in2,0,in2.length);
            return wrapped.deserialize(new DataIO.DataInputByteArray(in2),nodeSize);
        }

        @Override
        public int compare(Object o, int pos1, int pos2) {
            return wrapped.compare(o, pos1, pos2);
        }

        @Override
        public int compare(Object o, int pos, Object o2) {
            return wrapped.compare(o, pos, o2);
        }

        @Override
        public boolean compareIsSmaller(Object o, int pos, Object o2) {
            return wrapped.compareIsSmaller(o, pos, o2);
        }

        @Override
        public Object getKey(Object o, int pos) {
            return wrapped.getKey(o, pos);
        }

        @Override
        public Comparator comparator() {
            return wrapped.comparator();
        }

        @Override
        public Object emptyKeys() {
            return wrapped.emptyKeys();
        }

        @Override
        public int length(Object o) {
            return wrapped.length(o);
        }

        @Override
        public Object putKey(Object o, int pos, Object newKey) {
            return wrapped.putKey(o, pos, newKey);
        }

        @Override
        public Object copyOfRange(Object o, int from, int to) {
            return wrapped.copyOfRange(o, from, to);
        }

        @Override
        public Object deleteKey(Object o, int pos) {
            return wrapped.deleteKey(o, pos);
        }

        @Override
        public int findChildren(BTreeMap.BNode node, Object key) {
            return wrapped.findChildren(node, key);
        }

        @Override
        public int findChildren2(BTreeMap.BNode node, Object key) {
            return wrapped.findChildren2(node, key);
        }

        @Override
        public Object arrayToKeys(Object[] keys) {
            return wrapped.arrayToKeys(keys);
        }

        @Override
        public Object[] keysToArray(Object o) {
            return wrapped.keysToArray(o);
        }

    }
}