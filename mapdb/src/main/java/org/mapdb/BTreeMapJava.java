package org.mapdb;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Java code for BTreeMap. Mostly performance sensitive code.
 */
public class BTreeMapJava {

    static final int DIR = 1<<3;
    static final int LEFT = 1<<2;
    static final int RIGHT = 1<<1;
    static final int LAST_KEY_DOUBLE = 1;

    public static class Node{

        /** bit flags (dir, left most, right most, next key equal to last...) */
        final byte flags;
        /** link to next node */
        final long link;
        /** represents keys */
        final Object keys;
        /** represents values for leaf node, or ArrayLong of children for dir node  */
        final Object values;

        Node(int flags, long link, Object keys, Object values, Serializer keySerializer, Serializer valueSerializer) {
            this(flags, link, keys, values);

            if(CC.ASSERT) {
                int keysLen = keySerializer.valueArraySize(keys);
                if (isDir()){
                    // compare directory size
                    if( keysLen - 1 + intLeftEdge() + intRightEdge() !=
                                ((long[]) values).length) {
                        throw new AssertionError();
                    }
                } else{
                    // compare leaf size
                    if (
                            keysLen !=
                                    valueSerializer.valueArraySize(values) + 2 - intLeftEdge() - intRightEdge() - intLastKeyDouble()) {
                        throw new AssertionError();
                    }
                }
            }
        }
        Node(int flags, long link, Object keys, Object values){
            this.flags = (byte)flags;
            this.link = link;
            this.keys = keys;
            this.values = values;

            if(CC.ASSERT && isLastKeyDouble() && isDir())
                throw new AssertionError();

            if(CC.ASSERT && isRightEdge() && (link!=0L))
                throw new AssertionError();

            if(CC.ASSERT && !isRightEdge() && (link==0L))
                throw new AssertionError();
        }

        int intDir(){
            return (flags>>>3)&1;
        }

        int intLeftEdge(){
            return (flags>>>2)&1;
        }

        int intRightEdge(){
            return (flags>>>1)&1;
        }

        int intLastKeyDouble(){
            return flags&1;
        }


        boolean isDir(){
            return ((flags>>>3)&1)==1;
        }

        boolean isLeftEdge(){
            return ((flags>>>2)&1)==1;
        }

        boolean isRightEdge(){
            return ((flags>>>1)&1)==1;
        }

        boolean isLastKeyDouble(){
            return ((flags)&1)==1;
        }

        @Nullable
        public Object highKey(Serializer keySerializer) {
            int keysLen = keySerializer.valueArraySize(keys);
            return keySerializer.valueArrayGet(keys, keysLen-1);
        }

        public long[] getChildren(){
            return (long[]) values;
        }
    }

    static class NodeSerializer extends Serializer<Node>{

        final Serializer keySerializer;
        final Serializer valueSerializer;

        NodeSerializer(Serializer keySerializer, Serializer valueSerializer) {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }

        @Override
        public void serialize(@NotNull DataOutput2 out, @NotNull Node value) throws IOException {

            if(CC.ASSERT && value.flags>>>4!=0)
                throw new AssertionError();
            int keysLen = keySerializer.valueArraySize(value.keys)<<4;
            keysLen += value.flags;
            keysLen = DataIO.parity1Set(keysLen<<1);

            //keysLen and flags are combined into single packed long, that saves a byte for small nodes
            out.packInt(keysLen);
            if(!value.isRightEdge())
                out.packLong(value.link);
            keySerializer.valueArraySerialize(out, value.keys);
            if(value.isDir()) {
                long[] child = (long[]) value.values;
                out.packLongArray(child, 0, child.length );
            }else
                valueSerializer.valueArraySerialize(out, value.values);
        }

        @Override
        public Node deserialize(@NotNull DataInput2 input, int available) throws IOException {
            int keysLen = DataIO.parity1Get(input.unpackInt())>>>1;
            int flags = keysLen & 0xF;
            keysLen = keysLen>>>4;
            long link =  (flags&RIGHT)!=0
                    ? 0L :
                    input.unpackLong();

            Object keys = keySerializer.valueArrayDeserialize(input, keysLen);
            if(CC.ASSERT && keysLen!=keySerializer.valueArraySize(keys))
                throw new AssertionError();

            Object values;
            if((flags&DIR)!=0){
                keysLen = keysLen - 1 + (flags>>2&1) +(flags>>1&1);
                long[] c = new long[keysLen];
                values = c;
                input.unpackLongArray(c, 0, keysLen);
            }else{
                values = valueSerializer.valueArrayDeserialize(input,
                        keysLen - 2 + ((flags >>> 2) & 1) + ((flags >>> 1) & 1) + (flags & 1));
            }


            return new Node(flags, link, keys, values, keySerializer, valueSerializer);
        }

        @Override
        public boolean isTrusted() {
            return keySerializer.isTrusted() && valueSerializer.isTrusted();
        }
    }

    public static final Comparator COMPARABLE_COMPARATOR = new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
            return o1.compareTo(o2);
        }
    };



    static long findChild(Serializer keySerializer, Node node, Comparator comparator, Object key){
        if(CC.ASSERT && !node.isDir())
            throw new AssertionError();
        //find an index
        int pos = keySerializer.valueArraySearch(node.keys, key, comparator);

        if(pos<0)
            pos = -pos-1;

        pos += -1+node.intLeftEdge();

        pos = Math.max(0, pos);
        long[] children = (long[]) node.values;
        if(pos>=children.length) {
            if(CC.ASSERT && node.isRightEdge())
                throw new AssertionError();
            return node.link;
        }
        return children[pos];
    }



    static final Object LINK = new Object(){
        @Override
        public String toString() {
            return "BTreeMap.LINK";
        }
    };

    static Object leafGet(Node node, Comparator comparator, Object key, Serializer keySerializer, Serializer valueSerializer){
        int pos = keySerializer.valueArraySearch(node.keys, key, comparator);
        return leafGet(node, pos, keySerializer, valueSerializer);
    }

    static Object leafGet(Node node, int pos, Serializer keySerializer, Serializer valueSerializer){

        if(pos<0+1-node.intLeftEdge()) {
            if(!node.isRightEdge() && pos<-keySerializer.valueArraySize(node.keys))
                return LINK;
            else
                return null;
        }
        int valsLen = valueSerializer.valueArraySize(node.values);
        if(!node.isRightEdge() && pos==valsLen+1)
            return null;
        else if(pos>=valsLen+1){
            return LINK;
        }
        pos = pos-1+node.intLeftEdge();
        if(pos>=valsLen)
            return null;
        return valueSerializer.valueArrayGet(node.values, pos);
    }

    /* expand array size by 1, and put value at given position. No items from original array are lost*/
    protected static Object[] arrayPut(final Object[] array, final int pos, final Object value){
        final Object[] ret = Arrays.copyOf(array, array.length+1);
        if(pos<array.length){
            System.arraycopy(array, pos, ret, pos+1, array.length-pos);
        }
        ret[pos] = value;
        return ret;
    }

    /* expand array size by 1, and put value at given position. No items from original array are lost*/
    protected static long[] arrayPut(final long[] array, final int pos, final long value){
        final long[] ret = Arrays.copyOf(array, array.length+1);
        if(pos<array.length){
            System.arraycopy(array, pos, ret, pos+1, array.length-pos);
        }
        ret[pos] = value;
        return ret;
    }

    public static class BinaryGet<K, V> implements StoreBinaryGetLong {
        final Serializer<K> keySerializer;
        final Serializer<V> valueSerializer;
        final Comparator<K> comparator;
        final K key;

        V value = null;

        public BinaryGet(
                @NotNull Serializer<K> keySerializer,
                @NotNull Serializer<V> valueSerializer,
                @NotNull Comparator<K> comparator,
                @NotNull K key
                ) {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            this.comparator = comparator;
            this.key = key;
        }

        @Override
        public long get(DataInput2 input, int size) throws IOException {
            //read size and flags
            int keysLen = DataIO.parity1Get(input.unpackInt())>>>1;
            int flags = keysLen&0xF;
            keysLen = keysLen>>>4;

            long link =  (flags&RIGHT)!=0
                    ? 0L :
                    input.unpackLong();

            int intLeft = ((flags >>> 2) & 1);
            int intRight = ((flags >>> 1) & 1);

            int pos = keySerializer.valueArrayBinarySearch(key, input, keysLen, comparator);
            if((flags&DIR)!=0){
                //is directory, return related children

                if(pos<0)
                    pos = -pos-1;

                pos += -1 + intLeft;   // plus left edge
                pos = Math.max(0, pos);
                keysLen = keysLen - 1 + intLeft + intRight;

                if(pos>=keysLen) {
                    if(CC.ASSERT && intRight==1)
                        throw new AssertionError();
                    return link;
                }
                if(pos>0)
                    input.unpackLongSkip(pos-1);
                return input.unpackLong();
            }

            //is leaf, get value from leaf

            if(pos<0+1-intLeft) {
                if(intRight==0 && pos<-keysLen)
                    return link;
                else
                    return -1;
            }

            int valsLen = keysLen - 2 + intLeft + intRight + (flags & 1);

            if(intRight==0 /*is not right edge*/ && pos==valsLen+1) {
                //return null
                return -1;
            }else if(pos>=valsLen+1){
                return link;
            }

            pos = pos-1+((flags >>> 2) & 1);
            if(pos>=valsLen) {
                //return null
                return -1;
            }

            //found value, return it
            value = valueSerializer.valueArrayBinaryGet(input, pos);
            return -1L;
        }
    }
}

