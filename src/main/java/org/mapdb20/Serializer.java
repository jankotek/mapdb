/*
 *  Copyright (c) 2012 Jan Kotek
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.mapdb20;


import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.UUID;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

/**
 * Provides serialization and deserialization
 *
 * @author Jan Kotek
 */
public abstract class Serializer<A> {


    public static final Serializer<Character> CHAR =  new Serializer<Character>() {
        @Override
        public void serialize(DataOutput out, Character value) throws IOException {
            out.writeChar(value.charValue());
        }

        @Override
        public Character deserialize(DataInput in, int available) throws IOException {
            return in.readChar();
        }

        @Override
        public int fixedSize() {
            return 2;
        }

        @Override
        public boolean isTrusted() {
            return true;
        }
    };



    /**
     * <p>
     * Serializes strings using UTF8 encoding.
     * Stores string size so can be used as collection serializer.
     * Does not handle null values
     * </p><p>
     * Unlike {@link Serializer#STRING} this method hashes String with more reliable XXHash.
     * </p>
     */
    public static final Serializer<String> STRING_XXHASH = new StringValueSerializer (){
        @Override
        public void serialize(DataOutput out, String value) throws IOException {
            out.writeUTF(value);
        }

        @Override
        public String deserialize(DataInput in, int available) throws IOException {
            return in.readUTF();
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public BTreeKeySerializer getBTreeKeySerializer(Comparator comparator) {
            if(comparator!=null && comparator!=Fun.COMPARATOR) {
                return super.getBTreeKeySerializer(comparator);
            }
            return BTreeKeySerializer.STRING;
        }

        @Override
        public int hashCode(String s, int seed) {
            char[] c = s.toCharArray();
            return CHAR_ARRAY.hashCode(c, seed);
        }
    };

    /**
     * Serializes strings using UTF8 encoding.
     * Stores string size so can be used as collection serializer.
     * Does not handle null values
     */
    public static final Serializer<String> STRING = new StringValueSerializer (){
        @Override
        public void serialize(DataOutput out, String value) throws IOException {
            out.writeUTF(value);
        }

        @Override
        public String deserialize(DataInput in, int available) throws IOException {
            return in.readUTF();
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public BTreeKeySerializer getBTreeKeySerializer(Comparator comparator) {
            if(comparator!=null && comparator!=Fun.COMPARATOR) {
                return super.getBTreeKeySerializer(comparator);
            }
            return BTreeKeySerializer.STRING;
        }
    };

    private static abstract class StringValueSerializer extends Serializer<String>{
        @Override
        public void valueArraySerialize(DataOutput out, Object vals) throws IOException {
            DataIO.DataOutputByteArray out2 = (DataIO.DataOutputByteArray) out;
            char[][] vals2 = (char[][]) vals;
            for(char[] v:vals2){
                out2.packInt(v.length);
                for(char c:v){
                    out2.packInt(c);
                }
            }
        }

        @Override
        public Object valueArrayDeserialize(DataInput in, int size) throws IOException {
            DataIO.DataInputInternal in2 = (DataIO.DataInputInternal) in;
            char[][] ret = new char[size][];
            for(int i=0;i<size;i++){
                int size2 = in2.unpackInt();
                char[] cc = new char[size2];
                for(int j=0;j<size2;j++){
                    cc[j] = (char) in2.unpackInt();
                }
                ret[i] = cc;
            }
            return ret;
        }

        @Override
        public String valueArrayGet(Object vals, int pos) {
            return new String(((char[][])vals)[pos]);
        }

        @Override
        public int valueArraySize(Object vals) {
            return ((char[][])vals).length;
        }

        @Override
        public Object valueArrayEmpty() {
            return new char[0][];
        }

        @Override
        public Object valueArrayPut(Object vals, int pos, String newValue) {
            char[][] array = (char[][]) vals;
            final char[][] ret = Arrays.copyOf(array, array.length+1);
            if(pos<array.length){
                System.arraycopy(array, pos, ret, pos+1, array.length-pos);
            }
            ret[pos] = newValue.toCharArray();
            return ret;

        }

        @Override
        public Object valueArrayUpdateVal(Object vals, int pos, String newValue) {
            char[][] cc = (char[][]) vals;
            cc = cc.clone();
            cc[pos] = newValue.toCharArray();
            return cc;
        }

        @Override
        public Object valueArrayFromArray(Object[] objects) {
            char[][] ret = new char[objects.length][];
            for(int i=0;i<ret.length;i++){
                ret[i] = ((String)objects[i]).toCharArray();
            }
            return ret;
        }

        @Override
        public Object valueArrayCopyOfRange(Object vals, int from, int to) {
            return Arrays.copyOfRange((char[][]) vals, from, to);
        }

        @Override
        public Object valueArrayDeleteValue(Object vals, int pos) {
            char[][] valsOrig = (char[][]) vals;
            char[][] vals2 = new char[valsOrig.length-1][];
            System.arraycopy(vals,0,vals2, 0, pos-1);
            System.arraycopy(vals, pos, vals2, pos-1, vals2.length-(pos-1));
            return vals2;
        }
    }

    /**
     * Serializes strings using UTF8 encoding.
     * Deserialized String is interned {@link String#intern()},
     * so it could save some memory.
     *
     * Stores string size so can be used as collection serializer.
     * Does not handle null values
     */
    public static final Serializer<String> STRING_INTERN = new Serializer<String>() {
        @Override
        public void serialize(DataOutput out, String value) throws IOException {
            out.writeUTF(value);
        }

        @Override
        public String deserialize(DataInput in, int available) throws IOException {
            return in.readUTF().intern();
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

    };

    /**
     * Serializes strings using ASCII encoding (8 bit character).
     * Is faster compared to UTF8 encoding.
     * Stores string size so can be used as collection serializer.
     * Does not handle null values
     */
    public static final Serializer<String> STRING_ASCII = new Serializer<String>() {
        @Override
        public void serialize(DataOutput out, String value) throws IOException {
            int size = value.length();
            DataIO.packInt(out, size);
            for (int i = 0; i < size; i++) {
                out.write(value.charAt(i));
            }
        }

        @Override
        public String deserialize(DataInput in, int available) throws IOException {
            int size = DataIO.unpackInt(in);
            StringBuilder result = new StringBuilder(size);
            for (int i = 0; i < size; i++) {
                result.append((char)in.readUnsignedByte());
            }
            return result.toString();
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public BTreeKeySerializer getBTreeKeySerializer(Comparator comparator) {
            if(comparator!=null && comparator!=Fun.COMPARATOR) {
                return super.getBTreeKeySerializer(comparator);
            }
            return BTreeKeySerializer.STRING; //TODO ascii specific serializer?
        }

    };

    /**
     * Serializes strings using UTF8 encoding.
     * Used mainly for testing.
     * Does not handle null values.
     */
    public static final Serializer<String> STRING_NOSIZE = new StringValueSerializer (){

        private final Charset UTF8_CHARSET = Charset.forName("UTF8");

        @Override
        public void serialize(DataOutput out, String value) throws IOException {
            final byte[] bytes = value.getBytes(UTF8_CHARSET);
            out.write(bytes);
        }


        @Override
        public String deserialize(DataInput in, int available) throws IOException {
            if(available==-1) throw new IllegalArgumentException("STRING_NOSIZE does not work with collections.");
            byte[] bytes = new byte[available];
            in.readFully(bytes);
            return new String(bytes, UTF8_CHARSET);
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public BTreeKeySerializer getBTreeKeySerializer(Comparator comparator) {
            if(comparator!=null && comparator!=Fun.COMPARATOR) {
                return super.getBTreeKeySerializer(comparator);
            }
            return BTreeKeySerializer.STRING;
        }

    };


    abstract protected static class EightByteSerializer<E> extends Serializer<E>{

        protected abstract E unpack(long l);
        protected abstract long pack(E l);

        @Override
        public E valueArrayGet(Object vals, int pos){
            return unpack(((long[]) vals)[pos]);
        }


        @Override
        public int valueArraySize(Object vals){
            return ((long[])vals).length;
        }

        @Override
        public Object valueArrayEmpty(){
            return new long[0];
        }

        @Override
        public Object valueArrayPut(Object vals, int pos, E newValue) {

            long[] array = (long[]) vals;
            final long[] ret = Arrays.copyOf(array, array.length+1);
            if(pos<array.length){
                System.arraycopy(array, pos, ret, pos+1, array.length-pos);
            }
            ret[pos] = pack(newValue);
            return ret;
        }

        @Override
        public Object valueArrayUpdateVal(Object vals, int pos, E newValue) {
            long[] vals2 = ((long[])vals).clone();
            vals2[pos] = pack(newValue);
            return vals2;
        }

        @Override
        public Object valueArrayFromArray(Object[] objects) {
            long[] ret = new long[objects.length];
            int pos=0;

            for(Object o:objects){
                ret[pos++] = pack((E) o);
            }

            return ret;
        }

        @Override
        public Object valueArrayCopyOfRange(Object vals, int from, int to) {
            return Arrays.copyOfRange((long[])vals, from, to);
        }

        @Override
        public Object valueArrayDeleteValue(Object vals, int pos) {
            long[] valsOrig = (long[]) vals;
            long[] vals2 = new long[valsOrig.length-1];
            System.arraycopy(vals,0,vals2, 0, pos-1);
            System.arraycopy(vals, pos, vals2, pos-1, vals2.length-(pos-1));
            return vals2;
        }

        @Override
        public void valueArraySerialize(DataOutput out, Object vals) throws IOException {
            for(long o:(long[]) vals){
                out.writeLong(o);
            }
        }

        @Override
        public Object valueArrayDeserialize(DataInput in, int size) throws IOException {
            long[] ret = new long[size];
            for(int i=0;i<size;i++){
                ret[i] = in.readLong();
            }
            return ret;
        }


        @Override
        public boolean isTrusted() {
            return true;
        }


        @Override
        public int fixedSize() {
            return 8;
        }

    }


    abstract protected static class LongSerializer extends EightByteSerializer<Long> {

        @Override
        protected Long unpack(long l) {
            return new Long(l);
        }

        @Override
        protected long pack(Long l) {
            return l.longValue();
        }

        @Override
        public BTreeKeySerializer getBTreeKeySerializer(Comparator comparator) {
            if(comparator!=null && comparator!=Fun.COMPARATOR) {
                return super.getBTreeKeySerializer(comparator);
            }
            return BTreeKeySerializer.LONG;
        }
    }

    /** Serializes Long into 8 bytes, used mainly for testing.
     * Does not handle null values.*/

    public static final Serializer<Long> LONG = new LongSerializer() {

        @Override
        public void serialize(DataOutput out, Long value) throws IOException {
            out.writeLong(value);
        }

        @Override
        public Long deserialize(DataInput in, int available) throws IOException {
            return in.readLong();
        }

    };

    /**
     *  Packs positive LONG, so smaller positive values occupy less than 8 bytes.
     *  Large and negative values could occupy 8 or 9 bytes.
     */
    public static final Serializer<Long> LONG_PACKED = new LongSerializer(){
        @Override
        public void serialize(DataOutput out, Long value) throws IOException {
            ((DataIO.DataOutputByteArray) out).packLong(value);
        }

        @Override
        public Long deserialize(DataInput in, int available) throws IOException {
            return ((DataIO.DataInputInternal)in).unpackLong();
        }

        @Override
        public void valueArraySerialize(DataOutput out, Object vals) throws IOException {
            DataIO.DataOutputByteArray out2 = (DataIO.DataOutputByteArray) out;
            for(long o:(long[]) vals){
                out2.packLong(o);
            }
        }

        @Override
        public Object valueArrayDeserialize(DataInput in, int size) throws IOException {
            DataIO.DataInputInternal i = (DataIO.DataInputInternal) in;
            long[] ret = new long[size];
            i.unpackLongArray(ret,0,size);
            return ret;
        }

        @Override
        public int fixedSize() {
            return -1;
        }
    };


    /** packs Long so small values occupy less than 8 bytes. Large (positive and negative)
     * values could occupy more 8 to 9 bytes. It uses zigzag conversion before packing,
     * number is multiplied by two, with last bite indicating negativity.
     */
    public static final Serializer<Long> LONG_PACKED_ZIGZAG = new LongSerializer(){

        long wrap(long i){
            long plus = i<0?1:0; //this could be improved by eliminating condition
            return Math.abs(i*2)+plus;
        }

        long unwrap(long i){
            long m = 1 - 2 * (i&1); // +1 if even, -1 if odd
            return (i>>>1) * m;
        }

        @Override
        public void serialize(DataOutput out, Long value) throws IOException {
            ((DataIO.DataOutputByteArray) out).packLong(wrap(value));
        }

        @Override
        public Long deserialize(DataInput in, int available) throws IOException {
            return unwrap(((DataIO.DataInputInternal) in).unpackLong());
        }

        @Override
        public void valueArraySerialize(DataOutput out, Object vals) throws IOException {
            DataIO.DataOutputByteArray out2 = (DataIO.DataOutputByteArray) out;
            for(long o:(long[]) vals){
                out2.packLong(wrap(o));
            }
        }

        @Override
        public Object valueArrayDeserialize(DataInput in, int size) throws IOException {
            DataIO.DataInputInternal i = (DataIO.DataInputInternal) in;
            long[] ret = new long[size];
            i.unpackLongArray(ret,0,size);
            for(int a=0;a<size;a++){
                ret[a] = unwrap(ret[a]);
            }
            return ret;
        }

        @Override
        public int fixedSize() {
            return -1;
        }
    };


    abstract protected static class FourByteSerializer<E> extends Serializer<E>{

        protected abstract E unpack(int l);
        protected abstract int pack(E l);

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public int fixedSize() {
            return 4;
        }

        @Override
        public E valueArrayGet(Object vals, int pos){
            return unpack(((int[])vals)[pos]);
        }

        @Override
        public int valueArraySize(Object vals){
            return ((int[])vals).length;
        }

        @Override
        public Object valueArrayEmpty(){
            return new int[0];
        }

        @Override
        public Object valueArrayPut(Object vals, int pos, E newValue) {

            int[] array = (int[]) vals;
            final int[] ret = Arrays.copyOf(array, array.length+1);
            if(pos<array.length){
                System.arraycopy(array, pos, ret, pos+1, array.length-pos);
            }
            ret[pos] = pack(newValue);
            return ret;
        }

        @Override
        public Object valueArrayUpdateVal(Object vals, int pos, E newValue) {
            int[] vals2 = ((int[])vals).clone();
            vals2[pos] = pack(newValue);
            return vals2;
        }

        @Override
        public Object valueArrayFromArray(Object[] objects) {
            int[] ret = new int[objects.length];
            int pos=0;

            for(Object o:objects){
                ret[pos++] = pack((E) o);
            }

            return ret;
        }

        @Override
        public Object valueArrayCopyOfRange(Object vals, int from, int to) {
            return Arrays.copyOfRange((int[])vals, from, to);
        }

        @Override
        public Object valueArrayDeleteValue(Object vals, int pos) {
            int[] valsOrig = (int[]) vals;
            int[] vals2 = new int[valsOrig.length-1];
            System.arraycopy(vals,0,vals2, 0, pos-1);
            System.arraycopy(vals, pos, vals2, pos-1, vals2.length-(pos-1));
            return vals2;
        }


        @Override
        public void valueArraySerialize(DataOutput out, Object vals) throws IOException {
            for(int o:(int[]) vals){
                out.writeInt(o);
            }
        }

        @Override
        public Object valueArrayDeserialize(DataInput in, int size) throws IOException {
            int[] ret = new int[size];
            for(int i=0;i<size;i++){
                ret[i] = in.readInt();
            }
            return ret;
        }
    }

    abstract protected static class IntegerSerializer extends FourByteSerializer<Integer> {

        @Override
        protected Integer unpack(int l) {
            return l;
        }

        @Override
        protected int pack(Integer l) {
            return l;
        }

        @Override
        public BTreeKeySerializer getBTreeKeySerializer(Comparator comparator) {
            if(comparator!=null && comparator!=Fun.COMPARATOR) {
                return super.getBTreeKeySerializer(comparator);
            }
            return BTreeKeySerializer.INTEGER;
        }
    }

    /** Serializes Integer into 4 bytes, used mainly for testing.
     * Does not handle null values.*/

    public static final Serializer<Integer> INTEGER = new IntegerSerializer() {

        @Override
        public void serialize(DataOutput out, Integer value) throws IOException {
            out.writeInt(value);
        }

        @Override
        public Integer deserialize(DataInput in, int available) throws IOException {
            return in.readInt();
        }

    };

    /**
     *  Packs positive Integer, so smaller positive values occupy less than 4 bytes.
     *  Large and negative values could occupy 4 or 5 bytes.
     */
    public static final Serializer<Integer> INTEGER_PACKED = new IntegerSerializer(){
        @Override
        public void serialize(DataOutput out, Integer value) throws IOException {
            ((DataIO.DataOutputByteArray) out).packInt(value);
        }

        @Override
        public Integer deserialize(DataInput in, int available) throws IOException {
            return ((DataIO.DataInputInternal)in).unpackInt();
        }

        @Override
        public void valueArraySerialize(DataOutput out, Object vals) throws IOException {
            DataIO.DataOutputByteArray out2 = (DataIO.DataOutputByteArray) out;
            for(int o:(int[]) vals){
                out2.packIntBigger(o);
            }
        }

        @Override
        public Object valueArrayDeserialize(DataInput in, int size) throws IOException {
            DataIO.DataInputInternal i = (DataIO.DataInputInternal) in;
            int[] ret = new int[size];
            i.unpackIntArray(ret, 0, size);
            return ret;
        }

        @Override
        public int fixedSize() {
            return -1;
        }

    };

    /** packs Integer so small values occupy less than 4 bytes. Large (positive and negative)
     * values could occupy more 4 to 5 bytes. It uses zigzag conversion before packing,
     * number is multiplied by two, with last bite indicating negativity.
     */
    public static final Serializer<Integer> INTEGER_PACKED_ZIGZAG = new IntegerSerializer(){

        long wrap(int i){
            long plus = i<0?1:0; //this could be improved by eliminating condition
            return Math.abs(i*2)+plus;
        }

        int unwrap(long i){
            long m = 1 - 2 * (i&1); // +1 if even, -1 if odd
            return (int) ((i>>>1) * m);
        }

        @Override
        public void serialize(DataOutput out, Integer value) throws IOException {
            ((DataIO.DataOutputByteArray) out).packLong(wrap(value));
        }

        @Override
        public Integer deserialize(DataInput in, int available) throws IOException {
            return unwrap(((DataIO.DataInputInternal)in).unpackLong());
        }

        @Override
        public void valueArraySerialize(DataOutput out, Object vals) throws IOException {
            DataIO.DataOutputByteArray out2 = (DataIO.DataOutputByteArray) out;
            for(int o:(int[]) vals){
                out2.packLong(wrap(o));
            }
        }

        @Override
        public Object valueArrayDeserialize(DataInput in, int size) throws IOException {
            DataIO.DataInputInternal i = (DataIO.DataInputInternal) in;
            int[] ret = new int[size];
            for(int a=0;a<size;a++){
                ret[a] = unwrap(i.unpackLong());
            }
            return ret;
        }

        @Override
        public int fixedSize() {
            return -1;
        }
    };


    public static final Serializer<Boolean> BOOLEAN = new BooleanSer();

    protected static class BooleanSer extends Serializer<Boolean> {

        @Override
        public void serialize(DataOutput out, Boolean value) throws IOException {
            out.writeBoolean(value);
        }

        @Override
        public Boolean deserialize(DataInput in, int available) throws IOException {
            return in.readBoolean();
        }

        @Override
        public int fixedSize() {
            return 1;
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public void valueArraySerialize(DataOutput out, Object vals) throws IOException {
            for(boolean b:((boolean[])vals)){
                out.writeBoolean(b);
            }
        }

        @Override
        public Object valueArrayDeserialize(DataInput in, int size) throws IOException {
            boolean[] ret = new boolean[size];
            for(int i=0;i<size;i++){
                ret[i] = in.readBoolean();
            }
            return ret;
        }

        @Override
        public Boolean valueArrayGet(Object vals, int pos) {
            return ((boolean[])vals)[pos];
        }

        @Override
        public int valueArraySize(Object vals) {
            return ((boolean[])vals).length;
        }

        @Override
        public Object valueArrayEmpty() {
            return new boolean[0];
        }

        @Override
        public Object valueArrayPut(Object vals, int pos, Boolean newValue) {
            boolean[] array = (boolean[]) vals;
            final boolean[] ret = Arrays.copyOf(array, array.length+1);
            if(pos<array.length){
                System.arraycopy(array, pos, ret, pos+1, array.length-pos);
            }
            ret[pos] = newValue;
            return ret;

        }

        @Override
        public Object valueArrayUpdateVal(Object vals, int pos, Boolean newValue) {
            boolean[] vals2 = ((boolean[])vals).clone();
            vals2[pos] = newValue;
            return vals2;

        }

        @Override
        public Object valueArrayFromArray(Object[] objects) {
            boolean[] ret = new boolean[objects.length];
            for(int i=0;i<ret.length;i++){
                ret[i] = (Boolean)objects[i];
            }
            return ret;
        }

        @Override
        public Object valueArrayCopyOfRange(Object vals, int from, int to) {
            return Arrays.copyOfRange((boolean[]) vals, from, to);
        }

        @Override
        public Object valueArrayDeleteValue(Object vals, int pos) {
            boolean[] valsOrig = (boolean[]) vals;
            boolean[] vals2 = new boolean[valsOrig.length-1];
            System.arraycopy(vals,0,vals2, 0, pos-1);
            System.arraycopy(vals, pos, vals2, pos-1, vals2.length-(pos-1));
            return vals2;

        }
    };


    /** Packs recid + it adds 3bits checksum. */

    public static final Serializer<Long> RECID = new EightByteSerializer<Long>() {

        @Override
        public void serialize(DataOutput out, Long value) throws IOException {
            DataIO.packRecid(out, value);
        }

        @Override
        public Long deserialize(DataInput in, int available) throws IOException {
            return DataIO.unpackRecid(in);
        }

        @Override
        public int fixedSize() {
            return -1;
        }

        @Override
        protected Long unpack(long l) {
            return l;
        }

        @Override
        protected long pack(Long l) {
            return l;
        }

        @Override
        public boolean isTrusted() {
            return true;
        }


        @Override
        public void valueArraySerialize(DataOutput out, Object vals) throws IOException {
            for(long o:(long[]) vals){
                DataIO.packRecid(out,o);
            }
        }

        @Override
        public Object valueArrayDeserialize(DataInput in, int size) throws IOException {
            long[] ret = new long[size];
            for(int i=0;i<size;i++){
                ret[i] = DataIO.unpackRecid(in);
            }
            return ret;
        }
    };

    public static final Serializer<long[]> RECID_ARRAY = new Serializer<long[]>() {
        @Override
        public void serialize(DataOutput out, long[] value) throws IOException {
            DataIO.packInt(out,value.length);
            for(long recid:value){
                DataIO.packRecid(out,recid);
            }
        }

        @Override
        public long[] deserialize(DataInput in, int available) throws IOException {
            int size = DataIO.unpackInt(in);
            long[] ret = new long[size];
            for(int i=0;i<size;i++){
                ret[i] = DataIO.unpackRecid(in);
            }
            return ret;
        };


        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public boolean equals(long[] a1, long[] a2) {
            return Arrays.equals(a1,a2);
        }

        @Override
        public int hashCode(long[] bytes, int seed) {
            return LONG_ARRAY.hashCode(bytes,seed);
        }

    };

    /**
     * Always throws {@link IllegalAccessError} when invoked. Useful for testing and assertions.
     */
    public static final Serializer<Object> ILLEGAL_ACCESS = new Serializer<Object>() {
        @Override
        public void serialize(DataOutput out, Object value) throws IOException {
            throw new IllegalAccessError();
        }

        @Override
        public Object deserialize(DataInput in, int available) throws IOException {
            throw new IllegalAccessError();
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

    };


    /**
     * Serializes {@code byte[]} it adds header which contains size information
     */
    public static final Serializer<byte[] > BYTE_ARRAY = new Serializer<byte[]>() {

        @Override
        public void serialize(DataOutput out, byte[] value) throws IOException {
            DataIO.packInt(out,value.length);
            out.write(value);
        }

        @Override
        public byte[] deserialize(DataInput in, int available) throws IOException {
            int size = DataIO.unpackInt(in);
            byte[] ret = new byte[size];
            in.readFully(ret);
            return ret;
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public boolean equals(byte[] a1, byte[] a2) {
            return Arrays.equals(a1,a2);
        }

        public int hashCode(byte[] bytes, int seed) {
            return DataIO.longHash(
                    DataIO.hash(bytes, 0, bytes.length, seed));
        }

        @Override
        public BTreeKeySerializer getBTreeKeySerializer(Comparator comparator) {
            if(comparator!=null && comparator!=Fun.COMPARATOR) {
                return super.getBTreeKeySerializer(comparator);
            }
            return BTreeKeySerializer.BYTE_ARRAY;
        }
    } ;

    /**
     * Serializes {@code byte[]} directly into underlying store
     * It does not store size, so it can not be used in Maps and other collections.
     */
    public static final Serializer<byte[] > BYTE_ARRAY_NOSIZE = new Serializer<byte[]>() {

        @Override
        public void serialize(DataOutput out, byte[] value) throws IOException {
            out.write(value);
        }

        @Override
        public byte[] deserialize(DataInput in, int available) throws IOException {
            byte[] ret = new byte[available];
            in.readFully(ret);
            return ret;
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public boolean equals(byte[] a1, byte[] a2) {
            return Arrays.equals(a1,a2);
        }

        @Override
        public int hashCode(byte[] bytes, int seed) {
            return BYTE_ARRAY.hashCode(bytes, seed);
        }

        @Override
        public BTreeKeySerializer getBTreeKeySerializer(Comparator comparator) {
            if(comparator!=null && comparator!=Fun.COMPARATOR) {
                return super.getBTreeKeySerializer(comparator);
            }
            return BTreeKeySerializer.BYTE_ARRAY;
        }

    } ;

    /**
     * Serializes {@code char[]} it adds header which contains size information
     */
    public static final Serializer<char[] > CHAR_ARRAY = new Serializer<char[]>() {

        @Override
        public void serialize(DataOutput out, char[] value) throws IOException {
            DataIO.packInt(out, value.length);
            for(char c:value){
                out.writeChar(c);
            }
        }

        @Override
        public char[] deserialize(DataInput in, int available) throws IOException {
            final int size = DataIO.unpackInt(in);
            char[] ret = new char[size];
            for(int i=0;i<size;i++){
                ret[i] = in.readChar();
            }
            return ret;
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public boolean equals(char[] a1, char[] a2) {
            return Arrays.equals(a1,a2);
        }

        @Override
        public int hashCode(char[] bytes, int seed) {
            return DataIO.longHash(
                    DataIO.hash(bytes, 0, bytes.length, seed));
        }


    };


    /**
     * Serializes {@code int[]} it adds header which contains size information
     */
    public static final Serializer<int[] > INT_ARRAY = new Serializer<int[]>() {

        @Override
        public void serialize(DataOutput out, int[] value) throws IOException {
            DataIO.packInt(out,value.length);
            for(int c:value){
                out.writeInt(c);
            }
        }

        @Override
        public int[] deserialize(DataInput in, int available) throws IOException {
            final int size = DataIO.unpackInt(in);
            int[] ret = new int[size];
            for(int i=0;i<size;i++){
                ret[i] = in.readInt();
            }
            return ret;
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public boolean equals(int[] a1, int[] a2) {
            return Arrays.equals(a1,a2);
        }

        @Override
        public int hashCode(int[] bytes, int seed) {
            for (int i : bytes) {
                seed = (-1640531527) * seed + i;
            }
            return seed;
        }


    };

    /**
     * Serializes {@code long[]} it adds header which contains size information
     */
    public static final Serializer<long[] > LONG_ARRAY = new Serializer<long[]>() {

        @Override
        public void serialize(DataOutput out, long[] value) throws IOException {
            DataIO.packInt(out,value.length);
            for(long c:value){
                out.writeLong(c);
            }
        }

        @Override
        public long[] deserialize(DataInput in, int available) throws IOException {
            final int size = DataIO.unpackInt(in);
            long[] ret = new long[size];
            for(int i=0;i<size;i++){
                ret[i] = in.readLong();
            }
            return ret;
        }


        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public boolean equals(long[] a1, long[] a2) {
            return Arrays.equals(a1,a2);
        }

        @Override
        public int hashCode(long[] bytes, int seed) {
            for (long element : bytes) {
                int elementHash = (int)(element ^ (element >>> 32));
                seed = (-1640531527) * seed + elementHash;
            }
            return seed;
        }


    };

    /**
     * Serializes {@code double[]} it adds header which contains size information
     */
    public static final Serializer<double[] > DOUBLE_ARRAY = new Serializer<double[]>() {

        @Override
        public void serialize(DataOutput out, double[] value) throws IOException {
            DataIO.packInt(out,value.length);
            for(double c:value){
                out.writeDouble(c);
            }
        }

        @Override
        public double[] deserialize(DataInput in, int available) throws IOException {
            final int size = DataIO.unpackInt(in);
            double[] ret = new double[size];
            for(int i=0;i<size;i++){
                ret[i] = in.readDouble();
            }
            return ret;
        }


        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public boolean equals(double[] a1, double[] a2) {
            return Arrays.equals(a1,a2);
        }

        @Override
        public int hashCode(double[] bytes, int seed) {
            for (double element : bytes) {
                long bits = Double.doubleToLongBits(element);
                seed = (-1640531527) * seed + (int)(bits ^ (bits >>> 32));
            }
            return seed;
        }


    };


    /** Serializer which uses standard Java Serialization with {@link java.io.ObjectInputStream} and {@link java.io.ObjectOutputStream} */
    public static final Serializer<Object> JAVA = new Serializer<Object>() {
        @Override
        public void serialize(DataOutput out, Object value) throws IOException {
            ObjectOutputStream out2 = new ObjectOutputStream((OutputStream) out);
            out2.writeObject(value);
            out2.flush();
        }

        @Override
        public Object deserialize(DataInput in, int available) throws IOException {
            try {
                ObjectInputStream in2 = new ObjectInputStream(new DataIO.DataInputToStream(in));
                return in2.readObject();
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }

    };

    /** Serializers {@link java.util.UUID} class */
    public static final Serializer<java.util.UUID> UUID = new Serializer<java.util.UUID>() {
        @Override
        public void serialize(DataOutput out, UUID value) throws IOException {
            out.writeLong(value.getMostSignificantBits());
            out.writeLong(value.getLeastSignificantBits());
        }

        @Override
        public UUID deserialize(DataInput in, int available) throws IOException {
            return new UUID(in.readLong(), in.readLong());
        }

        @Override
        public int fixedSize() {
            return 16;
        }

        @Override
        public boolean isTrusted() {
            return true;
        }


        @Override
        public boolean equals(UUID a1, UUID a2) {
            //on java6 equals method is not thread safe
            return a1==a2 || (a1!=null && a1.getLeastSignificantBits() == a2.getLeastSignificantBits()
                    && a1.getMostSignificantBits()==a2.getMostSignificantBits());
        }

        @Override
        public int hashCode(UUID uuid, int seed) {
            //on java6 uuid.hashCode is not thread safe. This is workaround
            long a = uuid.getLeastSignificantBits() ^ uuid.getMostSignificantBits();
            return ((int)(a>>32))^(int) a;

        }


        @Override
        public void valueArraySerialize(DataOutput out, Object vals) throws IOException {
            for(long o:(long[]) vals){
                out.writeLong(o);
            }
        }

        @Override
        public Object valueArrayDeserialize(DataInput in, int size) throws IOException {
            size*=2;
            long[] ret = new long[size];
            for(int i=0;i<size;i++){
                ret[i] = in.readLong();
            }
            return ret;
        }

        @Override
        public UUID valueArrayGet(Object vals, int pos){
            long[] v = (long[])vals;
            pos*=2;
            return new UUID(v[pos++],v[pos]);
        }

        @Override
        public int valueArraySize(Object vals){
            return ((long[])vals).length/2;
        }

        @Override
        public Object valueArrayEmpty(){
            return new long[0];
        }

        @Override
        public Object valueArrayPut(Object vals, int pos, UUID newValue) {
            pos*=2;

            long[] array = (long[]) vals;
            final long[] ret = Arrays.copyOf(array, array.length+2);

            if(pos<array.length){
                System.arraycopy(array, pos, ret, pos+2, array.length-pos);
            }
            ret[pos++] = newValue.getMostSignificantBits();
            ret[pos] = newValue.getLeastSignificantBits();
            return ret;
        }

        @Override
        public Object valueArrayUpdateVal(Object vals, int pos, UUID newValue) {
            pos*=2;
            long[] vals2 = ((long[])vals).clone();
            vals2[pos++] = newValue.getMostSignificantBits();
            vals2[pos] = newValue.getLeastSignificantBits();
            return vals2;
        }


        @Override
        public Object valueArrayFromArray(Object[] objects) {
            long[] ret = new long[objects.length*2];
            int pos=0;

            for(Object o:objects){
                UUID uuid = (java.util.UUID) o;
                ret[pos++] = uuid.getMostSignificantBits();
                ret[pos++] = uuid.getLeastSignificantBits();
            }

            return ret;
        }

        @Override
        public Object valueArrayCopyOfRange(Object vals, int from, int to) {
            return Arrays.copyOfRange((long[])vals, from*2, to*2);
        }

        @Override
        public Object valueArrayDeleteValue(Object vals, int pos) {
            pos*=2;
            long[] valsOrig = (long[]) vals;
            long[] vals2 = new long[valsOrig.length-2];
            System.arraycopy(vals,0,vals2, 0, pos-2);
            System.arraycopy(vals, pos, vals2, pos-2, vals2.length-(pos-2));
            return vals2;
        }

        @Override
        public BTreeKeySerializer getBTreeKeySerializer(Comparator comparator) {
            if(comparator!=null && comparator!=Fun.COMPARATOR) {
                return super.getBTreeKeySerializer(comparator);
            }
            return BTreeKeySerializer.UUID;
        }
    };

    public static final Serializer<Byte> BYTE = new Serializer<Byte>() {
        @Override
        public void serialize(DataOutput out, Byte value) throws IOException {
            out.writeByte(value); //TODO test all new serialziers
        }

        @Override
        public Byte deserialize(DataInput in, int available) throws IOException {
            return in.readByte();
        }

        @Override
        public int fixedSize() {
            return 1;
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

    } ;

    public static final Serializer<Float> FLOAT = new FourByteSerializer<Float>() {

        @Override
        protected Float unpack(int l) {
            return Float.intBitsToFloat(l);
        }

        @Override
        protected int pack(Float l) {
            return Float.floatToIntBits(l);
        }

        @Override
        public void serialize(DataOutput out, Float value) throws IOException {
            out.writeFloat(value); //TODO test all new serialziers
        }

        @Override
        public Float deserialize(DataInput in, int available) throws IOException {
            return in.readFloat();
        }

    } ;


    public static final Serializer<Double> DOUBLE = new EightByteSerializer<Double>() {
        @Override
        protected Double unpack(long l) {
            return Double.longBitsToDouble(l);
        }

        @Override
        protected long pack(Double l) {
            return Double.doubleToLongBits(l);
        }

        @Override
        public void serialize(DataOutput out, Double value) throws IOException {
            out.writeDouble(value);
        }

        @Override
        public Double deserialize(DataInput in, int available) throws IOException {
            return in.readDouble();
        }

    } ;

    public static final Serializer<Short> SHORT = new Serializer<Short>() {
        @Override
        public void serialize(DataOutput out, Short value) throws IOException {
            out.writeShort(value.shortValue());
        }

        @Override
        public Short deserialize(DataInput in, int available) throws IOException {
            return in.readShort();
        }

        @Override
        public int fixedSize() {
            return 2;
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

    } ;

    public static final Serializer<boolean[]> BOOLEAN_ARRAY = new Serializer<boolean[]>() {
        @Override
        public void serialize(DataOutput out, boolean[] value) throws IOException {
            DataIO.packInt(out, value.length);//write the number of booleans not the number of bytes
            SerializerBase.writeBooleanArray(out,value);
        }

        @Override
        public boolean[] deserialize(DataInput in, int available) throws IOException {
            int size = DataIO.unpackInt(in);
            return SerializerBase.readBooleanArray(size, in);
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public boolean equals(boolean[] a1, boolean[] a2) {
            return Arrays.equals(a1,a2);
        }

        @Override
        public int hashCode(boolean[] booleans, int seed) {
            return Arrays.hashCode(booleans);
        }
    };



    public static final Serializer<short[]> SHORT_ARRAY = new Serializer<short[]>() {
        @Override
        public void serialize(DataOutput out, short[] value) throws IOException {
            DataIO.packInt(out,value.length);
            for(short v:value){
                out.writeShort(v);
            }
        }

        @Override
        public short[] deserialize(DataInput in, int available) throws IOException {
            short[] ret = new short[DataIO.unpackInt(in)];
            for(int i=0;i<ret.length;i++){
                ret[i] = in.readShort();
            }
            return ret;
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public boolean equals(short[] a1, short[] a2) {
            return Arrays.equals(a1, a2);
        }

        @Override
        public int hashCode(short[] shorts, int seed) {
            for (short element : shorts)
                seed = (-1640531527) * seed + element;
            return seed;
        }
    };


    public static final Serializer<float[]> FLOAT_ARRAY = new Serializer<float[]>() {
        @Override
        public void serialize(DataOutput out, float[] value) throws IOException {
            DataIO.packInt(out,value.length);
            for(float v:value){
                out.writeFloat(v);
            }
        }

        @Override
        public float[] deserialize(DataInput in, int available) throws IOException {
            float[] ret = new float[DataIO.unpackInt(in)];
            for(int i=0;i<ret.length;i++){
                ret[i] = in.readFloat();
            }
            return ret;
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public boolean equals(float[] a1, float[] a2) {
            return Arrays.equals(a1, a2);
        }

        @Override
        public int hashCode(float[] floats, int seed) {
            for (float element : floats)
                seed = (-1640531527) * seed + Float.floatToIntBits(element);
            return seed;
        }
    };

    public static final Serializer<BigInteger> BIG_INTEGER = new Serializer<BigInteger>() {
        @Override
        public void serialize(DataOutput out, BigInteger value) throws IOException {
            BYTE_ARRAY.serialize(out, value.toByteArray());
        }

        @Override
        public BigInteger deserialize(DataInput in, int available) throws IOException {
            return new BigInteger(BYTE_ARRAY.deserialize(in,available));
        }

        @Override
        public boolean isTrusted() {
            return true;
        }
    } ;

    public static final Serializer<BigDecimal> BIG_DECIMAL = new Serializer<BigDecimal>() {
        @Override
        public void serialize(DataOutput out, BigDecimal value) throws IOException {
            BYTE_ARRAY.serialize(out,value.unscaledValue().toByteArray());
            DataIO.packInt(out, value.scale());
        }

        @Override
        public BigDecimal deserialize(DataInput in, int available) throws IOException {
            return new BigDecimal(new BigInteger(
                    BYTE_ARRAY.deserialize(in,-1)),
                    DataIO.unpackInt(in));
        }

        @Override
        public boolean isTrusted() {
            return true;
        }
    } ;


    public static final Serializer<Class<?>> CLASS = new Serializer<Class<?>>() {

        @Override
        public void serialize(DataOutput out, Class<?> value) throws IOException {
            out.writeUTF(value.getName());
        }

        @Override
        public Class<?> deserialize(DataInput in, int available) throws IOException {
            //TODO this should respect registered ClassLoaders from DBMaker.serializerRegisterClasses()
            return SerializerPojo.DEFAULT_CLASS_LOADER.run(in.readUTF());
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public boolean equals(Class<?> a1, Class<?> a2) {
            return a1==a2 || (a1.toString().equals(a2.toString()));
        }

        @Override
        public int hashCode(Class<?> aClass, int seed) {
            //class does not override identity hash code
            return aClass.toString().hashCode();
        }
    };

    public static final Serializer<Date> DATE = new EightByteSerializer<Date>() {

        @Override
        public void serialize(DataOutput out, Date value) throws IOException {
            out.writeLong(value.getTime());
        }

        @Override
        public Date deserialize(DataInput in, int available) throws IOException {
            return new Date(in.readLong());
        }

        @Override
        protected Date unpack(long l) {
            return new Date(l);
        }

        @Override
        protected long pack(Date l) {
            return l.getTime();
        }
    };



    /** wraps another serializer and (de)compresses its output/input*/
    public final static class CompressionWrapper<E> extends Serializer<E> implements Serializable {

        private static final long serialVersionUID = 4440826457939614346L;
        protected final Serializer<E> serializer;
        protected final ThreadLocal<CompressLZF> LZF = new ThreadLocal<CompressLZF>() {
            @Override protected CompressLZF initialValue() {
                return new CompressLZF();
            }
        };

        // this flag is here for compatibility with 2.0-beta1 and beta2. Value compression was not added back then
        // this flag should be removed some time in future, and replaced with default value 'true'.
        // value 'false' is format used in 2.0
        protected final boolean compressValues;

        public CompressionWrapper(Serializer<E> serializer) {
            this.serializer = serializer;
            this.compressValues = true;
        }


        /** used for deserialization */
        @SuppressWarnings("unchecked")
		protected CompressionWrapper(SerializerBase serializerBase, DataInput is, SerializerBase.FastArrayList<Object> objectStack, boolean compressValues) throws IOException {
            objectStack.add(this);
            this.serializer = (Serializer<E>) serializerBase.deserialize(is,objectStack);
            this.compressValues = compressValues;
        }


        @Override
        public void serialize(DataOutput out, E value) throws IOException {
            DataIO.DataOutputByteArray out2 = new DataIO.DataOutputByteArray();
            serializer.serialize(out2,value);

            byte[] tmp = new byte[out2.pos+41];
            int newLen;
            try{
                newLen = LZF.get().compress(out2.buf,out2.pos,tmp,0);
            }catch(IndexOutOfBoundsException e){
                newLen=0; //larger after compression
            }
            if(newLen>=out2.pos||newLen==0){
                //compression adds size, so do not compress
                DataIO.packInt(out,0);
                out.write(out2.buf,0,out2.pos);
                return;
            }

            DataIO.packInt(out, out2.pos+1); //unpacked size, zero indicates no compression
            out.write(tmp,0,newLen);
        }

        @Override
        public E deserialize(DataInput in, int available) throws IOException {
            final int unpackedSize = DataIO.unpackInt(in)-1;
            if(unpackedSize==-1){
                //was not compressed
                return serializer.deserialize(in, available>0?available-1:available);
            }

            byte[] unpacked = new byte[unpackedSize];
            LZF.get().expand(in,unpacked,0,unpackedSize);
            DataIO.DataInputByteArray in2 = new DataIO.DataInputByteArray(unpacked);
            E ret =  serializer.deserialize(in2,unpackedSize);
            if(CC.ASSERT && ! (in2.pos==unpackedSize))
                throw new DBException.DataCorruption( "data were not fully read");
            return ret;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CompressionWrapper<?> that = (CompressionWrapper<?>) o;
            return serializer.equals(that.serializer) && compressValues == that.compressValues;
        }

        @Override
        public int hashCode() {
            return serializer.hashCode()+(compressValues ?1:0);
        }

        @Override
        public boolean isTrusted() {
            return true;
        }


        @Override
        public void valueArraySerialize(DataOutput out, Object vals) throws IOException {
            if(!compressValues) {
                super.valueArraySerialize(out, vals);
                return;
            }

            DataIO.DataOutputByteArray out2 = new DataIO.DataOutputByteArray();
            serializer.valueArraySerialize(out2, vals);

            if(out2.pos==0)
                return;


            byte[] tmp = new byte[out2.pos+41];
            int newLen;
            try{
                newLen = LZF.get().compress(out2.buf,out2.pos,tmp,0);
            }catch(IndexOutOfBoundsException e){
                newLen=0; //larger after compression
            }
            if(newLen>=out2.pos||newLen==0){
                //compression adds size, so do not compress
                DataIO.packInt(out,0);
                out.write(out2.buf,0,out2.pos);
                return;
            }

            DataIO.packInt(out, out2.pos+1); //unpacked size, zero indicates no compression
            out.write(tmp,0,newLen);
        }

        @Override
        public Object valueArrayDeserialize(DataInput in, int size) throws IOException {
            if(!compressValues) {
                return super.valueArrayDeserialize(in, size);
            }

            if(size==0)
                return serializer.valueArrayEmpty();

            final int unpackedSize = DataIO.unpackInt(in)-1;
            if(unpackedSize==-1){
                //was not compressed
                return serializer.valueArrayDeserialize(in,size);
            }

            byte[] unpacked = new byte[unpackedSize];
            LZF.get().expand(in,unpacked,0,unpackedSize);
            DataIO.DataInputByteArray in2 = new DataIO.DataInputByteArray(unpacked);
            Object ret =  serializer.valueArrayDeserialize(in2, size);
            if(CC.ASSERT && ! (in2.pos==unpackedSize))
                throw new DBException.DataCorruption( "data were not fully read");
            return ret;
        }

        @Override
        public E valueArrayGet(Object vals, int pos) {
            return compressValues ?
                    serializer.valueArrayGet(vals, pos):
                    super.valueArrayGet(vals, pos);
        }

        @Override
        public int valueArraySize(Object vals) {
            return compressValues ?
                    serializer.valueArraySize(vals):
                    super.valueArraySize(vals);
        }

        @Override
        public Object valueArrayEmpty() {
            return compressValues ?
                    serializer.valueArrayEmpty():
                    super.valueArrayEmpty();
        }

        @Override
        public Object valueArrayPut(Object vals, int pos, E newValue) {
            return compressValues ?
                serializer.valueArrayPut(vals, pos, newValue):
                super.valueArrayPut(vals, pos, newValue);
        }

        @Override
        public Object valueArrayUpdateVal(Object vals, int pos, E newValue) {
            return compressValues ?
                    serializer.valueArrayUpdateVal(vals, pos, newValue):
                    super.valueArrayUpdateVal(vals, pos, newValue);
        }

        @Override
        public Object valueArrayFromArray(Object[] objects) {
            return compressValues ?
                    serializer.valueArrayFromArray(objects):
                    super.valueArrayFromArray(objects);
        }

        @Override
        public Object valueArrayCopyOfRange(Object vals, int from, int to) {
            return compressValues ?
                    serializer.valueArrayCopyOfRange(vals, from, to):
                    super.valueArrayCopyOfRange(vals, from, to);
        }

        @Override
        public Object valueArrayDeleteValue(Object vals, int pos) {
            return compressValues ?
                    serializer.valueArrayDeleteValue(vals, pos):
                    super.valueArrayDeleteValue(vals, pos);
        }

        @Override
        public BTreeKeySerializer getBTreeKeySerializer(Comparator comparator) {
            //TODO compress BTreeKey serializer?
            return serializer.getBTreeKeySerializer(comparator);
        }

    }


    /** wraps another serializer and (de)compresses its output/input using Deflate*/
    public final static class CompressionDeflateWrapper<E> extends Serializer<E> implements Serializable {

        private static final long serialVersionUID = 8529699349939823553L;
        protected final Serializer<E> serializer;
        protected final int compressLevel;
        protected final byte[] dictionary;

        public CompressionDeflateWrapper(Serializer<E> serializer) {
            this(serializer, Deflater.DEFAULT_STRATEGY, null);
        }

        public CompressionDeflateWrapper(Serializer<E> serializer, int compressLevel, byte[] dictionary) {
            this.serializer = serializer;
            this.compressLevel = compressLevel;
            this.dictionary = dictionary==null || dictionary.length==0 ? null : dictionary;
        }

        /** used for deserialization */
        @SuppressWarnings("unchecked")
        protected CompressionDeflateWrapper(SerializerBase serializerBase, DataInput is, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            this.serializer = (Serializer<E>) serializerBase.deserialize(is,objectStack);
            this.compressLevel = is.readByte();
            int dictlen = DataIO.unpackInt(is);
            if(dictlen==0) {
                dictionary = null;
            } else {
                byte[] d = new byte[dictlen];
                is.readFully(d);
                dictionary = d;
            }
        }


        @Override
        public void serialize(DataOutput out, E value) throws IOException {
            DataIO.DataOutputByteArray out2 = new DataIO.DataOutputByteArray();
            serializer.serialize(out2,value);

            byte[] tmp = new byte[out2.pos+41];
            int newLen;
            try{
                Deflater deflater = new Deflater(compressLevel);
                if(dictionary!=null) {
                    deflater.setDictionary(dictionary);
                }

                deflater.setInput(out2.buf,0,out2.pos);
                deflater.finish();
                newLen = deflater.deflate(tmp);
                //LZF.get().compress(out2.buf,out2.pos,tmp,0);
            }catch(IndexOutOfBoundsException e){
                newLen=0; //larger after compression
            }
            if(newLen>=out2.pos||newLen==0){
                //compression adds size, so do not compress
                DataIO.packInt(out,0);
                out.write(out2.buf,0,out2.pos);
                return;
            }

            DataIO.packInt(out, out2.pos+1); //unpacked size, zero indicates no compression
            out.write(tmp,0,newLen);
        }

        @Override
        public E deserialize(DataInput in, int available) throws IOException {
            final int unpackedSize = DataIO.unpackInt(in)-1;
            if(unpackedSize==-1){
                //was not compressed
                return serializer.deserialize(in, available>0?available-1:available);
            }

            Inflater inflater = new Inflater();
            if(dictionary!=null) {
                inflater.setDictionary(dictionary);
            }

            InflaterInputStream in4 = new InflaterInputStream(
                    new DataIO.DataInputToStream(in), inflater);

            byte[] unpacked = new byte[unpackedSize];
            in4.read(unpacked,0,unpackedSize);

            DataIO.DataInputByteArray in2 = new DataIO.DataInputByteArray(unpacked);
            E ret =  serializer.deserialize(in2,unpackedSize);
            if(CC.ASSERT && ! (in2.pos==unpackedSize))
                throw new DBException.DataCorruption( "data were not fully read");
            return ret;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CompressionDeflateWrapper<?> that = (CompressionDeflateWrapper<?>) o;

            if (compressLevel != that.compressLevel) return false;
            if (!serializer.equals(that.serializer)) return false;
            return Arrays.equals(dictionary, that.dictionary);

        }

        @Override
        public int hashCode() {
            int result = serializer.hashCode();
            result = 31 * result + compressLevel;
            result = 31 * result + (dictionary != null ? Arrays.hashCode(dictionary) : 0);
            return result;
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public void valueArraySerialize(DataOutput out, Object vals) throws IOException {
            DataIO.DataOutputByteArray out2 = new DataIO.DataOutputByteArray();
            serializer.valueArraySerialize(out2,vals);
            if(out2.pos==0)
                return;

            byte[] tmp = new byte[out2.pos+41];
            int newLen;
            try{
                Deflater deflater = new Deflater(compressLevel);
                if(dictionary!=null) {
                    deflater.setDictionary(dictionary);
                }

                deflater.setInput(out2.buf,0,out2.pos);
                deflater.finish();
                newLen = deflater.deflate(tmp);
                //LZF.get().compress(out2.buf,out2.pos,tmp,0);
            }catch(IndexOutOfBoundsException e){
                newLen=0; //larger after compression
            }
            if(newLen>=out2.pos||newLen==0){
                //compression adds size, so do not compress
                DataIO.packInt(out,0);
                out.write(out2.buf,0,out2.pos);
                return;
            }

            DataIO.packInt(out, out2.pos+1); //unpacked size, zero indicates no compression
            out.write(tmp,0,newLen);
        }

        @Override
        public Object valueArrayDeserialize(DataInput in, int size) throws IOException {
            if(size==0) {
                return serializer.valueArrayEmpty();
            }

            //decompress all values in single blob, it has better compressibility
            final int unpackedSize = DataIO.unpackInt(in)-1;
            if(unpackedSize==-1){
                //was not compressed
                return serializer.valueArrayDeserialize(in,size);
            }

            Inflater inflater = new Inflater();
            if(dictionary!=null) {
                inflater.setDictionary(dictionary);
            }

            InflaterInputStream in4 = new InflaterInputStream(
                    new DataIO.DataInputToStream(in), inflater);

            byte[] unpacked = new byte[unpackedSize];
            in4.read(unpacked,0,unpackedSize);

            //now got data unpacked, so use serializer to deal with it

            DataIO.DataInputByteArray in2 = new DataIO.DataInputByteArray(unpacked);
            Object ret =  serializer.valueArrayDeserialize(in2, size);
            if(CC.ASSERT && ! (in2.pos==unpackedSize))
                throw new DBException.DataCorruption( "data were not fully read");
            return ret;
        }

        @Override
        public E valueArrayGet(Object vals, int pos) {
            return serializer.valueArrayGet(vals, pos);
        }

        @Override
        public int valueArraySize(Object vals) {
            return serializer.valueArraySize(vals);
        }

        @Override
        public Object valueArrayEmpty() {
            return serializer.valueArrayEmpty();
        }

        @Override
        public Object valueArrayPut(Object vals, int pos, E newValue) {
            return serializer.valueArrayPut(vals, pos, newValue);
        }

        @Override
        public Object valueArrayUpdateVal(Object vals, int pos, E newValue) {
            return serializer.valueArrayUpdateVal(vals, pos, newValue);
        }

        @Override
        public Object valueArrayFromArray(Object[] objects) {
            return serializer.valueArrayFromArray(objects);
        }

        @Override
        public Object valueArrayCopyOfRange(Object vals, int from, int to) {
            return serializer.valueArrayCopyOfRange(vals, from, to);
        }

        @Override
        public Object valueArrayDeleteValue(Object vals, int pos) {
            return serializer.valueArrayDeleteValue(vals, pos);
        }

        @Override
        public BTreeKeySerializer getBTreeKeySerializer(Comparator comparator) {
            //TODO compress BTreeKey serializer?
            return serializer.getBTreeKeySerializer(comparator);
        }
    }

    public static final class Array<T> extends Serializer<T[]> implements  Serializable{

		private static final long serialVersionUID = -7443421486382532062L;
		protected final Serializer<T> serializer;

        public Array(Serializer<T> serializer) {
            if(serializer==null)
                throw new NullPointerException("null serializer");
            this.serializer = serializer;
        }

        /** used for deserialization */
        @SuppressWarnings("unchecked")
		protected Array(SerializerBase serializerBase, DataInput is, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            this.serializer = (Serializer<T>) serializerBase.deserialize(is,objectStack);
        }


        @Override
        public void serialize(DataOutput out, T[] value) throws IOException {
            DataIO.packInt(out,value.length);
            for(T a:value){
                serializer.serialize(out,a);
            }
        }

        @Override
        public T[] deserialize(DataInput in, int available) throws IOException {
        	T[] ret =(T[]) new Object[DataIO.unpackInt(in)];
            for(int i=0;i<ret.length;i++){
                ret[i] = serializer.deserialize(in,-1);
            }
            return ret;
            
        }

        @Override
        public boolean isTrusted() {
            return serializer.isTrusted();
        }

        @Override
        public boolean equals(T[] a1, T[] a2) {
            if(a1==a2)
                return true;
            if(a1==null || a1.length!=a2.length)
                return false;

            for(int i=0;i<a1.length;i++){
                if(!serializer.equals(a1[i],a2[i]))
                    return false;
            }
            return true;
        }

        @Override
        public int hashCode(T[] objects, int seed) {
            seed+=objects.length;
            for(T a:objects){
                seed=(-1640531527)*seed+serializer.hashCode(a,seed);
            }
            return seed;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return serializer.equals(((Array<?>) o).serializer);

        }

        @Override
        public int hashCode() {
            return serializer.hashCode();
        }
    }

    //this has to be lazily initialized due to circular dependencies
    static final  class __BasicInstance {
        final static Serializer<Object> s = new SerializerBase();
    }


    /**
     * Basic serializer for most classes in {@code java.lang} and {@code java.util} packages.
     * It does not handle custom POJO classes. It also does not handle classes which
     * require access to {@code DB} itself.
     */
    public static final Serializer<Object> BASIC = new Serializer<Object>(){

        @Override
        public void serialize(DataOutput out, Object value) throws IOException {
            __BasicInstance.s.serialize(out,value);
        }

        @Override
        public Object deserialize(DataInput in, int available) throws IOException {
            return __BasicInstance.s.deserialize(in,available);
        }

        @Override
        public boolean isTrusted() {
            return true;
        }
    };


    /**
     * Serialize the content of an object into a ObjectOutput
     *
     * @param out ObjectOutput to save object into
     * @param value Object to serialize
     *
     * @throws java.io.IOException in case of IO error
     */
    abstract public void serialize(DataOutput out, A value)
            throws IOException;


    /**
     * Deserialize the content of an object from a DataInput.
     *
     * @param in to read serialized data from
     * @param available how many bytes are available in DataInput for reading, may be -1 (in streams) or 0 (null).
     * @return deserialized object
     * @throws java.io.IOException in case of IO error
     */
    abstract public A deserialize( DataInput in, int available)
            throws IOException;

    /**
     * Data could be serialized into record with variable size or fixed size.
     * Some optimizations can be applied to serializers with fixed size
     *
     * @return fixed size or -1 for variable size
     */
    public int fixedSize(){
        return -1;
    }

    /**
     * <p>
     * MapDB has relax record size boundary checking.
     * It expect deserializer to read exactly as many bytes as were writen during serialization.
     * If deserializer reads more bytes it might start reading others record data in store.
     * </p><p>
     * Some serializers (Kryo) have problems with this. To prevent this we can not read
     * data directly from store, but must copy them into separate {@code byte[]}.
     * So zero copy optimalizations is disabled by default, and must be explicitly enabled here.
     * </p><p>
     * This flag indicates if this serializer was 'verified' to read as many bytes as it
     * writes. It should be also much better tested etc.
     * </p>
     *
     * @return true if this serializer is well tested and writes as many bytes as it reads.
     */
    public boolean isTrusted(){
        return false;
    }

    public boolean equals(A a1, A a2){
        return a1==a2 || (a1!=null && a1.equals(a2));
    }

    public int hashCode(A a, int seed){
        return a.hashCode();
    }

    @SuppressWarnings("unchecked")
	public void valueArraySerialize(DataOutput out, Object vals) throws IOException {
        Object[] vals2 = (Object[]) vals;
        for(Object o:vals2){
            serialize(out, (A) o);
        }
    }

    public Object valueArrayDeserialize(DataInput in, int size) throws IOException {
        Object[] ret = new Object[size];
        for(int i=0;i<size;i++){
            ret[i] = deserialize(in,-1);
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
	public A valueArrayGet(Object vals, int pos){
        return (A) ((Object[])vals)[pos];
    }

    public int valueArraySize(Object vals){
        return ((Object[])vals).length;
    }

    public Object valueArrayEmpty(){
        return new Object[0];
    }

    public Object valueArrayPut(Object vals, int pos, A newValue) {
        return BTreeMap.arrayPut((Object[]) vals, pos, newValue);
    }

    public Object valueArrayUpdateVal(Object vals, int pos, A newValue) {
        Object[] vals2 = ((Object[])vals).clone();
        vals2[pos] = newValue;
        return vals2;
    }

    public Object valueArrayFromArray(Object[] objects) {
        return objects;
    }

    public Object valueArrayCopyOfRange(Object vals, int from, int to) {
        return Arrays.copyOfRange((Object[])vals, from, to);
    }

    public Object valueArrayDeleteValue(Object vals, int pos) {
        Object[] valsOrig = (Object[]) vals;
        Object[] vals2 = new Object[valsOrig.length-1];
        System.arraycopy(vals,0,vals2, 0, pos-1);
        System.arraycopy(vals, pos, vals2, pos-1, vals2.length-(pos-1));
        return vals2;
    }

    public BTreeKeySerializer getBTreeKeySerializer(Comparator comparator){
        if(comparator==null)
            comparator = Fun.COMPARATOR;
        return new BTreeKeySerializer.BasicKeySerializer(Serializer.this,comparator);
    }



}
