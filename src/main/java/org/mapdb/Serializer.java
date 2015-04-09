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
package org.mapdb;


import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

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
     * Serializes strings using UTF8 encoding.
     * Stores string size so can be used as collection serializer.
     * Does not handle null values
     */
    public static final Serializer<String> STRING = new Serializer<String>() {
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
        public BTreeKeySerializer getBTreeKeySerializer(boolean descending) {
            if(descending) {
                return super.getBTreeKeySerializer(descending);
            }
            return BTreeKeySerializer.STRING;
        }
    };

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
            char[] cc = new char[value.length()];
            //TODO does this really works? is not char 2 byte unsigned?
            value.getChars(0,cc.length,cc,0);
            DataIO.packInt(out,cc.length);
            for(char c:cc){
                out.write(c);
            }
        }

        @Override
        public String deserialize(DataInput in, int available) throws IOException {
            int size = DataIO.unpackInt(in);
            char[] cc = new char[size];
            for(int i=0;i<size;i++){
                cc[i] = (char) in.readUnsignedByte();
            }
            return new String(cc);
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public BTreeKeySerializer getBTreeKeySerializer(boolean descending) {
            if(descending) {
                return super.getBTreeKeySerializer(descending);
            }
            return BTreeKeySerializer.STRING; //TODO ascii specific serializer?
        }

    };

    /**
     * Serializes strings using UTF8 encoding.
     * Used mainly for testing.
     * Does not handle null values.
     */
    public static final Serializer<String> STRING_NOSIZE = new Serializer<String>() {

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
        public BTreeKeySerializer getBTreeKeySerializer(boolean descending) {
            if(descending) {
                return super.getBTreeKeySerializer(descending);
            }
            return BTreeKeySerializer.STRING;
        }

    };





    /** Serializes Long into 8 bytes, used mainly for testing.
     * Does not handle null values.*/

    public static final Serializer<Long> LONG = new Serializer<Long>() {
        @Override
        public void serialize(DataOutput out, Long value) throws IOException {
            out.writeLong(value);
        }

        @Override
        public Long deserialize(DataInput in, int available) throws IOException {
            return in.readLong();
        }

        @Override
        public int fixedSize() {
            return 8;
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public void valueArraySerialize(DataOutput out, Object vals) throws IOException {
            for(long o:(long[]) vals){
                out.writeLong(o); //TODO pack?
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
        public Long valueArrayGet(Object vals, int pos){
            return ((long[])vals)[pos];
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
        public Object valueArrayPut(Object vals, int pos, Long newValue) {

            long[] array = (long[]) vals;
            final long[] ret = Arrays.copyOf(array, array.length+1);
            if(pos<array.length){
                System.arraycopy(array, pos, ret, pos+1, array.length-pos);
            }
            ret[pos] = newValue;
            return ret;
        }

        @Override
        public Object valueArrayUpdateVal(Object vals, int pos, Long newValue) {
            long[] vals2 = ((long[])vals).clone();
            vals2[pos] = newValue;
            return vals2;
        }

        @Override
        public Object valueArrayFromArray(Object[] objects) {
            long[] ret = new long[objects.length];
            int pos=0;

            for(Object o:objects){
                ret[pos++] = (Long)o;
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
        public BTreeKeySerializer getBTreeKeySerializer(boolean descending) {
            if(descending) {
                return super.getBTreeKeySerializer(descending);
            }
            return BTreeKeySerializer.LONG;
        }

    };

    /** Serializes Integer into 4 bytes.
     * Does not handle null values.*/

    public static final Serializer<Integer> INTEGER = new Serializer<Integer>(){

        @Override
        public void serialize(DataOutput out, Integer value) throws IOException {
            out.writeInt(value);
        }

        @Override
        public Integer deserialize(DataInput in, int available) throws IOException {
            return in.readInt();
        }

        @Override
        public int fixedSize() {
            return 4;
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public void valueArraySerialize(DataOutput out, Object vals) throws IOException {
            for(int o:(int[]) vals){
                out.writeInt(o); //TODO pack?
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

        @Override
        public Integer valueArrayGet(Object vals, int pos){
            return ((int[])vals)[pos];
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
        public Object valueArrayPut(Object vals, int pos, Integer newValue) {

            int[] array = (int[]) vals;
            final int[] ret = Arrays.copyOf(array, array.length+1);
            if(pos<array.length){
                System.arraycopy(array, pos, ret, pos+1, array.length-pos);
            }
            ret[pos] = newValue;
            return ret;
        }

        @Override
        public Object valueArrayUpdateVal(Object vals, int pos, Integer newValue) {
            int[] vals2 = ((int[])vals).clone();
            vals2[pos] = newValue;
            return vals2;
        }

        @Override
        public Object valueArrayFromArray(Object[] objects) {
            int[] ret = new int[objects.length];
            int pos=0;

            for(Object o:objects){
                ret[pos++] = (Integer)o;
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
        public BTreeKeySerializer getBTreeKeySerializer(boolean descending) {
            if(descending) {
                return super.getBTreeKeySerializer(descending);
            }
            return BTreeKeySerializer.INTEGER;
        }

    };


    public static final Serializer<Boolean> BOOLEAN = new Serializer<Boolean>() {
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


    };


    /** Packs recid + it adds 3bits checksum. */

    public static final Serializer<Long> RECID = new Serializer<Long>() {
        @Override
        public void serialize(DataOutput out, Long value) throws IOException {
            long val = value<<3;
            val = DataIO.parity3Set(val);
            DataIO.packLong(out,val);
        }

        @Override
        public Long deserialize(DataInput in, int available) throws IOException {
            long val =  DataIO.unpackLong(in);
            val = DataIO.parity3Get(val);
            return val >>> 3;
        }

        @Override
        public int fixedSize() {
            return 8;
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

        //TODO RECID btree key serializer (long with added parity checks)
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

        @Override
        public int hashCode(byte[] bytes) {
            return Arrays.hashCode(bytes);
        }

        @Override
        public BTreeKeySerializer getBTreeKeySerializer(boolean descending) {
            if(descending) {
                return super.getBTreeKeySerializer(descending);
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
        public int hashCode(byte[] bytes) {
            return Arrays.hashCode(bytes);
        }

        @Override
        public BTreeKeySerializer getBTreeKeySerializer(boolean descending) {
            if(descending) {
                return super.getBTreeKeySerializer(descending);
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
        public int hashCode(char[] bytes) {
            return Arrays.hashCode(bytes);
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
        public int hashCode(int[] bytes) {
            return Arrays.hashCode(bytes);
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
        public int hashCode(long[] bytes) {
            return Arrays.hashCode(bytes);
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
        public int hashCode(double[] bytes) {
            return Arrays.hashCode(bytes);
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
                ObjectInputStream in2 = new ObjectInputStream((InputStream) in);
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
        public int hashCode(UUID uuid) {
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
        public BTreeKeySerializer getBTreeKeySerializer(boolean descending) {
            if(descending) {
                return super.getBTreeKeySerializer(descending);
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
    public static final Serializer<Float> FLOAT = new Serializer<Float>() {
        @Override
        public void serialize(DataOutput out, Float value) throws IOException {
            out.writeFloat(value); //TODO test all new serialziers
        }

        @Override
        public Float deserialize(DataInput in, int available) throws IOException {
            return in.readFloat();
        }

        @Override
        public int fixedSize() {
            return 4;
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

    } ;


    public static final Serializer<Double> DOUBLE = new Serializer<Double>() {
        @Override
        public void serialize(DataOutput out, Double value) throws IOException {
            out.writeDouble(value);
        }

        @Override
        public Double deserialize(DataInput in, int available) throws IOException {
            return in.readDouble();
        }

        @Override
        public int fixedSize() {
            return 8;
        }

        @Override
        public boolean isTrusted() {
            return true;
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
            byte[] a = SerializerBase.booleanToByteArray(value);
            out.write(a);
        }

        @Override
        public boolean[] deserialize(DataInput in, int available) throws IOException {
            return SerializerBase.readBooleanArray(DataIO.unpackInt(in), in);
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
        public int hashCode(boolean[] booleans) {
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
        public int hashCode(short[] shorts) {
            return Arrays.hashCode(shorts);
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
        public int hashCode(float[] floats) {
            return Arrays.hashCode(floats);
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


    public static final Serializer<Class> CLASS = new Serializer<Class>() {

        @Override
        public void serialize(DataOutput out, Class value) throws IOException {
            out.writeUTF(value.getName());
        }

        @Override
        public Class deserialize(DataInput in, int available) throws IOException {
            return SerializerPojo.classForName(in.readUTF());
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public boolean equals(Class a1, Class a2) {
            return a1==a2 || (a1.toString().equals(a2.toString()));
        }

        @Override
        public int hashCode(Class aClass) {
            //class does not override identity hash code
            return aClass.toString().hashCode();
        }
    };

    public static final Serializer<Date> DATE = new Serializer<Date>() {

        @Override
        public void serialize(DataOutput out, Date value) throws IOException {
            out.writeLong(value.getTime());
        }

        @Override
        public Date deserialize(DataInput in, int available) throws IOException {
            return new Date(in.readLong());
        }

        @Override
        public int fixedSize() {
            return 8;
        }

        @Override
        public boolean isTrusted() {
            return true;
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

        public CompressionWrapper(Serializer<E> serializer) {
            this.serializer = serializer;
        }

        /** used for deserialization */
        protected CompressionWrapper(SerializerBase serializerBase, DataInput is, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            this.serializer = (Serializer<E>) serializerBase.deserialize(is,objectStack);
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
            if(newLen>=out2.pos){
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
            if(CC.PARANOID && ! (in2.pos==unpackedSize))
                throw new AssertionError( "data were not fully read");
            return ret;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CompressionWrapper that = (CompressionWrapper) o;
            return serializer.equals(that.serializer);
        }

        @Override
        public int hashCode() {
            return serializer.hashCode();
        }

        @Override
        public boolean isTrusted() {
            return true;
        }
    }

    public static final class Array extends Serializer<Object[]> implements  Serializable{

        protected final Serializer serializer;

        public Array(Serializer serializer) {
            this.serializer = serializer;
        }

        /** used for deserialization */
        protected Array(SerializerBase serializerBase, DataInput is, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            this.serializer = (Serializer) serializerBase.deserialize(is,objectStack);
        }


        @Override
        public void serialize(DataOutput out, Object[] value) throws IOException {
            DataIO.packInt(out,value.length);
            for(Object a:value){
                serializer.serialize(out,a);
            }
        }

        @Override
        public Object[] deserialize(DataInput in, int available) throws IOException {
            Object[] ret = new Object[DataIO.unpackInt(in)];
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
        public boolean equals(Object[] a1, Object[] a2) {
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
        public int hashCode(Object[] objects) {
            int ret = objects.length;
            for(Object a:objects){
                ret=31*ret+serializer.hashCode(a);
            }
            return ret;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return serializer.equals(((Array) o).serializer);

        }

        @Override
        public int hashCode() {
            return serializer.hashCode();
        }
    }

    //this has to be lazily initialized due to circular dependencies
    static final  class __BasicInstance {
        final static Serializer s = new SerializerBase();
    }


    /**
     * Basic serializer for most classes in {@code java.lang} and {@code java.util} packages.
     * It does not handle custom POJO classes. It also does not handle classes which
     * require access to {@code DB} itself.
     */
    @SuppressWarnings("unchecked")
    public static final Serializer<Object> BASIC = new Serializer(){

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
     */
    abstract public void serialize( DataOutput out, A value)
            throws IOException;


    /**
     * Deserialize the content of an object from a DataInput.
     *
     * @param in to read serialized data from
     * @param available how many bytes are available in DataInput for reading, may be -1 (in streams) or 0 (null).
     * @return deserialized object
     * @throws java.io.IOException
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

    public boolean isTrusted(){
        return false;
    }

    public boolean equals(A a1, A a2){
        return a1==a2 || (a1!=null && a1.equals(a2));
    }

    public int hashCode(A a){
        return a.hashCode();
    }

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

    public BTreeKeySerializer getBTreeKeySerializer(boolean descending){
        return new BTreeKeySerializer.BasicKeySerializer(Serializer.this,
                descending? Fun.REVERSE_COMPARATOR : Fun.COMPARATOR);
    }



}
