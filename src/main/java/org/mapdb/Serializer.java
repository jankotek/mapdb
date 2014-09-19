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
import java.util.Date;
import java.util.UUID;

/**
 * Provides serialization and deserialization
 *
 * @author Jan Kotek
 */
public interface Serializer<A> {


    Serializer<Character> CHAR =  new Serializer<Character>() {
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
    };


    /**
     * Serializes strings using UTF8 encoding.
     * Stores string size so can be used as collection serializer.
     * Does not handle null values
     */
    Serializer<String> STRING = new Serializer<String>() {
        @Override
        public void serialize(DataOutput out, String value) throws IOException {
            out.writeUTF(value);
        }

        @Override
        public String deserialize(DataInput in, int available) throws IOException {
            return in.readUTF();
        }

        @Override
        public int fixedSize() {
            return -1;
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
    Serializer<String> STRING_INTERN = new Serializer<String>() {
        @Override
        public void serialize(DataOutput out, String value) throws IOException {
            out.writeUTF(value);
        }

        @Override
        public String deserialize(DataInput in, int available) throws IOException {
            return in.readUTF().intern();
        }

        @Override
        public int fixedSize() {
            return -1;
        }

    };

    /**
     * Serializes strings using ASCII encoding (8 bit character).
     * Is faster compared to UTF8 encoding.
     * Stores string size so can be used as collection serializer.
     * Does not handle null values
     */
    Serializer<String> STRING_ASCII = new Serializer<String>() {
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
        public int fixedSize() {
            return -1;
        }

    };

    /**
     * Serializes strings using UTF8 encoding.
     * Used mainly for testing.
     * Does not handle null values.
     */
    Serializer<String> STRING_NOSIZE = new Serializer<String>() {

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
        public int fixedSize() {
            return -1;
        }

    };





    /** Serializes Long into 8 bytes, used mainly for testing.
     * Does not handle null values.*/
     
     Serializer<Long> LONG = new Serializer<Long>() {
        @Override
        public void serialize(DataOutput out, Long value) throws IOException {
            if(value != null)
                out.writeLong(value);
        }

        @Override
        public Long deserialize(DataInput in, int available) throws IOException {
            if(available==0) return null;
            return in.readLong();
        }

        @Override
        public int fixedSize() {
            return 8;
        }

    };

    /** Serializes Integer into 4 bytes.
     * Does not handle null values.*/
    
    Serializer<Integer> INTEGER = new Serializer<Integer>() {
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

    };

    
    Serializer<Boolean> BOOLEAN = new Serializer<Boolean>() {
        @Override
        public void serialize(DataOutput out, Boolean value) throws IOException {
            out.writeBoolean(value);
        }

        @Override
        public Boolean deserialize(DataInput in, int available) throws IOException {
            if(available==0) return null;
            return in.readBoolean();
        }

        @Override
        public int fixedSize() {
            return 1;
        }

    };

    


    /**
     * Always throws {@link IllegalAccessError} when invoked. Useful for testing and assertions.
     */
    Serializer<Object> ILLEGAL_ACCESS = new Serializer<Object>() {
        @Override
        public void serialize(DataOutput out, Object value) throws IOException {
            throw new IllegalAccessError();
        }

        @Override
        public Object deserialize(DataInput in, int available) throws IOException {
            throw new IllegalAccessError();
        }

        @Override
        public int fixedSize() {
            return -1;
        }

    };


    /**
     * Serializes `byte[]` it adds header which contains size information
     */
    Serializer<byte[] > BYTE_ARRAY = new Serializer<byte[]>() {

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
        public int fixedSize() {
            return -1;
        }

    } ;

    /**
     * Serializes `byte[]` directly into underlying store
     * It does not store size, so it can not be used in Maps and other collections.
     */
    Serializer<byte[] > BYTE_ARRAY_NOSIZE = new Serializer<byte[]>() {

        @Override
        public void serialize(DataOutput out, byte[] value) throws IOException {
            if(value==null||value.length==0) return;
            out.write(value);
        }

        @Override
        public byte[] deserialize(DataInput in, int available) throws IOException {
            if(available==-1) throw new IllegalArgumentException("BYTE_ARRAY_NOSIZE does not work with collections.");
            if(available==0) return null;
            byte[] ret = new byte[available];
            in.readFully(ret);
            return ret;
        }

        @Override
        public int fixedSize() {
            return -1;
        }

    } ;

    /**
     * Serializes `char[]` it adds header which contains size information
     */
    Serializer<char[] > CHAR_ARRAY = new Serializer<char[]>() {

        @Override
        public void serialize(DataOutput out, char[] value) throws IOException {
            DataIO.packInt(out,value.length);
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
        public int fixedSize() {
            return -1;
        }

    };


    /**
     * Serializes `int[]` it adds header which contains size information
     */
    Serializer<int[] > INT_ARRAY = new Serializer<int[]>() {

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
        public int fixedSize() {
            return -1;
        }

    };

    /**
     * Serializes `long[]` it adds header which contains size information
     */
    Serializer<long[] > LONG_ARRAY = new Serializer<long[]>() {

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
        public int fixedSize() {
            return -1;
        }

    };

    /**
     * Serializes `double[]` it adds header which contains size information
     */
    Serializer<double[] > DOUBLE_ARRAY = new Serializer<double[]>() {

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
        public int fixedSize() {
            return -1;
        }

    };


    /** Serializer which uses standard Java Serialization with {@link java.io.ObjectInputStream} and {@link java.io.ObjectOutputStream} */
    Serializer<Object> JAVA = new Serializer<Object>() {
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

        @Override
        public int fixedSize() {
            return -1;
        }

    };

    /** Serializers {@link java.util.UUID} class */
    Serializer<java.util.UUID> UUID = new Serializer<java.util.UUID>() {
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

    };

    Serializer<Byte> BYTE = new Serializer<Byte>() {
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
    } ;
    Serializer<Float> FLOAT = new Serializer<Float>() {
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
    } ;


    Serializer<Double> DOUBLE = new Serializer<Double>() {
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
    } ;

    Serializer<Short> SHORT = new Serializer<Short>() {
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
    } ;

    Serializer<boolean[]> BOOLEAN_ARRAY = new Serializer<boolean[]>() {
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
        public int fixedSize() {
            return -1;
        }
    };



    Serializer<short[]> SHORT_ARRAY = new Serializer<short[]>() {
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
        public int fixedSize() {
            return -1;
        }
    };


    Serializer<float[]> FLOAT_ARRAY = new Serializer<float[]>() {
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
        public int fixedSize() {
            return -1;
        }
    };

    Serializer<BigInteger> BIG_INTEGER = new Serializer<BigInteger>() {
        @Override
        public void serialize(DataOutput out, BigInteger value) throws IOException {
            BYTE_ARRAY.serialize(out,value.toByteArray());
        }

        @Override
        public BigInteger deserialize(DataInput in, int available) throws IOException {
            return new BigInteger(BYTE_ARRAY.deserialize(in,available));
        }

        @Override
        public int fixedSize() {
            return -1;
        }
    } ;

    Serializer<BigDecimal> BIG_DECIMAL = new Serializer<BigDecimal>() {
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
        public int fixedSize() {
            return -1;
        }
    } ;


    Serializer<Class> CLASS = new Serializer<Class>() {

        @Override
        public void serialize(DataOutput out, Class value) throws IOException {
            out.writeUTF(value.getName());
        }

        @Override
        public Class deserialize(DataInput in, int available) throws IOException {
            return SerializerPojo.classForName(in.readUTF());
        }

        @Override
        public int fixedSize() {
            return -1;
        }
    };

    Serializer<Date> DATE = new Serializer<Date>() {

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
    };


    /** wraps another serializer and (de)compresses its output/input*/
    public final static class CompressionWrapper<E> implements Serializer<E>, Serializable {

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
            assert(in2.pos==unpackedSize): "data were not fully read";
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
        public int fixedSize() {
            return -1;
        }

    }

    //this has to be lazily initialized due to circular dependencies
    static final  class __BasicInstance {
        final static Serializer s = new SerializerBase();
    }


    /**
     * Basic serializer for most classes in 'java.lang' and 'java.util' packages.
     * It does not handle custom POJO classes. It also does not handle classes which
     * require access to `DB` itself.
     */
    @SuppressWarnings("unchecked")
    Serializer<Object> BASIC = new Serializer(){

        @Override
        public void serialize(DataOutput out, Object value) throws IOException {
            __BasicInstance.s.serialize(out,value);
        }

        @Override
        public Object deserialize(DataInput in, int available) throws IOException {
            return __BasicInstance.s.deserialize(in,available);
        }

        @Override
        public int fixedSize() {
            return -1;
        }
    };


    /**
     * Serialize the content of an object into a ObjectOutput
     *
     * @param out ObjectOutput to save object into
     * @param value Object to serialize
     */
    public void serialize( DataOutput out, A value)
            throws IOException;


    /**
     * Deserialize the content of an object from a DataInput.
     *
     * @param in to read serialized data from
     * @param available how many bytes are available in DataInput for reading, may be -1 (in streams) or 0 (null).
     * @return deserialized object
     * @throws java.io.IOException
     */
    public A deserialize( DataInput in, int available)
            throws IOException;

    /**
     * Data could be serialized into record with variable size or fixed size.
     * Some optimizations can be applied to serializers with fixed size
     *
     * @return fixed size or -1 for variable size
     */
    public int fixedSize();

}
