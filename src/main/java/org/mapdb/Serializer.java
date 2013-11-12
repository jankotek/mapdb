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
import java.util.*;

/**
 * Provides serialization and deserialization
 *
 * @author Jan Kotek
 */
public interface Serializer<A> {



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
            value.getChars(0,cc.length,cc,0);
            Utils.packInt(out,cc.length);
            for(char c:cc){
                out.write(c);
            }
        }

        @Override
        public String deserialize(DataInput in, int available) throws IOException {
            int size = Utils.unpackInt(in);
            char[] cc = new char[size];
            for(int i=0;i<size;i++){
                cc[i] = (char) in.readUnsignedByte();
            }
            return new String(cc);
        }
    };

    /**
     * Serializes strings using UTF8 encoding.
     * Used mainly for testing.
     * Does not handle null values.
     */
    Serializer<String> STRING_NOSIZE = new Serializer<String>() {

        @Override
		public void serialize(DataOutput out, String value) throws IOException {
            final byte[] bytes = value.getBytes(Utils.UTF8_CHARSET);
            out.write(bytes);
        }


        @Override
		public String deserialize(DataInput in, int available) throws IOException {
            if(available==-1) throw new IllegalArgumentException("STRING_NOSIZE does not work with collections.");
            byte[] bytes = new byte[available];
            in.readFully(bytes);
            return new String(bytes, Utils.UTF8_CHARSET);
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
    };

    /** Serializes Integer into 4 bytes, used mainly for testing.
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
    };

    /**
     * Basic serializer for most classes in 'java.lang' and 'java.util' packages.
     * It does not handle custom POJO classes. It also does not handle classes which
     * require access to `DB` itself.
     */
    @SuppressWarnings("unchecked")
    Serializer<Object> BASIC = new SerializerBase();


    /**
     * Serializes `byte[]` it adds header which contains size information
     */
    Serializer<byte[] > BYTE_ARRAY = new Serializer<byte[]>() {

        @Override
        public void serialize(DataOutput out, byte[] value) throws IOException {
            Utils.packInt(out,value.length);
            out.write(value);
        }

        @Override
        public byte[] deserialize(DataInput in, int available) throws IOException {
            int size = Utils.unpackInt(in);
            byte[] ret = new byte[size];
            in.readFully(ret);
            return ret;
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
    } ;

    /**
     * Serializes `char[]` it adds header which contains size information
     */
    Serializer<char[] > CHAR_ARRAY = new Serializer<char[]>() {

        @Override
        public void serialize(DataOutput out, char[] value) throws IOException {
            Utils.packInt(out,value.length);
            for(char c:value){
                out.writeChar(c);
            }
        }

        @Override
        public char[] deserialize(DataInput in, int available) throws IOException {
            final int size = Utils.unpackInt(in);
            char[] ret = new char[size];
            for(int i=0;i<size;i++){
                ret[i] = in.readChar();
            }
            return ret;
        }
    };


    /**
     * Serializes `int[]` it adds header which contains size information
     */
    Serializer<int[] > INT_ARRAY = new Serializer<int[]>() {

        @Override
        public void serialize(DataOutput out, int[] value) throws IOException {
            Utils.packInt(out,value.length);
            for(int c:value){
                out.writeInt(c);
            }
        }

        @Override
        public int[] deserialize(DataInput in, int available) throws IOException {
            final int size = Utils.unpackInt(in);
            int[] ret = new int[size];
            for(int i=0;i<size;i++){
                ret[i] = in.readInt();
            }
            return ret;
        }
    };

    /**
     * Serializes `long[]` it adds header which contains size information
     */
    Serializer<long[] > LONG_ARRAY = new Serializer<long[]>() {

        @Override
        public void serialize(DataOutput out, long[] value) throws IOException {
            Utils.packInt(out,value.length);
            for(long c:value){
                out.writeLong(c);
            }
        }

        @Override
        public long[] deserialize(DataInput in, int available) throws IOException {
            final int size = Utils.unpackInt(in);
            long[] ret = new long[size];
            for(int i=0;i<size;i++){
                ret[i] = in.readLong();
            }
            return ret;
        }
    };

    /**
     * Serializes `double[]` it adds header which contains size information
     */
    Serializer<double[] > DOUBLE_ARRAY = new Serializer<double[]>() {

        @Override
        public void serialize(DataOutput out, double[] value) throws IOException {
            Utils.packInt(out,value.length);
            for(double c:value){
                out.writeDouble(c);
            }
        }

        @Override
        public double[] deserialize(DataInput in, int available) throws IOException {
            final int size = Utils.unpackInt(in);
            double[] ret = new double[size];
            for(int i=0;i<size;i++){
                ret[i] = in.readDouble();
            }
            return ret;
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
    };

    /** wraps another serializer and (de)compresses its output/input*/
    public final static class CompressionWrapper<E> implements Serializer<E>, Serializable {

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
            DataOutput2 out2 = new DataOutput2();
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
                Utils.packInt(out,0);
                out.write(out2.buf,0,out2.pos);
                return;
            }

            Utils.packInt(out, out2.pos+1); //unpacked size, zero indicates no compression
            out.write(tmp,0,newLen);
        }

        @Override
        public E deserialize(DataInput in, int available) throws IOException {
            final int unpackedSize = Utils.unpackInt(in)-1;
            if(unpackedSize==-1){
                //was not compressed
                return serializer.deserialize(in, available>0?available-1:available);
            }

            byte[] unpacked = new byte[unpackedSize];
            LZF.get().expand(in,unpacked,0,unpackedSize);
            DataInput2 in2 = new DataInput2(unpacked);
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
    }
}
