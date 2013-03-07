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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Various IO related utilities
 *
 * @author Jan Kotek
 * @author Nathan Sweet wrote long packer utils
 */
@SuppressWarnings("unchecked")
final public class Utils {

    static final Logger LOG = Logger.getLogger("JDBM");


    @SuppressWarnings("rawtypes")
	public static final Comparator<Comparable> COMPARABLE_COMPARATOR = new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
            return o1.compareTo(o2);
        }
    };

    @SuppressWarnings("rawtypes")
	public static final Comparator<Comparable> COMPARABLE_COMPARATOR_WITH_NULLS = new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
            return o1 == null && o2 != null ? -1 : (o1 != null && o2 == null ? 1 : o1.compareTo(o2));
        }
    };


    public static final String EMPTY_STRING = "";
    public static final String UTF8 = "UTF8";
    public static Random RANDOM = new Random();


    /**
     * Pack  non-negative long into output stream.
     * It will occupy 1-10 bytes depending on value (lower values occupy smaller space)
     *
     * @param out DataOutput to put value into
     * @param value to be serialized, must be non-negative
     * @throws java.io.IOException
     */
    static public void packLong(DataOutput out, long value) throws IOException {

        if (value < 0) {
            throw new IllegalArgumentException("negative value: keys=" + value);
        }

        while ((value & ~0x7FL) != 0) {
            out.write((((int) value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.write((byte) value);
    }


    /**
     * Unpack positive long value from the input stream.
     *
     * @param in The input stream.
     * @return The long value.
     * @throws java.io.IOException
     */
    static public long unpackLong(DataInput in) throws IOException {

        long result = 0;
        for (int offset = 0; offset < 64; offset += 7) {
            long b = in.readUnsignedByte();
            result |= (b & 0x7F) << offset;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new Error("Malformed long.");
    }


    /**
     * Pack  non-negative long into output stream.
     * It will occupy 1-5 bytes depending on value (lower values occupy smaller space)
     *
     * @param in DataOutput to put value into
     * @param value to be serialized, must be non-negative
     * @throws IOException
     */

    static public void packInt(DataOutput in, int value) throws IOException {
        if (value < 0) {
            throw new IllegalArgumentException("negative value: keys=" + value);
        }

        while ((value & ~0x7F) != 0) {
            in.write(((value & 0x7F) | 0x80));
            value >>>= 7;
        }

        in.write((byte) value);
    }

    static public int unpackInt(DataInput is) throws IOException {
        for (int offset = 0, result = 0; offset < 32; offset += 7) {
            int b = is.readUnsignedByte();
            result |= (b & 0x7F) << offset;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new Error("Malformed int.");
    }


    public static int longHash(final long key) {
        int h = (int)(key ^ (key >>> 32));
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
    }

    /** clone value using serialization */
    public static <E> E clone(E value, Serializer<E> serializer){
        try{
            DataOutput2 out = new DataOutput2();
            serializer.serialize(out,value);
            DataInput2 in = new DataInput2(ByteBuffer.wrap(out.copyBytes()), 0);

            return serializer.deserialize(in,out.pos);
        }catch(IOException ee){
            throw new IOError(ee);
        }
    }

    /** expand array size by 1, and put value at given position. No items from original array are lost*/
    public static Object[] arrayPut(final Object[] array, final int pos, final Object value){
        final Object[] ret = Arrays.copyOf(array, array.length+1);
        if(pos<array.length){
            System.arraycopy(array, pos, ret, pos+1, array.length-pos);
        }
        ret[pos] = value;
        return ret;
    }

    public static long[] arrayLongPut(final long[] array, final int pos, final long value) {
        final long[] ret = Arrays.copyOf(array,array.length+1);
        if(pos<array.length){
            System.arraycopy(array,pos,ret,pos+1,array.length-pos);
        }
        ret[pos] = value;
        return ret;
    }


    /** Compute nearest bigger power of two*/
    public static int nextPowTwo(final int value){
        int ret = 2;
        while(ret<value)
            ret = ret<<1;
        return ret;
    }

    /**
     * Create temporary file in temp folder. All associated db files will be deleted on JVM exit.
     */
    public static File tempDbFile() {
        try{
            File index = File.createTempFile("mapdb","db");
            index.deleteOnExit();
            new File(index.getPath()+StorageDirect.DATA_FILE_EXT).deleteOnExit();
            new File(index.getPath()+ StorageJournaled.TRANS_LOG_FILE_EXT).deleteOnExit();

            return index;
        }catch(IOException e){
            throw new IOError(e);
        }
    }

    /** check if Operating System is Windows */
    public static boolean isWindows(){
        String os = System.getProperty("os.name");
        return os!=null && (os.toLowerCase().indexOf("win") >= 0);

    }

    /** check if Operating System is Android */
    public static boolean isAndroid(){
        return "Dalvik".equalsIgnoreCase(System.getProperty("java.vm.name"));
    }


    /**
     * Check if large files can be mapped into memory.
     * For example 32bit JVM can only address 2GB and large files can not be mapped,
     * so for 32bit JVM this function returns false.
     *
     */
    public static boolean JVMSupportsLargeMappedFiles() {
        String prop = System.getProperty("os.arch");
        if(prop!=null && prop.contains("64")) return true;
        //TODO better check for 32bit JVM
        return false;
    }

    private static boolean collectionAsMapValueLogged = false;

    public static void checkMapValueIsNotCollecion(Object value){
        if(!CC.LOG_HINTS || collectionAsMapValueLogged) return;
        if(value instanceof Collection || value instanceof Map){
            collectionAsMapValueLogged = true;
            LOG.warning("You should not use collections as Map values. MapDB requires key/values to be immutable! Checkout MultiMap example for 1:N mapping.");
        }
    }

    public static void printer(final AtomicLong value){
        new Thread("printer"){
            {
                setDaemon(true);
            }


            @Override
            public void run() {
                long startValue = value.get();
                long startTime = System.currentTimeMillis();
                long old = value.get();
                while(true){

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        return;
                    }

                    long current = value.get();
                    long totalSpeed = 1000*(current-startValue)/(System.currentTimeMillis()-startTime);
                    System.out.print("total: "+current+" - items per last second: "+(current-old)+" - avg items per second: "+totalSpeed+"\r");
                    old = current;
                }

            }
        }.start();
    }

}
