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
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 * Various IO related utilities
 *
 * @author Jan Kotek
 * @author Nathan Sweet wrote long packer utils
 */
@SuppressWarnings("unchecked")
final public class Utils {

    static final Logger LOG = Logger.getLogger("mapdb");


    @SuppressWarnings("rawtypes")
	public static final Comparator COMPARABLE_COMPARATOR = new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
            return o1.compareTo(o2);
        }
    };


    public static final String EMPTY_STRING = "";
    public static final String UTF8 = "UTF8";
    public static final Charset UTF8_CHARSET = Charset.forName(UTF8);


    private static final int LOCK_MASK = CC.CONCURRENCY-1;



    /** empty iterator (note: Collections.EMPTY_ITERATOR is Java 7 specific and should not be used)*/
    public static final Iterator EMPTY_ITERATOR = new ArrayList(0).iterator();


    /**
     * Pack  non-negative long into output stream.
     * It will occupy 1-10 bytes depending on value (lower values occupy smaller space)
     *
     * @param out DataOutput to put value into
     * @param value to be serialized, must be non-negative
     * @throws java.io.IOException
     */
    static public void packLong(DataOutput out, long value) throws IOException {

        assert(value>=0):"negative value: "+value;

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

    /** get number of bytes occupied by packed long */
    public static int packedLongSize(long value) {
        int ret = 1;
        while ((value & ~0x7FL) != 0) {
            ret++;
            value >>>= 7;
        }
        return ret;
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
        assert(value>=0):"negative value: "+value;

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

    public static int intHash(int h) {
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
    }

    public static int lockPos(final long key) {
        int h = (int)(key ^ (key >>> 32));
        h ^= (h >>> 20) ^ (h >>> 12);
        h ^= (h >>> 7) ^ (h >>> 4);
        return h & LOCK_MASK;
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
            new File(index.getPath()+ StoreDirect.DATA_FILE_EXT).deleteOnExit();
            new File(index.getPath()+ StoreWAL.TRANS_LOG_FILE_EXT).deleteOnExit();

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




    public static String randomString(int size) {
        String chars = "0123456789abcdefghijklmnopqrstuvwxyz !@#$%^&*()_+=-{}[]:\",./<>?|\\";
        StringBuilder b = new StringBuilder(size);
        Random r = new Random();
        for(int i=0;i<size;i++){
            b.append(chars.charAt(r.nextInt(chars.length())));
        }
        return b.toString();
    }

    public static ReentrantReadWriteLock[] newReadWriteLocks() {
        ReentrantReadWriteLock[] locks = new ReentrantReadWriteLock[CC.CONCURRENCY];
        for(int i=0;i<locks.length;i++) locks[i] = new ReentrantReadWriteLock(CC.FAIR_LOCKS);
        return locks;
    }

    public static ReentrantLock[] newLocks() {
        ReentrantLock[] locks = new ReentrantLock[CC.CONCURRENCY];
        for(int i=0;i<locks.length;i++) locks[i] = new ReentrantLock(CC.FAIR_LOCKS);
        return locks;
    }



    public static void lock(LongConcurrentHashMap<Thread> locks, long recid){
        //feel free to rewrite, if you know better (more efficient) way
        if(locks.get(recid)==Thread.currentThread()){
            //check node is not already locked by this thread
            throw new InternalError("node already locked by current thread: "+recid);
        }

        while(locks.putIfAbsent(recid, Thread.currentThread()) != null){
            LockSupport.parkNanos(10);
        }
    }



    public static void unlock(LongConcurrentHashMap<Thread> locks,final long recid) {
        final Thread t = locks.remove(recid);
        if(t!=Thread.currentThread())
            throw new InternalError("unlocked wrong thread");
    }

    public static void unlockAll(LongConcurrentHashMap<Thread> locks) {
        final Thread t = Thread.currentThread();
        LongMap.LongMapIterator<Thread> iter = locks.longMapIterator();
        while(iter.moveToNext())
            if(iter.value()==t)
                iter.remove();
    }


    public static void assertNoLocks(LongConcurrentHashMap<Thread> locks){
            LongMap.LongMapIterator<Thread> i = locks.longMapIterator();
            while(i.moveToNext()){
                if(i.value()==Thread.currentThread()){
                    throw new InternalError("Node "+i.key()+" is still locked");
                }
            }
    }


    /** clone value using serialization */
    public static <E> E clone(E value, Serializer<E> serializer) {
        try{
            DataOutput2 out = new DataOutput2();
            serializer.serialize(out, value);
            DataInput2 in = new DataInput2(ByteBuffer.wrap(out.copyBytes()), 0);

            return serializer.deserialize(in,out.pos);
        }catch(IOException ee){
            throw new IOError(ee);
        }
    }

    public static <E> Iterator<E> arrayIterator(final Object[] array, final int fromIndex, final int toIndex) {
        return new Iterator<E>(){

            int index = fromIndex;

            @Override
            public boolean hasNext() {
                return index<toIndex;
            }

            @Override
            public E next() {
                if(index>=toIndex) throw new NoSuchElementException();
                return (E) array[index++];
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private  static final char[] HEXA_CHARS = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};

    public static String toHexa( byte [] bb ) {
        char[] ret = new char[bb.length*2];
        for(int i=0;i<bb.length;i++){
            ret[i*2] =HEXA_CHARS[((bb[i]& 0xF0) >> 4)];
            ret[i*2+1] = HEXA_CHARS[((bb[i] & 0x0F))];
        }
        return new String(ret);
    }

    public static byte[] fromHexa(String s ) {
        byte[] ret = new byte[s.length()/2];
        for(int i=0;i<ret.length;i++){
            ret[i] = (byte) Integer.parseInt(s.substring(i*2,i*2+2),16);
        }
        return ret;
    }
}
