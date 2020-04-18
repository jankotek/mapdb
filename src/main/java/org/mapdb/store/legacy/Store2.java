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
package org.mapdb.store.legacy;

import org.mapdb.CC;
import org.mapdb.io.DataOutput2ByteArray;
import org.mapdb.ser.Serializer;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 * Low level record store.
 */
public abstract class Store2 implements Store {

    protected static final Logger LOG = Logger.getLogger(Store.class.getName());

    public static final int VOLUME_CHUNK_SHIFT = 20; // 1 MB

    protected final static int CHECKSUM_FLAG_MASK = 1;
    protected final static int COMPRESS_FLAG_MASK = 1<<2;
    protected final static int ENCRYPT_FLAG_MASK = 1<<3;


    protected static final int CHUNK_SIZE = 1<< VOLUME_CHUNK_SHIFT;


    protected static final int CHUNK_SIZE_MOD_MASK = CHUNK_SIZE -1;

    public abstract long getMaxRecid();
    public abstract ByteBuffer getRaw(long recid);
    public abstract Iterator<Long> getFreeRecids();
    public abstract void updateRaw(long recid, ByteBuffer data);

    /** returns maximal store size or `0` if there is no limit */
    public abstract long getSizeLimit();

    /** returns current size occupied by physical store (does not include index). It means file allocated by physical file */
    public abstract long getCurrSize();

    /** returns free size in  physical store (does not include index). */
    public abstract long getFreeSize();

    /** get some statistics about store. This may require traversing entire store, so it can take some time.*/
    public abstract String calculateStatistics();

    public void printStatistics(){
        System.out.println(calculateStatistics());
    }

    protected Lock serializerPojoInitLock = new ReentrantLock();


    protected final ReentrantLock structuralLock = new ReentrantLock();
    protected final ReentrantReadWriteLock newRecidLock = new ReentrantReadWriteLock();
    protected final ReentrantReadWriteLock locks = new ReentrantReadWriteLock();

    protected void lockAllWrite() {
        newRecidLock.writeLock().lock();
        locks.writeLock().lock();
        structuralLock.lock();
    }

    protected void unlockAllWrite() {
        structuralLock.unlock();
        locks.writeLock().unlock();
        newRecidLock.writeLock().unlock();
    }

    protected <A> DataOutput2ByteArray serialize(A value, Serializer<A> serializer){
        if(value==null)
            return null;

        DataOutput2ByteArray out = newDataOut2();

        serializer.serialize(out,value);

        if(out.pos>0){

            if(CC.PARANOID)try{
                //check that array is the same after deserialization
                DataInput2Exposed inp = new DataInput2Exposed(ByteBuffer.wrap(Arrays.copyOf(out.buf,out.pos)));
                byte[] decompress = deserialize(Serializers.BYTE_ARRAY_NOSIZE,out.pos,inp);

                DataOutput2ByteArray expected = newDataOut2();
                serializer.serialize(expected,value);

                byte[] expected2 = Arrays.copyOf(expected.buf, expected.pos);
                //check arrays equals
                assert(Arrays.equals(expected2,decompress));


            }catch(Exception e){
                throw new RuntimeException(e);
            }
        }
        return out;

    }

    protected DataOutput2ByteArray newDataOut2() {
        return  new DataOutput2ByteArray();
    }


    protected <A> A deserialize(Serializer<A> serializer, int size, DataInput2Exposed di) throws IOException {

        int start = di.pos;

        A ret = serializer.deserialize(di);
        if(size+start>di.pos)
            throw new AssertionError("data were not fully read, check your serializer ");
        if(size+start<di.pos)
            throw new AssertionError("data were read beyond record size, check your serializer");
        return ret;
    }

    protected int expectedMasks(){
        return 0;
    }

    List<Runnable> closeListeners = new CopyOnWriteArrayList<Runnable>();

    public void closeListenerRegister(Runnable closeListener) {
        closeListeners.add(closeListener);
    }

    public void closeListenerUnregister(Runnable closeListener) {
        closeListeners.remove(closeListener);
    }

}
