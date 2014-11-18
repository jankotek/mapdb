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

import java.io.DataInput;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;
import java.util.zip.CRC32;

/**
 * Low level record store.
 */
public abstract class Store implements Engine{

    protected static final Logger LOG = Logger.getLogger(Store.class.getName());

    protected final String fileName;
    protected final boolean checksum;
    protected final boolean compress;
    protected final boolean encrypt;
    protected final byte[] password;
    protected final EncryptionXTEA encryptionXTEA;

    protected final static int CHECKSUM_FLAG_MASK = 1;
    protected final static int COMPRESS_FLAG_MASK = 1<<2;
    protected final static int ENCRYPT_FLAG_MASK = 1<<3;


    protected static final int SLICE_SIZE = 1<< CC.VOLUME_SLICE_SHIFT;

    protected static final int SLICE_SIZE_MOD_MASK = SLICE_SIZE -1;
    protected final Fun.Function1<Volume, String> volumeFactory;

    /** default serializer used for persistence. Handles POJO and other stuff which requires write-able access to Engine */
    protected SerializerPojo serializerPojo;



    protected final ThreadLocal<CompressLZF> LZF;

    protected Store(String fileName, Fun.Function1<Volume, String> volumeFactory, boolean checksum, boolean compress, byte[] password) {
        this.fileName = fileName;
        this.volumeFactory = volumeFactory;
        structuralLock = new ReentrantLock(CC.FAIR_LOCKS);
        newRecidLock = new ReentrantReadWriteLock(CC.FAIR_LOCKS);
        locks = new ReentrantReadWriteLock[CC.CONCURRENCY];
        for(int i=0;i< locks.length;i++){
            locks[i] = new ReentrantReadWriteLock(CC.FAIR_LOCKS);
        }

        this.checksum = checksum;
        this.compress = compress;
        this.encrypt =  password!=null;
        this.password = password;
        this.encryptionXTEA = !encrypt?null:new EncryptionXTEA(password);

        this.LZF = !compress?null:new ThreadLocal<CompressLZF>() {
            @Override
            protected CompressLZF initialValue() {
                return new CompressLZF();
            }
        };
    }

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

    protected Lock serializerPojoInitLock = new ReentrantLock(CC.FAIR_LOCKS);

    /**
     * @return default serializer used in this DB, it handles POJO and other stuff.
     */
    public  SerializerPojo getSerializerPojo() {
        final Lock pojoLock = serializerPojoInitLock;
        if(pojoLock!=null) {
            pojoLock.lock();
            try{
                if(serializerPojo==null){
                    final CopyOnWriteArrayList<SerializerPojo.ClassInfo> classInfos = get(Engine.RECID_CLASS_CATALOG, SerializerPojo.serializer);
                    serializerPojo = new SerializerPojo(classInfos);
                    serializerPojoInitLock = null;
                }
            }finally{
                pojoLock.unlock();
            }

        }
        return serializerPojo;
    }


    protected final ReentrantLock structuralLock;
    protected final ReentrantReadWriteLock newRecidLock;
    protected final ReentrantReadWriteLock[] locks;


    protected void lockAllWrite() {
        newRecidLock.writeLock().lock();
        for(ReentrantReadWriteLock l: locks) {
            l.writeLock().lock();
        }
        structuralLock.lock();
    }

    protected void unlockAllWrite() {
        structuralLock.unlock();
        for(ReentrantReadWriteLock l: locks) {
            l.writeLock().unlock();
        }
        newRecidLock.writeLock().unlock();
    }



    protected final Queue<DataIO.DataOutputByteArray> recycledDataOuts = new ArrayBlockingQueue<DataIO.DataOutputByteArray>(128);


    protected <A> DataIO.DataOutputByteArray serialize(A value, Serializer<A> serializer){
        try {
            DataIO.DataOutputByteArray out = newDataOut2();

            serializer.serialize(out,value);

            if(out.pos>0){

                if(compress){
                    DataIO.DataOutputByteArray tmp = newDataOut2();
                    tmp.ensureAvail(out.pos+40);
                    final CompressLZF lzf = LZF.get();
                    int newLen;
                    try{
                        newLen = lzf.compress(out.buf,out.pos,tmp.buf,0);
                    }catch(IndexOutOfBoundsException e){
                        newLen=0; //larger after compression
                    }
                    if(newLen>=out.pos) newLen= 0; //larger after compression

                    if(newLen==0){
                        recycledDataOuts.offer(tmp);
                        //compression had no effect, so just write zero at beginning and move array by 1
                        out.ensureAvail(out.pos+1);
                        System.arraycopy(out.buf,0,out.buf,1,out.pos);
                        out.pos+=1;
                        out.buf[0] = 0;
                    }else{
                        //compression had effect, so write decompressed size and compressed array
                        final int decompSize = out.pos;
                        out.pos=0;
                        DataIO.packInt(out,decompSize);
                        out.write(tmp.buf,0,newLen);
                        recycledDataOuts.offer(tmp);
                    }

                }


                if(encrypt){
                    int size = out.pos;
                    //round size to 16
                    if(size%EncryptionXTEA.ALIGN!=0)
                        size += EncryptionXTEA.ALIGN - size%EncryptionXTEA.ALIGN;
                    final int sizeDif=size-out.pos;
                    //encrypt
                    out.ensureAvail(sizeDif+1);
                    encryptionXTEA.encrypt(out.buf,0,size);
                    //and write diff from 16
                    out.pos = size;
                    out.writeByte(sizeDif);
                }

                if(checksum){
                    CRC32 crc = new CRC32();
                    crc.update(out.buf,0,out.pos);
                    out.writeInt((int)crc.getValue());
                }

                if(CC.PARANOID)try{
                    //check that array is the same after deserialization
                    DataInput inp = new DataIO.DataInputByteArray(Arrays.copyOf(out.buf,out.pos));
                    byte[] decompress = deserialize(Serializer.BYTE_ARRAY_NOSIZE,out.pos,inp);

                    DataIO.DataOutputByteArray expected = newDataOut2();
                    serializer.serialize(expected,value);

                    byte[] expected2 = Arrays.copyOf(expected.buf, expected.pos);
                    //check arrays equals
                    if(CC.PARANOID && ! (Arrays.equals(expected2,decompress)))
                        throw new AssertionError();


                }catch(Exception e){
                    throw new RuntimeException(e);
                }
            }
            return out;
        } catch (IOException e) {
            throw new IOError(e);
        }

    }

    protected DataIO.DataOutputByteArray newDataOut2() {
        DataIO.DataOutputByteArray tmp = recycledDataOuts.poll();
        if(tmp==null) tmp = new DataIO.DataOutputByteArray();
        else tmp.pos=0;
        return tmp;
    }


    protected <A> A deserialize(Serializer<A> serializer, int size, DataInput input) throws IOException {
        DataIO.DataInputInternal di = (DataIO.DataInputInternal) input;
        if(size>0){
            if(checksum){
                //last two digits is checksum
                size -= 4;

                //read data into tmp buffer
                DataIO.DataOutputByteArray tmp = newDataOut2();
                tmp.ensureAvail(size);
                int oldPos = di.getPos();
                di.readFully(tmp.buf, 0, size);
                final int checkExpected = di.readInt();
                di.setPos(oldPos);
                //calculate checksums
                CRC32 crc = new CRC32();
                crc.update(tmp.buf, 0, size);
                recycledDataOuts.offer(tmp);
                int check = (int) crc.getValue();
                if(check!=checkExpected)
                    throw new IOException("Checksum does not match, data broken");
            }

            if(encrypt){
                DataIO.DataOutputByteArray tmp = newDataOut2();
                size-=1;
                tmp.ensureAvail(size);
                di.readFully(tmp.buf, 0, size);
                encryptionXTEA.decrypt(tmp.buf, 0, size);
                int cut = di.readUnsignedByte(); //length dif from 16bytes
                di = new DataIO.DataInputByteArray(tmp.buf);
                size -= cut;
            }

            if(compress) {
                //final int origPos = di.pos;
                int decompSize = DataIO.unpackInt(di);
                if(decompSize==0){
                    size-=1;
                    //rest of `di` is uncompressed data
                }else{
                    DataIO.DataOutputByteArray out = newDataOut2();
                    out.ensureAvail(decompSize);
                    CompressLZF lzf = LZF.get();
                    //TODO copy to heap if Volume is not mapped
                    //argument is not needed; unpackedSize= size-(di.pos-origPos),
                    byte[] b = di.internalByteArray();
                    if(b!=null) {
                        lzf.expand(b, di.getPos(), out.buf, 0, decompSize);
                    }else{
                        ByteBuffer bb = di.internalByteBuffer();
                        if(bb!=null) {
                            lzf.expand(bb, di.getPos(), out.buf, 0, decompSize);
                        }else{
                            lzf.expand(di,out.buf, 0, decompSize);
                        }
                    }
                    di = new DataIO.DataInputByteArray(out.buf);
                    size = decompSize;
                }
            }

        }

        int start = di.getPos();

        A ret = serializer.deserialize(di,size);
        if(size+start>di.getPos())
            throw new AssertionError("data were not fully read, check your serializer ");
        if(size+start<di.getPos())
            throw new AssertionError("data were read beyond record size, check your serializer");
        return ret;
    }


    /** traverses {@link EngineWrapper}s and returns underlying {@link Store}*/
    public static Store forDB(DB db){
        return forEngine(db.engine);
    }

    /** traverses {@link EngineWrapper}s and returns underlying {@link Store}*/
    public static Store forEngine(Engine e){
        if(e instanceof EngineWrapper)
            return forEngine(((EngineWrapper) e).getWrappedEngine());
        if(e instanceof TxEngine.Tx)
            return forEngine(((TxEngine.Tx) e).getWrappedEngine());
        return (Store) e;
    }

    protected int expectedMasks(){
        return (encrypt?ENCRYPT_FLAG_MASK:0) |
                (checksum?CHECKSUM_FLAG_MASK:0) |
                (compress?COMPRESS_FLAG_MASK:0);
    }

    private static final int LOCK_MASK = CC.CONCURRENCY-1;

    protected static int lockPos(final long key) {
        return DataIO.longHash(key) & LOCK_MASK;
    }

    @Override
    public boolean canSnapshot() {
        return false;
    }

    @Override
    public Engine snapshot() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Snapshots are not supported");
    }



}
