package org.mapdb;

import java.io.DataInput;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;

/**
 *
 */
public abstract class Store implements Engine {


    /** protects structural layout of records. Memory allocator is single threaded under this lock */
    protected final ReentrantLock structuralLock = new ReentrantLock(CC.FAIR_LOCKS);

    /** protects lifecycle methods such as commit, rollback and close() */
    protected final ReentrantLock commitLock = new ReentrantLock(CC.FAIR_LOCKS);

    /** protects data from being overwritten while read */
    protected final ReentrantReadWriteLock[] locks;


    protected volatile boolean closed = false;
    protected final boolean readonly;

    protected final String fileName;
    protected Fun.Function1<Volume, String> volumeFactory;
    protected boolean checksum;
    protected boolean compress;
    protected boolean encrypt;
    protected final EncryptionXTEA encryptionXTEA;
    protected final ThreadLocal<CompressLZF> LZF;


    protected Store(
            String fileName,
            Fun.Function1<Volume, String> volumeFactory,
            boolean checksum,
            boolean compress,
            byte[] password,
            boolean readonly) {
        this.fileName = fileName;
        this.volumeFactory = volumeFactory;
        locks = new ReentrantReadWriteLock[CC.CONCURRENCY];
        for(int i=0;i< locks.length;i++){
            locks[i] = new ReentrantReadWriteLock(CC.FAIR_LOCKS);
        }

        this.checksum = checksum;
        this.compress = compress;
        this.encrypt =  password!=null;
        this.readonly = readonly;
        this.encryptionXTEA = !encrypt?null:new EncryptionXTEA(password);

        this.LZF = !compress?null:new ThreadLocal<CompressLZF>() {
            @Override
            protected CompressLZF initialValue() {
                return new CompressLZF();
            }
        };
    }

    public void init(){}

    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        if(serializer==null)
            throw new NullPointerException();

        final Lock lock = locks[lockPos(recid)].readLock();
        lock.lock();
        try{
            return get2(recid,serializer);
        }finally {
            lock.unlock();
        }
    }

    protected abstract <A> A get2(long recid, Serializer<A> serializer);

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        if(serializer==null)
            throw new NullPointerException();

        //serialize outside lock
        DataIO.DataOutputByteArray out = serialize(value, serializer);

        final Lock lock = locks[lockPos(recid)].writeLock();
        lock.lock();
        try{
            update2(recid,out);
        }finally {
            lock.unlock();
        }
    }

    protected final AtomicReference<DataIO.DataOutputByteArray> recycledDataOut =
            new AtomicReference<DataIO.DataOutputByteArray>();

    protected <A> DataIO.DataOutputByteArray serialize(A value, Serializer<A> serializer){
        if(value==null)
            return null;
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
                        recycledDataOut.lazySet(tmp);
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
                        recycledDataOut.lazySet(tmp);
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
                    DataInput inp = new DataIO.DataInputByteArray(Arrays.copyOf(out.buf, out.pos));
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
        DataIO.DataOutputByteArray tmp = recycledDataOut.getAndSet(null);
        if(tmp==null) tmp = new DataIO.DataOutputByteArray();
        else tmp.pos=0;
        return tmp;
    }


    protected <A> A deserialize(Serializer<A> serializer, int size, DataInput input){
        try {
            //TODO if serializer is not trusted, use boundary check
            //TODO return future and finish deserialization outside lock, does even bring any performance bonus?

            DataIO.DataInputInternal di = (DataIO.DataInputInternal) input;
            if (size > 0) {
                if (checksum) {
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
                    recycledDataOut.lazySet(tmp);
                    int check = (int) crc.getValue();
                    if (check != checkExpected)
                        throw new IOException("Checksum does not match, data broken");
                }

                if (encrypt) {
                    DataIO.DataOutputByteArray tmp = newDataOut2();
                    size -= 1;
                    tmp.ensureAvail(size);
                    di.readFully(tmp.buf, 0, size);
                    encryptionXTEA.decrypt(tmp.buf, 0, size);
                    int cut = di.readUnsignedByte(); //length dif from 16bytes
                    di = new DataIO.DataInputByteArray(tmp.buf);
                    size -= cut;
                }

                if (compress) {
                    //final int origPos = di.pos;
                    int decompSize = DataIO.unpackInt(di);
                    if (decompSize == 0) {
                        size -= 1;
                        //rest of `di` is uncompressed data
                    } else {
                        DataIO.DataOutputByteArray out = newDataOut2();
                        out.ensureAvail(decompSize);
                        CompressLZF lzf = LZF.get();
                        //TODO copy to heap if Volume is not mapped
                        //argument is not needed; unpackedSize= size-(di.pos-origPos),
                        byte[] b = di.internalByteArray();
                        if (b != null) {
                            lzf.expand(b, di.getPos(), out.buf, 0, decompSize);
                        } else {
                            ByteBuffer bb = di.internalByteBuffer();
                            if (bb != null) {
                                lzf.expand(bb, di.getPos(), out.buf, 0, decompSize);
                            } else {
                                lzf.expand(di, out.buf, 0, decompSize);
                            }
                        }
                        di = new DataIO.DataInputByteArray(out.buf);
                        size = decompSize;
                    }
                }

            }

            int start = di.getPos();

            A ret = serializer.deserialize(di, size);
            if (size + start > di.getPos())
                throw new AssertionError("data were not fully read, check your serializer ");
            if (size + start < di.getPos())
                throw new AssertionError("data were read beyond record size, check your serializer");
            return ret;
        }catch(IOException e){
            throw new IOError(e);
        }
    }

    protected abstract  void update2(long recid, DataIO.DataOutputByteArray out);

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        if(serializer==null)
            throw new NullPointerException();

        //TODO binary CAS & serialize outside lock
        final Lock lock = locks[lockPos(recid)].writeLock();
        lock.lock();
        try{
            A oldVal = get2(recid,serializer);
            if(oldVal==expectedOldValue || (oldVal!=null && serializer.equals(oldVal,expectedOldValue))){
                update2(recid,serialize(newValue,serializer));
                return true;
            }
            return false;
        }finally {
            lock.unlock();
        }
    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer) {
        if(serializer==null)
            throw new NullPointerException();

        final Lock lock = locks[lockPos(recid)].writeLock();
        lock.lock();
        try{
            delete2(recid, serializer);
        }finally {
            lock.unlock();
        }
    }

    protected abstract <A> void delete2(long recid, Serializer<A> serializer);

    private static final int LOCK_MASK = CC.CONCURRENCY-1;

    protected static final int lockPos(final long recid) {
        return DataIO.longHash(recid) & LOCK_MASK;
    }

    protected void assertReadLocked(long recid) {
//        if(locks[lockPos(recid)].writeLock().getHoldCount()!=0){
//            throw new AssertionError();
//        }
    }

    protected void assertWriteLocked(long recid) {
        if(!locks[lockPos(recid)].isWriteLockedByCurrentThread()){
            throw new AssertionError();
        }
    }


    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public boolean isReadOnly() {
        return readonly;
    }

    /** traverses {@link EngineWrapper}s and returns underlying {@link Store}*/
    public static Store forDB(DB db){
        return forEngine(db.engine);
    }

    /** traverses {@link EngineWrapper}s and returns underlying {@link Store}*/
    public static Store forEngine(Engine e){
        if(e instanceof EngineWrapper)
            return forEngine(((EngineWrapper) e).getWrappedEngine());
        return (Store) e;
    }

    public abstract long getCurrSize();

    public abstract long getFreeSize();

}
