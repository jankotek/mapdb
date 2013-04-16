package org.mapdb;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Append only storage. Uses different file format than Direct and Journaled storage
 */

// TODO mod operation '%' is significantly slower than bitwise and. If you use size w/ exact pow 2, bitwise and is a no-brainer for position numbers.
public class StoreAppend implements Engine{

    protected final File file;
    protected final boolean useRandomAccessFile;
    protected final boolean readOnly;

    protected final static long FILE_NUMBER_SHIFT = 28;
    protected final static long FILE_OFFSET_MASK = 0x0FFFFFFFL;

    protected final static long FILE_HEADER = 56465465456465L;

    protected static final int CONCURRENCY_FACTOR = 32;
    protected final ReentrantReadWriteLock[] readLocks;
    protected final Lock structuralLock = new ReentrantLock();
    protected final static Long THUMBSTONE = Long.MIN_VALUE;
    protected final static int THUMBSTONE_SIZE = -3;
    protected final static long EOF = -1;
    protected final static long COMMIT = -2;
    protected final static long ROLLBACK = -2;

    volatile protected Volume currentVolume;
    volatile protected long currentVolumeNum;
    volatile protected int currentFileOffset;
    volatile protected long maxRecid;

    protected LongConcurrentHashMap<Volume> volumes = new LongConcurrentHashMap<Volume>();
    protected final LongConcurrentHashMap<Long> recidsInTx = new LongConcurrentHashMap<Long>();


    protected final Volume recidsTable = new Volume.MemoryVol(true);
    protected static final int MAX_FILE_SIZE = 1024 * 1024 * 10;
    protected final boolean deleteFilesAfterClose;

    public StoreAppend(File file, boolean useRandomAccessFile, boolean readOnly, boolean transactionsDisabled,  boolean deleteFilesAfterClose) {
        this.file = file;
        this.useRandomAccessFile = useRandomAccessFile;
        this.readOnly = readOnly;
        this.deleteFilesAfterClose = deleteFilesAfterClose;
        //TODO special mode with transactions disabled

        readLocks = new ReentrantReadWriteLock[CONCURRENCY_FACTOR];
        for(int i=0;i<readLocks.length;i++) readLocks[i] = new ReentrantReadWriteLock();

        File zeroFile = getFileNum(0);
        if(zeroFile.exists()){
            replayLog();
        }else{
            //create zero file
            recidsTable.ensureAvailable(LAST_RESERVED_RECID*8+8);
            for(long i=0;i<=LAST_RESERVED_RECID;i++){
                recidsTable.putLong(i*8,0);
            }
            maxRecid=LAST_RESERVED_RECID+1;
            currentVolume = Volume.volumeForFile(zeroFile, useRandomAccessFile, readOnly);
            currentVolume.ensureAvailable(8);
            currentVolume.putLong(0, FILE_HEADER);
            currentFileOffset = 8;
            volumes.put(0L, currentVolume);
        }




    }

    protected void replayLog() {
        try{
        for(long fileNum=0;;fileNum++){
            File f = getFileNum(fileNum);
            if(!f.exists()) return;
            currentVolume = Volume.volumeForFile(f, useRandomAccessFile, readOnly);
            volumes.put(fileNum, currentVolume);
            currentVolumeNum = fileNum;

            //replay file and rebuild recid index table
            LongHashMap<Long> recidsTable2 = new LongHashMap<Long>();
            if(!currentVolume.isEmpty()){
                currentFileOffset =0;
                long header = currentVolume.getLong(currentFileOffset); currentFileOffset+=8;
                if(header!=FILE_HEADER) throw new InternalError();

                for(;;){
                    long recid = currentVolume.getLong(currentFileOffset); currentFileOffset+=8;
                    maxRecid = Math.max(recid, maxRecid);

                    if(recid == EOF || recid == 0){
                        break; //end of file
                    }else if(recid == COMMIT){
                        //move stuff from temporary table to currently used
                        commitRecids(recidsTable2);
                        continue;
                    }else if(recid == ROLLBACK){
                        //do not use last recids
                        recidsTable2.clear();
                        continue;
                    }

                    long filePos = (fileNum<<FILE_NUMBER_SHIFT) | currentFileOffset;
                    int size = currentVolume.getInt(currentFileOffset); currentFileOffset+=4;
                    if(size!=THUMBSTONE_SIZE){
                        //skip data
                        currentFileOffset+=size;
                        //store location within the log files in memory
                        recidsTable2.put(recid, filePos);
                    }else{
                        //record was deleted (THUMBSTONE mark)
                        recidsTable2.put(recid, THUMBSTONE);
                    }
                }

            }
        }
        }catch(IOError e){
            //TODO error is part of workflow, but maybe change workflow?
        }
    }

    protected File getFileNum(long fileNum) {
        return new File(file.getPath()+"."+fileNum);
    }


    protected void commitRecids(LongMap<Long> recidsTable2) {
        LongMap.LongMapIterator<Long> iter = recidsTable2.longMapIterator();
        while(iter.moveToNext()){
            long recidsTableOffset = iter.key()*8;
            recidsTable.ensureAvailable(recidsTableOffset+8);
            recidsTable.putLong(recidsTableOffset, iter.value());
        }
        recidsTable2.clear();
    }


    @Override
    public <A> long put(A value, Serializer<A> serializer) {

        DataOutput2 out = Utils.serializer(serializer,value);

        long recid;
        long pos;
        long volNum;
        Volume vol;
        structuralLock.lock();
        try{
            recid= maxRecid++; //TODO free recid management
            pos = currentFileOffset;
            currentFileOffset += 8 + 4 + out.pos;
            currentVolume.ensureAvailable(currentFileOffset);
            volNum = currentVolumeNum;
            vol = currentVolume;
            rollOverFile();
        }finally {
            structuralLock.unlock();
        }
        Lock lock = readLocks[Utils.longHash(recid)%readLocks.length].writeLock();
        lock.lock();
        try{
            vol.putLong(pos, recid);
            pos+=8;
            long filePos = (volNum<<FILE_NUMBER_SHIFT) | pos;
            vol.putInt(pos,out.pos);
            pos+=4;
            vol.putData(pos,out.buf, 0, out.pos);
            recidsInTx.put(recid, filePos);

            return recid;
        }finally {
            lock.unlock();
        }
    }

    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        Lock lock = readLocks[Utils.longHash(recid)%readLocks.length].readLock();
        lock.lock();
        try {
            return getNoLock(recid, serializer);
        } catch (IOException e) {
            throw new IOError(e);
        }finally {
            lock.unlock();
        }

    }

    protected <A> A getNoLock(long recid, Serializer<A> serializer) throws IOException {
        Long fileNum2 = recidsInTx.get(recid);
        if(fileNum2 == null){
                recidsTable.ensureAvailable(recid*8+8);
                fileNum2 = recidsTable.getLong(recid*8);
        }

        if(fileNum2 == THUMBSTONE){  //there is warning about '==', it is ok
            //record was deleted;
            return null;
        }

        if(fileNum2 == 0){
            return serializer.deserialize(new DataInput2(new byte[0]), 0);
        }

        long fileNum = fileNum2;

        long fileOffset = fileNum & FILE_OFFSET_MASK;
        if(fileOffset>MAX_FILE_SIZE) throw new InternalError();
        fileNum = fileNum>>>FILE_NUMBER_SHIFT;
        Volume v = volumes.get(fileNum);

        int size = v.getInt(fileOffset);
        DataInput2 input = v.getDataInput(fileOffset+4, size);

        return serializer.deserialize(input, size);
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        DataOutput2 out = Utils.serializer(serializer,value);
        Lock lock = readLocks[Utils.longHash(recid)%readLocks.length].writeLock();
        lock.lock();
        try{

            long pos;
            long volNum;
            Volume vol;
            structuralLock.lock();
            try{
                pos = currentFileOffset;
                currentFileOffset += 8 + 4 + out.pos;
                currentVolume.ensureAvailable(currentFileOffset);
                volNum = currentVolumeNum;
                vol = currentVolume;
                rollOverFile();
            }finally {
                structuralLock.unlock();
            }
            vol.putLong(pos, recid);
            pos+=8;
            long filePos = (volNum<<FILE_NUMBER_SHIFT) | pos;
            vol.putInt(pos,out.pos);
            pos+=4;
            vol.putData(pos,out.buf, 0, out.pos);
            recidsInTx.put(recid, filePos);
        }finally {
            lock.unlock();
        }
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        Lock lock = readLocks[Utils.longHash(recid)%readLocks.length].writeLock();
        lock.lock();
        try{
            Object oldVal = get(recid, serializer);
            //TODO compare binary stuff?
            if(!((oldVal==null && expectedOldValue==null)|| (oldVal!=null && oldVal.equals(expectedOldValue)))){
                return false;
            }

            DataOutput2 out = Utils.serializer(serializer,newValue);

            long pos;
            long volNum;
            Volume vol;
            structuralLock.lock();
            try{
                pos = currentFileOffset;
                currentFileOffset += 8 + 4 + out.pos;
                currentVolume.ensureAvailable(currentFileOffset);
                volNum = currentVolumeNum;
                vol = currentVolume;
                rollOverFile();
            }finally {
                structuralLock.unlock();
            }
            vol.putLong(pos, recid);
            pos+=8;
            long filePos = (volNum<<FILE_NUMBER_SHIFT) | pos;
            vol.putInt(pos,out.pos);
            pos+=4;
            vol.putData(pos,out.buf, 0, out.pos);
            recidsInTx.put(recid, filePos);
            return true;
        }finally {
            lock.unlock();
        }
    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer){
        Lock lock = readLocks[Utils.longHash(recid)%readLocks.length].writeLock();
        lock.lock();
        try{
            structuralLock.lock();
            try{
                currentVolume.ensureAvailable(currentFileOffset+8+4);
                currentVolume.putLong(currentFileOffset, recid);
                currentFileOffset+=8;
                currentVolume.putInt(currentFileOffset, THUMBSTONE_SIZE);
                currentFileOffset+=4;
                recidsInTx.put(recid, THUMBSTONE);
                rollOverFile();

            }finally{
                structuralLock.unlock();
            }

        }finally {
            lock.unlock();
        }

    }

    @Override
    public void close() {
        structuralLock.lock();
        currentVolume.sync();
        currentVolume.close();

        if(deleteFilesAfterClose)
            currentVolume.deleteFile();
        for(Iterator<Volume> volIter = volumes.valuesIterator();volIter.hasNext();){
            Volume vol = volIter.next();
            if(vol==null) continue;
            if(deleteFilesAfterClose)vol.deleteFile();
        }

        currentVolume = null;
        volumes = null;
        structuralLock.unlock();
    }

    @Override
    public boolean isClosed() {
        return volumes==null;
    }

    @Override
    public void commit() {
        //TODO lock all locks?
        //append commit mark
        structuralLock.lock();
        try{
            commitRecids(recidsInTx);
            currentVolume.ensureAvailable(currentFileOffset+8);
            currentVolume.putLong(currentFileOffset, COMMIT);
            currentFileOffset+=8;
            currentVolume.sync();
            rollOverFile();
        }finally {
            structuralLock.unlock();
        }
    }

    @Override
    public void rollback() throws UnsupportedOperationException {
        //TODO lock all locks?
        //append rollback mark
        structuralLock.lock();
        try{
            currentVolume.ensureAvailable(currentFileOffset+8);
            currentVolume.putLong(currentFileOffset, ROLLBACK);
            currentFileOffset+=8;
            currentVolume.sync();
            recidsInTx.clear();
            rollOverFile();
        }finally {
            structuralLock.unlock();
        }


    }


    /** check if current file is too big, if yes finish it and start next file */
    protected void rollOverFile(){
        if(currentFileOffset<MAX_FILE_SIZE-8) return;

        currentVolume.ensureAvailable(currentFileOffset + 8);
        currentVolume.putLong(currentFileOffset, EOF);
        currentVolume.sync();
        currentVolumeNum++;
        currentVolume = Volume.volumeForFile(
                getFileNum(currentVolumeNum), useRandomAccessFile, readOnly);
        currentVolume.ensureAvailable(MAX_FILE_SIZE);
        currentVolume.putLong(0, FILE_HEADER);
        currentFileOffset = 8;
        currentVolume.sync();
        volumes.put(currentVolumeNum,currentVolume);
    }


    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public void compact() {
        //traverse list of recids, find and delete files which are not used
        //TODO lock all locks?
        structuralLock.lock();
        try{
            if(!recidsInTx.isEmpty()) throw new IllegalAccessError("Uncommited changes");

            LongHashMap<Boolean> ff = new LongHashMap<Boolean>();
            for(long recid=0;recid<maxRecid;recid++){
                long indexVal = recidsTable.getLong(recid*8);
                if(indexVal ==0)continue;
                long fileNum = indexVal>>>FILE_NUMBER_SHIFT;
                ff.put(fileNum,true);
            }

            //now traverse files and delete unused
            LongMap.LongMapIterator<Volume> iter = volumes.longMapIterator();
            while(iter.moveToNext()){
                long recid = iter.key();
                if(ff.get(recid)!=null) continue;
                Volume v = iter.value();
                v.close();
                v.deleteFile();
                iter.remove();
            }

        }finally {
            structuralLock.unlock();
        }


    }

}


