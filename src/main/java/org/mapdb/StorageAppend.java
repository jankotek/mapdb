package org.mapdb;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Append only storage. Uses different file format than Direct and Journaled storage
 */
public class StorageAppend implements Engine{

    protected final File file;
    protected final boolean useRandomAccessFile;
    protected final boolean readOnly;

    protected final static long FILE_NUMBER_SHIFT = 28;
    protected final static long FILE_OFFSET_MASK = 0x0FFFFFFFL;

    protected final static long FILE_HEADER = 56465465456465L;

    protected final ReentrantReadWriteLock appendLock = new ReentrantReadWriteLock();
    protected final static Long THUMBSTONE = Long.MIN_VALUE;
    protected final static int THUMBSTONE_SIZE = -3;
    protected final static long EOF = -1;
    protected final static long COMMIT = -2;
    protected final static long ROLLBACK = -2;

    protected Volume currentVolume;
    protected long currentVolumeNum;
    protected int currentFileOffset;
    protected long maxRecid = 10;

    protected LongConcurrentHashMap<Volume> volumes = new LongConcurrentHashMap<Volume>();
    protected final LongConcurrentHashMap<Long> recidsInTx = new LongConcurrentHashMap<Long>();


    protected final Volume recidsTable = new Volume.MemoryVol(true);
    protected static final int MAX_FILE_SIZE = 1024 * 1024 * 10;

    public StorageAppend(File file, boolean useRandomAccessFile, boolean readOnly, boolean transactionsDisabled) {
        this.file = file;
        this.useRandomAccessFile = useRandomAccessFile;
        this.readOnly = readOnly;
        //TODO special mode with transactions disabled

        File zeroFile = getFileNum(0);
        if(zeroFile.exists()){
            replayLog();
        }else{
            //create zero file
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
                int pos =0;
                long header = currentVolume.getLong(pos); pos+=8;
                if(header!=FILE_HEADER) throw new InternalError();

                for(;;){
                    long recid = currentVolume.getLong(pos); pos+=8;
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

                    long filePos = (fileNum<<FILE_NUMBER_SHIFT) | pos;
                    int size = currentVolume.getInt(pos); pos+=4;
                    if(size!=THUMBSTONE_SIZE){
                        //skip data
                        pos+=size;
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
        try{
            DataOutput2 out = new DataOutput2();
            serializer.serialize(out, value);
            appendLock.writeLock().lock();
            try{

                long newRecid = maxRecid++; //TODO free recid management
                update2(newRecid, out);
                rollOverFile();
                return newRecid;
            }finally {
                appendLock.writeLock().unlock();
            }
        }catch(IOException e){
            throw new IOError(e);
        }
    }

    protected void update2(long recid, DataOutput2 out) {
        currentVolume.ensureAvailable(currentFileOffset+8+4+out.pos);
        currentVolume.putLong(currentFileOffset,recid);
        currentFileOffset+=8;
        long filePos = (currentVolumeNum<<FILE_NUMBER_SHIFT) | currentFileOffset;

        currentVolume.putInt(currentFileOffset,out.pos);
        currentFileOffset+=4;
        currentVolume.putData(currentFileOffset,out.buf, out.pos);
        currentFileOffset+=out.pos;
        recidsInTx.put(recid, filePos);
    }

    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        appendLock.readLock().lock();
        try {
            Long fileNum2 = recidsInTx.get(recid);
            if(fileNum2 == null)
                    fileNum2 = recidsTable.getLong(recid*8);

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
        } catch (IOException e) {
            throw new IOError(e);
        }finally {
            appendLock.readLock().unlock();
        }

    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        try{
            DataOutput2 out = new DataOutput2();
            serializer.serialize(out, value);
            appendLock.writeLock().lock();
            try {
                update2(recid, out);
                rollOverFile();
            }finally {
                appendLock.writeLock().unlock();
            }

        }catch(IOException e){
            throw new IOError(e);
        }

    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        appendLock.writeLock().lock();
        try{
            Object oldVal = get(recid, serializer);
            //TODO compare binary stuff?
            if((oldVal==null && expectedOldValue==null)|| (oldVal!=null && oldVal.equals(expectedOldValue))){
                DataOutput2 out = new DataOutput2();
                try {
                    serializer.serialize(out, newValue); //TODO serialize outside of APPEND_LOCK
                } catch (IOException e) {
                    throw new IOError(e);
                }
                update2(recid, out);
                rollOverFile();
                return true;
            }else{
                return false;
            }
        }finally {
            appendLock.writeLock().unlock();
        }

    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer){
        //put thumbstone into log
        appendLock.writeLock().lock();
        try{

            currentVolume.ensureAvailable(currentFileOffset+8+4);
            currentVolume.putLong(currentFileOffset, recid);
            currentFileOffset+=8;
            currentVolume.putInt(currentFileOffset, THUMBSTONE_SIZE);
            currentFileOffset+=4;
            recidsInTx.put(recid, THUMBSTONE);
            rollOverFile();
        }finally {
            appendLock.writeLock().unlock();
        }

    }

    @Override
    public void close() {
        currentVolume = null;
        volumes = null;
    }

    @Override
    public boolean isClosed() {
        return volumes==null;
    }

    @Override
    public void commit() {
        //append commit mark
        appendLock.writeLock().lock();
        try{
            commitRecids(recidsInTx);
            currentVolume.ensureAvailable(currentFileOffset+8);
            currentVolume.putLong(currentFileOffset, COMMIT);
            currentFileOffset+=8;
            currentVolume.sync();
            rollOverFile();
        }finally {
            appendLock.writeLock().unlock();
        }
    }

    @Override
    public void rollback() throws UnsupportedOperationException {
        //append rollback mark
        appendLock.writeLock().lock();
        try{
            currentVolume.ensureAvailable(currentFileOffset+8);
            currentVolume.putLong(currentFileOffset, ROLLBACK);
            currentFileOffset+=8;
            currentVolume.sync();
            recidsInTx.clear();
            rollOverFile();
        }finally {
            appendLock.writeLock().unlock();
        }


    }


    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public void compact() {
        //TODO implement compaction on StorageAppend
    }

    /** check if current file is too big, if yes finish it and start next file */
    protected void rollOverFile(){
        if(currentFileOffset<MAX_FILE_SIZE-8) return;


        currentVolume.ensureAvailable(currentFileOffset+8);
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

}


