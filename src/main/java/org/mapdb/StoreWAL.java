package org.mapdb;

import java.io.IOError;
import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Write-Ahead-Log
 */
public class StoreWAL extends StoreDirect {

    protected static final byte WAL_INDEX_LONG = 101;
    protected static final byte WAL_PHYS_LONG = 102;
    protected static final byte WAL_PHYS_SIX_LONG = 103;
    protected static final byte WAL_PHYS_ARRAY = 104;
    protected static final byte WAL_SKIP_REST_OF_BLOCK = 105;

    /** last instruction in log file */
    protected static final byte WAL_SEAL = 111;
    /** added to offset 8 into log file, indicates that log was synced and closed*/
    protected static final long LOG_SEAL = 4566556446554645L;

    public static final String TRANS_LOG_FILE_EXT = ".t";

    protected static final long[] TOMBSTONE = new long[1];

    protected final Volume.Factory volFac;
    protected Volume log;

    protected long logSize;

    protected final LongConcurrentHashMap<long[]> modified = new LongConcurrentHashMap<long[]>();


    public StoreWAL(Volume.Factory volFac) {
        this(volFac,false,false);
    }
    public StoreWAL(Volume.Factory volFac, boolean readOnly, boolean deleteFilesAfterClose) {
        super(volFac, readOnly, deleteFilesAfterClose);
        this.volFac = volFac;
        this.log = volFac.createTransLogVolume();
        reloadIndexFile();
        replayLogFile();
        log = null;
    }

    protected void reloadIndexFile() {
        logSize = 0;
        modified.clear();
        indexSize = index.getLong(IO_INDEX_SIZE);
        physSize = index.getLong(IO_PHYS_SIZE);
    }

    protected void openLogIfNeeded(){
        if(log !=null) return;
        log = volFac.createTransLogVolume();
        log.ensureAvailable(16);
        log.putLong(0, HEADER);
        log.putLong(8, 0L);
        logSize = 16;
    }




    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        DataOutput2 out = serialize(value, serializer);

        final long ioRecid;
        final long[] physPos;
        final long[] logPos;

        structuralLock.lock();
        try{
            openLogIfNeeded();
            ioRecid = freeIoRecidTake(false);
            //first get space in phys
            physPos = physAllocate(out.pos,false);
            //now get space in log
            logPos = logAllocate(physPos);

        }finally{
            structuralLock.unlock();
        }

        //write data into log
        walIndexVal((logPos[0]&MASK_OFFSET) - 1-8-8-1-8, ioRecid, physPos[0]);
        walPhysArray(out, physPos, logPos);

        modified.put(ioRecid,logPos);
        return (ioRecid-IO_USER_START)/8;
    }

    protected void walPhysArray(DataOutput2 out, long[] physPos, long[] logPos) {
        //write byte[] data
        int outPos = 0;

        for(int i=0;i<logPos.length;i++){
            int c = ccc(logPos.length, i);
            long pos = logPos[i]&MASK_OFFSET;
            int size = (int) ((logPos[i]&MASK_SIZE) >>>48);

            log.putByte(pos -  8 - 1, WAL_PHYS_ARRAY);
            log.putLong(pos -  8, physPos[i]);

            if(c>0){
                log.putLong(pos, physPos[i + 1]);
                pos+=8;
            }
            if(c==12){
                log.putInt(pos, out.pos);
                pos+=4;
            }
            log.putData(pos, out.buf, outPos, size - c);
            outPos +=size-c;
        }
        if(outPos!=out.pos)throw new InternalError();
    }


    protected void walIndexVal(long logPos, long ioRecid, long indexVal) {

        log.putByte(logPos, WAL_INDEX_LONG);
        log.putLong(logPos + 1, ioRecid);
        log.putLong(logPos + 9, indexVal);

    }


    protected long[] logAllocate(long[] physPos) {

        openLogIfNeeded();
        logSize+=1+8+8; //space used for index val

        long[] ret = new long[physPos.length];
        for(int i=0;i<physPos.length;i++){
            long size = (physPos[i]&MASK_SIZE)>>>48;
            //would overlaps Volume Block?
            checkLogRounding();
            logSize+=1+8; //space used for WAL_PHYS_ARRAY
            ret[i] = (size<<48) | logSize;

            logSize+=size;

        }

        log.ensureAvailable(logSize);
        return ret;
    }

    protected void checkLogRounding() {
        if(logSize% Volume.BUF_SIZE+MAX_REC_SIZE*2>Volume.BUF_SIZE){
            log.ensureAvailable(logSize+1);
            log.putByte(logSize, WAL_SKIP_REST_OF_BLOCK);
            logSize += Volume.BUF_SIZE - logSize%Volume.BUF_SIZE;
        }
    }


    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        final long ioRecid = IO_USER_START + recid*8;
        Utils.readLock(locks,recid);
        try{
            return get2(ioRecid, serializer);
        }catch(IOException e){
            throw new IOError(e);
        }finally{
            Utils.readUnlock(locks,recid);
        }
    }

    @Override
    protected <A> A get2(long ioRecid, Serializer<A> serializer) throws IOException {
        //check if record was modified in current transaction
        long[] r = modified.get(ioRecid);
        //yes, read version
        if(r==null) return super.get2(ioRecid, serializer);
        //chech for tombstone (was deleted in current trans)
        if(r==TOMBSTONE || r.length==0) return null;

        //was modified in current transaction, so read it from trans log
        if(r.length==1){
            //single record
            final int size = (int) ((r[0]&MASK_SIZE)>>>48);
            DataInput2 in = log.getDataInput(r[0]&MASK_OFFSET, size);
            return serializer.deserialize(in, size);
        }else{
            //linked record
            int totalSize = 0;
            for(int i=0;i<r.length;i++){
                int c = ccc(r.length, i);
                totalSize+=  (int) ((r[i]&MASK_SIZE)>>>48)-c;
            }
            byte[] b = new byte[totalSize];
            int pos = 0;
            for(int i=0;i<r.length;i++){
                int c = ccc(r.length, i);
                int size = (int) ((r[i]&MASK_SIZE)>>>48) -c;
                log.getDataInput((r[i] & MASK_OFFSET) + c, size).readFully(b,pos,size);
                pos+=size;
            }
            if(pos!=totalSize)throw new InternalError();

            return serializer.deserialize(new DataInput2(b),totalSize);
        }
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        DataOutput2 out = serialize(value, serializer);
        final long ioRecid = IO_USER_START + recid*8;
        Utils.writeLock(locks,recid);
        try{
            final long[] physPos;
            final long[] logPos;
            structuralLock.lock();
            try{
                openLogIfNeeded();
                //first get space in phys
                physPos = physAllocate(out.pos,false);
                //now get space in log
                logPos = logAllocate(physPos);

            }finally{
                structuralLock.unlock();
            }

            //write data into log
            walIndexVal((logPos[0]&MASK_OFFSET) - 1-8-8-1-8, ioRecid, physPos[0]);
            walPhysArray(out, physPos, logPos);

            modified.put(ioRecid,logPos);
        }finally{
            Utils.writeUnlock(locks,recid);
        }
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        final long ioRecid = IO_USER_START + recid*8;
        Utils.writeLock(locks,recid);
        try{

            A oldVal = get2(ioRecid,serializer);
            if((oldVal == null && expectedOldValue!=null) || (oldVal!=null && !oldVal.equals(expectedOldValue)))
                return false;

            DataOutput2 out = serialize(newValue, serializer);

            final long[] physPos;
            final long[] logPos;
            structuralLock.lock();
            try{
                openLogIfNeeded();
                //first get space in phys
                physPos = physAllocate(out.pos,false);
                //now get space in log
                logPos = logAllocate(physPos);

            }finally{
                structuralLock.unlock();
            }

            //write data into log
            walIndexVal((logPos[0]&MASK_OFFSET) - 1-8-8-1-8, ioRecid, physPos[0]);
            walPhysArray(out, physPos, logPos);

            modified.put(ioRecid,logPos);
            return true;
        }catch(IOException e){
            throw new IOError(e);
        }finally{
            Utils.writeUnlock(locks,recid);
        }

    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer) {
        final long ioRecid = IO_USER_START + recid*8;
        Utils.writeLock(locks,recid);
        try{
            structuralLock.lock();
            final long logPos;
            try{
                openLogIfNeeded();
                logPos = logSize;
                checkLogRounding();
                logSize+=1+8+8; //space used for index val
                log.ensureAvailable(logSize);

            }finally {
                structuralLock.unlock();
            }
            walIndexVal(logPos,ioRecid,0);
            modified.put(ioRecid,TOMBSTONE);
        }finally {
            Utils.writeUnlock(locks,recid);
        }
        }

    @Override
    public void commit() {
        structuralLock.lock();
        Utils.writeLockAll(locks);
        try{
            if(log==null) return; //no modifications
            //update physical and logical filesize

            log.ensureAvailable(logSize + 17 + 17 + 1);
            walIndexVal(logSize,IO_PHYS_SIZE, physSize);
            logSize+=17;
            walIndexVal(logSize,IO_INDEX_SIZE, indexSize);
            logSize+=17;
            //seal log file
            log.putByte(logSize, WAL_SEAL);
            logSize+=1;
            //flush log file
            log.sync();
            //and write mark it was sealed
            log.putLong(8, LOG_SEAL);
            log.sync();

            replayLogFile();
            reloadIndexFile();

        }finally {
            Utils.writeUnlockAll(locks);
            structuralLock.unlock();
        }
    }

    protected void replayLogFile(){

        logSize = 0;

        if(log !=null){
            log.sync();
        }


        //read headers
        if(log.isEmpty() || log.getLong(0)!=HEADER || log.getLong(8) !=LOG_SEAL){
            //wrong headers, discard log
            log.close();
            log.deleteFile();
            log = null;
            return;
        }


        //all good, start replay
        logSize=16;
        byte ins = log.getByte(logSize);
        logSize+=1;

        while(ins!=WAL_SEAL){
            if(ins == WAL_INDEX_LONG){
                long ioRecid = log.getLong(logSize);
                logSize+=8;
                long indexVal = log.getLong(logSize);
                logSize+=8;
                index.ensureAvailable(ioRecid+8);
                index.putLong(ioRecid, indexVal);
            }else if(ins == WAL_PHYS_LONG){
                long offset = log.getLong(logSize);
                logSize+=8;
                long val = log.getLong(logSize);
                logSize+=8;
                phys.ensureAvailable(offset+8);
                phys.putLong(offset,val);
            }else if(ins == WAL_PHYS_SIX_LONG){
                long offset = log.getLong(logSize);
                logSize+=8;
                long val = log.getSixLong(logSize);
                logSize+=6;
                phys.ensureAvailable(offset+6);
                phys.putSixLong(offset, val);
            }else if(ins == WAL_PHYS_ARRAY){
                long offset = log.getLong(logSize);
                logSize+=8;
                final int size = (int) ((offset&MASK_SIZE)>>>48);
                offset = offset&MASK_OFFSET;

                //transfer byte[] directly from log file without copying into memory
                DataInput2 input = log.getDataInput(logSize, size);
                synchronized (input.buf){
                    input.buf.position(input.pos);
                    input.buf.limit(input.pos+size);
                    phys.ensureAvailable(offset+size);
                    phys.putData(offset, input.buf);
                    input.buf.clear();
                }
                logSize+=size;
            }else if(ins == WAL_SKIP_REST_OF_BLOCK){
                logSize += Volume.BUF_SIZE-logSize%Volume.BUF_SIZE;
            }else{
                throw new InternalError("unknown trans log instruction: "+ins +" at log offset: "+(logSize-1));
            }

            ins = log.getByte(logSize);
            logSize+=1;
        }
        logSize=0;

        //flush dbs
        phys.sync();
        index.sync();
        //and discard log
        log.putLong(0, 0);
        log.putLong(8, 0); //destroy seal to prevent log file from being replayed
        log.close();
        log.deleteFile();
        log = null;
    }



    @Override
    public void rollback() throws UnsupportedOperationException {
        structuralLock.lock();
        Utils.writeLockAll(locks);
        try{
            //discard trans log
            if(log !=null){
                log.close();
                log.deleteFile();
                log = null;
            }

            reloadIndexFile();
        }finally {
            Utils.writeUnlockAll(locks);
            structuralLock.unlock();
        }
    }

    @Override
    protected long longStackTake(long ioList) {
        return 0;
    }

    @Override
    protected void longStackPut(long ioList, long offset) {
        //TODO long stack in WAL
    }

    @Override
    public void close() {
        structuralLock.lock();
        Utils.writeLockAll(locks);
        try{
            if(log !=null){
                log.sync();
                log.close();
                if(deleteFilesAfterClose){
                    log.deleteFile();
                }
            }

            index.sync();
            phys.sync();

            index.close();
            phys.close();
            if(deleteFilesAfterClose){
                index.deleteFile();
                phys.deleteFile();
            }
            index = null;
            phys = null;
        }finally {
            Utils.writeUnlockAll(locks);
            structuralLock.unlock();
        }
    }
}
