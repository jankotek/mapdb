package org.mapdb;

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Write-Ahead-Log
 */
public class StoreWAL extends StoreDirect {

    protected static final long LOG_MASK_OFFSET = 0x0000FFFFFFFFFFFFL;

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
    protected final LongMap<long[]> longStackPages = new LongHashMap<long[]>();
    protected final long[] indexVals = new long[IO_USER_START/8];
    protected final boolean[] indexValsModified = new boolean[indexVals.length];


    public StoreWAL(Volume.Factory volFac) {
        this(volFac,false,false,5,false,0L);
    }
    public StoreWAL(Volume.Factory volFac, boolean readOnly, boolean deleteFilesAfterClose,
                    int spaceReclaimMode, boolean syncOnCommitDisabled, long sizeLimit) {
        super(volFac, readOnly, deleteFilesAfterClose, spaceReclaimMode,syncOnCommitDisabled,sizeLimit);
        this.volFac = volFac;
        this.log = volFac.createTransLogVolume();

        reloadIndexFile();
        replayLogFile();
        log = null;
    }

    protected void reloadIndexFile() {
        logSize = 0;
        modified.clear();
        longStackPages.clear();
        indexSize = index.getLong(IO_INDEX_SIZE);
        physSize = index.getLong(IO_PHYS_SIZE);
        freeSize = index.getLong(IO_FREE_SIZE);
        for(int i = IO_FREE_RECID;i<IO_USER_START;i+=8){
            indexVals[i/8] = index.getLong(i);
        }
        Arrays.fill(indexValsModified, false);
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
        walIndexVal((logPos[0]&LOG_MASK_OFFSET) - 1-8-8-1-8, ioRecid, physPos[0]|MASK_ARCHIVE);
        walPhysArray(out, physPos, logPos);

        modified.put(ioRecid,logPos);
        recycledDataOuts.offer(out);
        return (ioRecid-IO_USER_START)/8;
    }

    protected void walPhysArray(DataOutput2 out, long[] physPos, long[] logPos) {
        //write byte[] data
        int outPos = 0;

        for(int i=0;i<logPos.length;i++){
            int c =  i==logPos.length-1 ? 0: 8;
            long pos = logPos[i]&LOG_MASK_OFFSET;
            int size = (int) (logPos[i]>>>48);

            log.putByte(pos -  8 - 1, WAL_PHYS_ARRAY);
            log.putLong(pos -  8, physPos[i]);

            if(c>0){
                log.putLong(pos, physPos[i + 1]);
                pos+=8;
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
            long size = physPos[i]>>>48;
            //would overlaps Volume Block?
            logSize+=1+8; //space used for WAL_PHYS_ARRAY
            ret[i] = (size<<48) | logSize;

            logSize+=size;
            checkLogRounding();

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
        final Lock lock  = locks[Utils.longHash(recid)&Utils.LOCK_MASK].readLock();
        lock.lock();
        try{
            return get2(ioRecid, serializer);
        }catch(IOException e){
            throw new IOError(e);
        }finally{
            lock.unlock();
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
            final int size = (int) (r[0]>>>48);
            DataInput2 in = log.getDataInput(r[0]&LOG_MASK_OFFSET, size);
            return serializer.deserialize(in, size);
        }else{
            //linked record
            int totalSize = 0;
            for(int i=0;i<r.length;i++){
                int c =  i==r.length-1 ? 0: 8;
                totalSize+=  (int) (r[i]>>>48)-c;
            }
            byte[] b = new byte[totalSize];
            int pos = 0;
            for(int i=0;i<r.length;i++){
                int c =  i==r.length-1 ? 0: 8;
                int size = (int) (r[i]>>>48) -c;
                log.getDataInput((r[i] & LOG_MASK_OFFSET) + c, size).readFully(b,pos,size);
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
        final Lock lock  = locks[Utils.longHash(recid)&Utils.LOCK_MASK].writeLock();
        lock.lock();
        try{
            final long[] physPos;
            final long[] logPos;

            long indexVal = 0;
            long[] linkedRecords = getLinkedRecordsFromLog(ioRecid);
            if(linkedRecords==null){
                indexVal = index.getLong(ioRecid);
                linkedRecords = getLinkedRecordsIndexVals(indexVal);
            }

            structuralLock.lock();
            try{
                openLogIfNeeded();

                //free first record pointed from indexVal
                if(indexVal!=0)
                    freePhysPut(indexVal);

                //if there are more linked records, free those as well
                if(linkedRecords!=null){
                    for(int i=0; i<linkedRecords.length &&linkedRecords[i]!=0;i++){
                        freePhysPut(linkedRecords[i]);
                    }
                }


                //first get space in phys
                physPos = physAllocate(out.pos,false);
                //now get space in log
                logPos = logAllocate(physPos);

            }finally{
                structuralLock.unlock();
            }

            //write data into log
            walIndexVal((logPos[0]&LOG_MASK_OFFSET) - 1-8-8-1-8, ioRecid, physPos[0]|MASK_ARCHIVE);
            walPhysArray(out, physPos, logPos);

            modified.put(ioRecid,logPos);
        }finally{
            lock.unlock();
        }
        recycledDataOuts.offer(out);
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        final long ioRecid = IO_USER_START + recid*8;
        final Lock lock  = locks[Utils.longHash(recid)&Utils.LOCK_MASK].writeLock();
        lock.lock();
        DataOutput2 out;
        try{

            A oldVal = get2(ioRecid,serializer);
            if((oldVal == null && expectedOldValue!=null) || (oldVal!=null && !oldVal.equals(expectedOldValue)))
                return false;

            out = serialize(newValue, serializer);

            final long[] physPos;
            final long[] logPos;

            long indexVal = 0;
            long[] linkedRecords = getLinkedRecordsFromLog(ioRecid);
            if(linkedRecords==null){
                indexVal = index.getLong(ioRecid);
                linkedRecords = getLinkedRecordsIndexVals(indexVal);
            }

            structuralLock.lock();
            try{
                openLogIfNeeded();

                //free first record pointed from indexVal
                if(indexVal!=0)
                    freePhysPut(indexVal);

                //if there are more linked records, free those as well
                if(linkedRecords!=null){
                    for(int i=0; i<linkedRecords.length &&linkedRecords[i]!=0;i++){
                        freePhysPut(linkedRecords[i]);
                    }
                }


                //first get space in phys
                physPos = physAllocate(out.pos,false);
                //now get space in log
                logPos = logAllocate(physPos);

            }finally{
                structuralLock.unlock();
            }

            //write data into log
            walIndexVal((logPos[0]&LOG_MASK_OFFSET) - 1-8-8-1-8, ioRecid, physPos[0]|MASK_ARCHIVE);
            walPhysArray(out, physPos, logPos);

            modified.put(ioRecid,logPos);

        }catch(IOException e){
            throw new IOError(e);
        }finally{
            lock.unlock();
        }
        recycledDataOuts.offer(out);
        return true;
    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer) {
        final long ioRecid = IO_USER_START + recid*8;
        final Lock lock  = locks[Utils.longHash(recid)&Utils.LOCK_MASK].writeLock();
        lock.lock();
        try{
            final long logPos;

            long indexVal = 0;
            long[] linkedRecords = getLinkedRecordsFromLog(ioRecid);
            if(linkedRecords==null){
                indexVal = index.getLong(ioRecid);
                linkedRecords = getLinkedRecordsIndexVals(indexVal);
            }
            structuralLock.lock();
            try{
                openLogIfNeeded();
                logPos = logSize;
                checkLogRounding();
                logSize+=1+8+8; //space used for index val
                log.ensureAvailable(logSize);
                longStackPut(IO_FREE_RECID, ioRecid);

                //free first record pointed from indexVal
                if(indexVal!=0)
                    freePhysPut(indexVal);

                //if there are more linked records, free those as well
                if(linkedRecords!=null){
                    for(int i=0; i<linkedRecords.length &&linkedRecords[i]!=0;i++){
                        freePhysPut(linkedRecords[i]);
                    }
                }

            }finally {
                structuralLock.unlock();
            }
            walIndexVal(logPos,ioRecid,0|MASK_ARCHIVE);
            modified.put(ioRecid,TOMBSTONE);
        }finally {
            lock.unlock();
        }
        }

    @Override
    public void commit() {
        structuralLock.lock();
        for(ReentrantReadWriteLock lock:locks) lock.writeLock().lock();
        try{
            if(!longStackPages.isEmpty() && log==null) openLogIfNeeded();

            if(log==null){
                return; //no modifications
            }
            //update physical and logical filesize

            //dump long stack pages
            LongMap.LongMapIterator<long[]> iter = longStackPages.longMapIterator();
            while(iter.moveToNext()){
                long pageSize = iter.value()[0]>>>48;
                log.ensureAvailable(logSize+1+8+pageSize);
                log.putByte(logSize, WAL_PHYS_ARRAY);
                logSize+=1;
                log.putLong(logSize, (pageSize<<48)|iter.key());
                logSize+=8;
                //first long in array
                long[] array = iter.value();
                log.putLong(logSize,array[0]);
                logSize+=8;
                int numItems = (int) ((pageSize-8)/6);
                for(int i=0;i<numItems;i++){
                    log.putSixLong(logSize,array[i+1]);
                    logSize+=6;
                }
                checkLogRounding();
            }


            log.ensureAvailable(logSize + 17 + 17 + 17 + 1);
            walIndexVal(logSize,IO_PHYS_SIZE, physSize);
            logSize+=17;
            walIndexVal(logSize,IO_INDEX_SIZE, indexSize);
            logSize+=17;
            walIndexVal(logSize,IO_FREE_SIZE, freeSize);
            logSize+=17;

            for(int i=IO_FREE_RECID;i<IO_USER_START;i+=8){
                if(!indexValsModified[i/8]) continue;
                log.ensureAvailable(logSize + 17);
                walIndexVal(logSize, i,indexVals[i/8]);
                logSize+=17;
            }

            //seal log file
            log.putByte(logSize, WAL_SEAL);
            logSize+=1;
            //flush log file
            if(!syncOnCommitDisabled) log.sync();
            //and write mark it was sealed
            log.putLong(8, LOG_SEAL);
            if(!syncOnCommitDisabled) log.sync();

            replayLogFile();
            reloadIndexFile();

        }finally {
            for(ReentrantReadWriteLock lock:locks) lock.writeLock().unlock();
            structuralLock.unlock();
        }
    }

    protected void replayLogFile(){

        logSize = 0;

        if(log !=null && !syncOnCommitDisabled){
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
                final int size = (int) (offset>>>48);
                offset = offset&MASK_OFFSET;

                //transfer byte[] directly from log file without copying into memory
                DataInput2 input = log.getDataInput(logSize, size);
                ByteBuffer buf = input.buf.duplicate();

                buf.position(input.pos);
                buf.limit(input.pos+size);
                phys.ensureAvailable(offset+size);
                phys.putData(offset, buf);

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
        if(!syncOnCommitDisabled){
            phys.sync();
            index.sync();
        }
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
        for(ReentrantReadWriteLock lock:locks) lock.writeLock().lock();
        try{
            //discard trans log
            if(log !=null){
                log.close();
                log.deleteFile();
                log = null;
            }

            reloadIndexFile();
        }finally {
            for(ReentrantReadWriteLock lock:locks) lock.writeLock().unlock();
            structuralLock.unlock();
        }
    }

    private long[] getLongStackPage(final long physOffset, boolean read){
        long[] buf = longStackPages.get(physOffset);
        if(buf == null){
            buf = new long[LONG_STACK_PREF_COUNT+1];
            if(read){
                buf[0] = phys.getLong(physOffset);
                for(int i=1;i<buf.length;i++){
                    buf[i] = phys.getSixLong(physOffset + 2 + i * 6);
                }
            }
            longStackPages.put(physOffset,buf);
        }
        return buf;
    }


    @Override
    protected long longStackTake(long ioList) {
        final int ii = ((int) (ioList / 8));

        long dataOffset = indexVals[ii];
        if(dataOffset == 0) return 0; //empty

        long pos = dataOffset>>>48;
        dataOffset&=MASK_OFFSET;

        if(pos<8) throw new InternalError();

        long[] buf = getLongStackPage(dataOffset,true);

        final long ret = buf[((int) ((pos - 2) / 6))];

        //was it only record at that page?
        if(pos==8){
            //yes, delete this page
            long next = buf[0]&MASK_OFFSET;
            long size = buf[0]>>>48;


            if(next != 0){
                //update index so it points to previous page
                long nextSize = getLongStackPage(next,true)[0]>>>48;
                indexVals[ii] = ((nextSize-6)<<48)|next;
                indexValsModified[ii] = true;
            }else{
                indexVals[ii] = 0;
                indexValsModified[ii] = true;
            }

            //put space used by this page into free list
            longStackPages.remove(dataOffset); //TODO write zeroes to phys file

            freePhysPut((size<<48)|dataOffset);
        }else{
            //no, it was not last record at this page, so just decrement the counter
            pos-=6;
            indexVals[ii] = (pos<<48)|dataOffset;
            indexValsModified[ii] = true;
        }
        return ret;

    }

    @Override
    protected void longStackPut(long ioList, long offset) {
        if(offset>>>48!=0) throw new IllegalArgumentException();
        //index position was cleared, put into free index list

        final int ii = ((int) (ioList / 8));

        long dataOffset = indexVals[ii];
        long pos = dataOffset>>>48;
        dataOffset &= MASK_OFFSET;

        if(dataOffset == 0){ //empty list?
            //yes empty, create new page and fill it with values
            final long listPhysid = freePhysTake((int) LONG_STACK_PREF_SIZE,false) &MASK_OFFSET;
            long[] buf = getLongStackPage(listPhysid,false);
            if(listPhysid == 0) throw new InternalError();

            //set size and link to old page
            buf[0] = (LONG_STACK_PREF_SIZE<<48) | dataOffset;
            //set  record
            buf[1] = offset;
            //and update index file with new page location
            indexVals[ii] =  (8L << 48) | listPhysid;
            indexValsModified[ii] = true;
        }else{
            //non empty list
            long[] buf = getLongStackPage(dataOffset,true);
            final long next = buf[0]&MASK_OFFSET;
            final long size = buf[0]>>>48;
            final int numberOfRecordsInPage = (int) (buf[0]>>>(8*7));

            if(pos+6==size){ //is current page full?
                //yes it is full, so we need to allocate new page and write our number there
                final long listPhysid = freePhysTake((int) LONG_STACK_PREF_SIZE, false) &MASK_OFFSET;
                long[] bufNew = getLongStackPage(listPhysid,false);
                if(listPhysid == 0) throw new InternalError();

                //set location to previous page and set current page size
                bufNew[0]=(LONG_STACK_PREF_SIZE<<48)|dataOffset;

                //set the value itself
                bufNew[1] = offset;

                //and update index file with new page location and number of records
                indexVals[ii] =  (8L<<48) | listPhysid;
                indexValsModified[ii] = true;
            }else{
                //there is space on page, so just write released recid and increase the counter
                pos+=6;
                buf[((int) ((pos - 2) / 6))] = offset;
                indexVals[ii] = (pos<<48)|dataOffset;
                indexValsModified[ii] = true;

            }
        }

    }

    protected long[] getLinkedRecordsFromLog(long ioRecid){
        long[] ret0 = modified.get(ioRecid);
        if(ret0!=null){
            long[] ret = new long[ret0.length];
            for(int i=0;i<ret0.length;i++){
                long offset = ret0[i] & LOG_MASK_OFFSET;
                //offset now points to log file, read phys offset from log file
                ret[i] =  log.getLong(offset-8);
            }
            return ret;
        }
        return null;
    }

    @Override
    public void close() {
        structuralLock.lock();
        for(ReentrantReadWriteLock lock:locks) lock.writeLock().lock();
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
            for(ReentrantReadWriteLock lock:locks) lock.writeLock().unlock();
            structuralLock.unlock();
        }
    }

    @Override
    public void compact() {

        //TODO lock it down here
        if(log!=null && !log.isEmpty()) //TODO thread unsafe?
            throw new IllegalAccessError("WAL not empty; commit first, than compact");
        super.compact();
        reloadIndexFile();

    }

    @Override
    public boolean canRollback(){
        return true;
    }


}
