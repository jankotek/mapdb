package org.mapdb;

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.zip.CRC32;

/**
 * Write-Ahead-Log
 */
public class StoreWAL extends StoreDirect {

    protected static final long LOG_MASK_OFFSET = 0x0000FFFFFFFFFFFFL;

    protected static final byte WAL_INDEX_LONG = 101;
    protected static final byte WAL_LONGSTACK_PAGE = 102;
    protected static final byte WAL_PHYS_ARRAY_ONE_LONG = 103;

    protected static final byte WAL_PHYS_ARRAY = 104;
    protected static final byte WAL_SKIP_REST_OF_BLOCK = 105;


    /** last instruction in log file */
    protected static final byte WAL_SEAL = 111;
    /** added to offset 8 into log file, indicates that log was synced and closed*/
    protected static final long LOG_SEAL = 4566556446554645L;

    public static final String TRANS_LOG_FILE_EXT = ".t";

    protected static final long[] TOMBSTONE = new long[0];
    protected static final long[] PREALLOC = new long[0];

    protected final Volume.Factory volFac;
    protected Volume log;

    protected volatile long logSize;

    protected final LongConcurrentHashMap<long[]> modified = new LongConcurrentHashMap<long[]>();
    protected final LongMap<long[]> longStackPages = new LongHashMap<long[]>();
    protected final long[] indexVals = new long[IO_USER_START/8];
    protected final boolean[] indexValsModified = new boolean[indexVals.length];

    protected boolean replayPending = true;


    protected final AtomicInteger logChecksum = new AtomicInteger();

    public StoreWAL(Volume.Factory volFac) {
        this(volFac, false, false, 5, false, 0L, false, false, null, false);
    }
    public StoreWAL(Volume.Factory volFac, boolean readOnly, boolean deleteFilesAfterClose,
                    int spaceReclaimMode, boolean syncOnCommitDisabled, long sizeLimit,
                    boolean checksum, boolean compress, byte[] password, boolean fullChunkAllocation) {
        super(volFac, readOnly, deleteFilesAfterClose, spaceReclaimMode, syncOnCommitDisabled, sizeLimit,
                checksum, compress, password, fullChunkAllocation);
        this.volFac = volFac;
        this.log = volFac.createTransLogVolume();

        structuralLock.lock();
        try{
            reloadIndexFile();
            if(verifyLogFile()){
                replayLogFile();
            }
            replayPending = false;
            checkHeaders();
            log = null;
        }catch(IOError e){
            close();
            throw new IOError(e);
        }finally {
            structuralLock.unlock();
        }
    }

    @Override
    protected void checkHeaders() {
        if(replayPending) return;
        super.checkHeaders();
    }

    protected void reloadIndexFile() {
        assert(structuralLock.isHeldByCurrentThread());
        logSize = 0;
        modified.clear();
        longStackPages.clear();
        indexSize = index.getLong(IO_INDEX_SIZE);
        physSize = index.getLong(IO_PHYS_SIZE);
        freeSize = index.getLong(IO_FREE_SIZE);
        for(int i = 0;i<IO_USER_START;i+=8){
            indexVals[i/8] = index.getLong(i);
        }
        Arrays.fill(indexValsModified, false);

        logChecksum.set(0);

        maxUsedIoList=IO_USER_START-8;
        while(indexVals[((int) (maxUsedIoList / 8))]!=0 && maxUsedIoList>IO_FREE_RECID)
            maxUsedIoList-=8;
    }

    protected void openLogIfNeeded(){
        assert(structuralLock.isHeldByCurrentThread());
        if(log !=null) return;
        log = volFac.createTransLogVolume();
        log.ensureAvailable(16);
        log.putLong(0, HEADER);
        log.putLong(8, 0L);
        logSize = 16;
    }


    @Override
    public  long preallocate() {
        final long ioRecid;
        final long logPos;

        newRecidLock.readLock().lock();
        try{
            structuralLock.lock();
            try{
                openLogIfNeeded();
                ioRecid = freeIoRecidTake(false);
                //now get space in log
                openLogIfNeeded();
                logPos = logSize;
                logSize+=1+8+8; //space used for index val
                log.ensureAvailable(logSize);

            }finally{
                structuralLock.unlock();
            }
            final Lock lock  = locks[Utils.lockPos(ioRecid)].writeLock();
            lock.lock();
            try{

                //write data into log
                walIndexVal(logPos, ioRecid, MASK_DISCARD);
                modified.put(ioRecid, PREALLOC);
            }finally{
                lock.unlock();
            }
        }finally{
            newRecidLock.readLock().unlock();
        }

        long recid =  (ioRecid-IO_USER_START)/8;
        assert(recid>0);
        return recid;
    }


    @Override
    public void preallocate(final long[] recids) {
        long logPos;

        newRecidLock.readLock().lock();
        try{

            structuralLock.lock();
            try{
                openLogIfNeeded();
                logPos = logSize;
                for(int i=0;i<recids.length;i++)
                    recids[i] = freeIoRecidTake(false) ;

                //now get space in log
                openLogIfNeeded();
                logSize+=recids.length*(1+8+8); //space used for index vals
                log.ensureAvailable(logSize);

            }finally{
                structuralLock.unlock();
            }
            //write data into log
            for(int i=0;i<recids.length;i++){
                final long ioRecid = recids[i];
                final Lock lock2 = locks[Utils.lockPos(ioRecid)].writeLock();
                lock2.lock();
                try{
                    walIndexVal(logPos, ioRecid, MASK_DISCARD);
                    logPos+=1+8+8;
                    modified.put(ioRecid, PREALLOC);
                }finally{
                    lock2.unlock();
                }
                recids[i] =  (ioRecid-IO_USER_START)/8;
                assert(recids[i]>0);
            }
        }finally{
            newRecidLock.readLock().unlock();
        }
    }


    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        assert(value!=null);
        DataOutput2 out = serialize(value, serializer);

        final long ioRecid;
        final long[] physPos;
        final long[] logPos;

        newRecidLock.readLock().lock();
        try{

            structuralLock.lock();
        try{
            openLogIfNeeded();
            ioRecid = freeIoRecidTake(false);
            //first get space in phys
            physPos = physAllocate(out.pos,false,false);
            //now get space in log
            logPos = logAllocate(physPos);

        }finally{
            structuralLock.unlock();
        }

        final Lock lock  = locks[Utils.lockPos(ioRecid)].writeLock();
        lock.lock();
        try{
            //write data into log
            walIndexVal((logPos[0]&LOG_MASK_OFFSET) - 1-8-8-1-8, ioRecid, physPos[0]|MASK_ARCHIVE);
            walPhysArray(out, physPos, logPos);

            modified.put(ioRecid,logPos);
            recycledDataOuts.offer(out);
        }finally{
            lock.unlock();
        }
        }finally{
            newRecidLock.readLock().unlock();
        }

        long recid =  (ioRecid-IO_USER_START)/8;
        assert(recid>0);
        return recid;
    }

    protected void walPhysArray(DataOutput2 out, long[] physPos, long[] logPos) {
        //write byte[] data
        int outPos = 0;
        int logC = 0;
        CRC32 crc32  = new CRC32();

        for(int i=0;i<logPos.length;i++){
            int c =  i==logPos.length-1 ? 0: 8;
            final long pos = logPos[i]&LOG_MASK_OFFSET;
            int size = (int) (logPos[i]>>>48);

            byte header = c==0 ? WAL_PHYS_ARRAY : WAL_PHYS_ARRAY_ONE_LONG;
            log.putByte(pos -  8 - 1, header);
            log.putLong(pos -  8, physPos[i]);

            if(c>0){
                log.putLong(pos, physPos[i + 1]);
            }
            log.putData(pos+c, out.buf, outPos, size - c);

            crc32.reset();
            crc32.update(out.buf,outPos, size-c);
            logC |= Utils.longHash( pos | header | physPos[i] | (c>0?physPos[i+1]:0) | crc32.getValue());

            outPos +=size-c;
            assert(logSize>=outPos);
        }
        logChecksumAdd(logC);
        assert(outPos==out.pos);
    }


    protected void walIndexVal(long logPos, long ioRecid, long indexVal) {
        assert(locks[Utils.lockPos(ioRecid)].writeLock().isHeldByCurrentThread());
        assert(logSize>=logPos+1+8+8);
        log.putByte(logPos, WAL_INDEX_LONG);
        log.putLong(logPos + 1, ioRecid);
        log.putLong(logPos + 9, indexVal);

        logChecksumAdd(Utils.longHash(logPos | WAL_INDEX_LONG | ioRecid | indexVal));
    }


    protected long[] logAllocate(long[] physPos) {
        assert(structuralLock.isHeldByCurrentThread());
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
        assert(structuralLock.isHeldByCurrentThread());
        if((logSize&Volume.CHUNK_SIZE_MOD_MASK)+MAX_REC_SIZE*2>Volume.CHUNK_SIZE){
            log.ensureAvailable(logSize+1);
            log.putByte(logSize, WAL_SKIP_REST_OF_BLOCK);
            logSize += Volume.CHUNK_SIZE - (logSize&Volume.CHUNK_SIZE_MOD_MASK);
        }
    }


    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        assert(recid>0);
        final long ioRecid = IO_USER_START + recid*8;
        final Lock lock  = locks[Utils.lockPos(ioRecid)].readLock();
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
        assert(locks[Utils.lockPos(ioRecid)].getWriteHoldCount()==0||
                locks[Utils.lockPos(ioRecid)].writeLock().isHeldByCurrentThread());

        //check if record was modified in current transaction
        long[] r = modified.get(ioRecid);
        //no, read main version
        if(r==null) return super.get2(ioRecid, serializer);
        //check for tombstone (was deleted in current trans)
        if(r==TOMBSTONE || r==PREALLOC || r.length==0) return null;

        //was modified in current transaction, so read it from trans log
        if(r.length==1){
            //single record
            final int size = (int) (r[0]>>>48);
            DataInput2 in = log.getDataInput(r[0]&LOG_MASK_OFFSET, size);
            return deserialize(serializer,size,in);
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

            return deserialize(serializer,totalSize, new DataInput2(b));
        }
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        assert(recid>0);
        assert(value!=null);
        DataOutput2 out = serialize(value, serializer);
        final long ioRecid = IO_USER_START + recid*8;
        final Lock lock  = locks[Utils.lockPos(ioRecid)].writeLock();
        lock.lock();
        try{
            final long[] physPos;
            final long[] logPos;

            long indexVal = 0;
            long[] linkedRecords = getLinkedRecordsFromLog(ioRecid);
            if(linkedRecords==null){
                indexVal = index.getLong(ioRecid);
                linkedRecords = getLinkedRecordsIndexVals(indexVal);
            }else if(linkedRecords == PREALLOC){
                linkedRecords = null;
            }

            structuralLock.lock();
            try{
                openLogIfNeeded();

                //free first record pointed from indexVal
                if((indexVal>>>48)>0)
                    freePhysPut(indexVal,false);

                //if there are more linked records, free those as well
                if(linkedRecords!=null){
                    for(int i=0; i<linkedRecords.length &&linkedRecords[i]!=0;i++){
                        freePhysPut(linkedRecords[i],false);
                    }
                }


                //first get space in phys
                physPos = physAllocate(out.pos,false,false);
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
        assert(recid>0);
        assert(expectedOldValue!=null && newValue!=null);
        final long ioRecid = IO_USER_START + recid*8;
        final Lock lock  = locks[Utils.lockPos(ioRecid)].writeLock();
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
                if((indexVal>>>48)>0)
                    freePhysPut(indexVal,false);

                //if there are more linked records, free those as well
                if(linkedRecords!=null){
                    for(int i=0; i<linkedRecords.length &&linkedRecords[i]!=0;i++){
                        freePhysPut(linkedRecords[i],false);
                    }
                }


                //first get space in phys
                physPos = physAllocate(out.pos,false,false);
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
        assert(recid>0);
        final long ioRecid = IO_USER_START + recid*8;
        final Lock lock  = locks[Utils.lockPos(ioRecid)].writeLock();
        lock.lock();
        try{
            final long logPos;

            long indexVal = 0;
            long[] linkedRecords = getLinkedRecordsFromLog(ioRecid);
            if(linkedRecords==null){
                indexVal = index.getLong(ioRecid);
                if(indexVal==MASK_DISCARD) return;
                linkedRecords = getLinkedRecordsIndexVals(indexVal);
            }
            structuralLock.lock();
            try{
                openLogIfNeeded();
                logPos = logSize;
                checkLogRounding();
                logSize+=1+8+8; //space used for index val
                log.ensureAvailable(logSize);
                longStackPut(IO_FREE_RECID, ioRecid,false);

                //free first record pointed from indexVal
                if((indexVal>>>48)>0)
                    freePhysPut(indexVal,false);

                //if there are more linked records, free those as well
                if(linkedRecords!=null){
                    for(int i=0; i<linkedRecords.length &&linkedRecords[i]!=0;i++){
                        freePhysPut(linkedRecords[i],false);
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
        lockAllWrite();
        try{
            if(serializerPojo!=null && serializerPojo.hasUnsavedChanges()){
                serializerPojo.save(this);
            }

            if(!longStackPages.isEmpty() && log==null) openLogIfNeeded();

            if(log==null){
                return; //no modifications
            }
            //dump long stack pages
            int crc = 0;
            LongMap.LongMapIterator<long[]> iter = longStackPages.longMapIterator();
            while(iter.moveToNext()){
                long pageSize = iter.value()[0]>>>48;
                long firstVal = (pageSize<<48)|iter.key();
                long[] array = iter.value();
                log.ensureAvailable(logSize+1+8+pageSize);

                crc |= Utils.longHash(logSize|WAL_LONGSTACK_PAGE|firstVal|array[0]);

                log.putByte(logSize, WAL_LONGSTACK_PAGE);
                logSize+=1;
                log.putLong(logSize, firstVal);
                logSize+=8;
                //first long in array

                log.putLong(logSize,array[0]);
                logSize+=8;
                int numItems = (int) ((pageSize-8)/6);
                for(int i=1;i<=numItems;i++){
                    crc|=Utils.longHash(logSize|array[i]);
                    log.putSixLong(logSize,array[i]);
                    logSize+=6;
                }
                checkLogRounding();
            }


            for(int i=IO_FREE_RECID;i<IO_USER_START;i+=8){
                if(!indexValsModified[i/8]) continue;
                log.ensureAvailable(logSize + 17);
                logSize+=17;
                walIndexVal(logSize-17, i,indexVals[i/8]);
                //no need to update crc, since IndexVal already does it
            }

            //seal log file
            log.ensureAvailable(logSize + 1 + 3*6 + 8+4);
            long indexChecksum = indexHeaderChecksumUncommited();
            crc|=Utils.longHash(logSize|WAL_SEAL|indexSize|physSize|freeSize|indexChecksum);
            log.putByte(logSize, WAL_SEAL);
            logSize+=1;
            log.putSixLong(logSize, indexSize);
            logSize+=6;
            log.putSixLong(logSize,physSize);
            logSize+=6;
            log.putSixLong(logSize,freeSize);
            logSize+=6;
            log.putLong(logSize, indexChecksum);
            logSize+=8;
            log.putInt(logSize, crc|logChecksum.get());
            logSize+=4;

            //write mark it was sealed
            log.putLong(8, LOG_SEAL);

            //and flush log file
            if(!syncOnCommitDisabled) log.sync();

            replayLogFile();
            reloadIndexFile();
        }finally {
             unlockAllWrite();
        }

    }

    protected long indexHeaderChecksumUncommited() {
        long ret = 0;

        for(int offset = 0;offset<IO_USER_START;offset+=8){
            if(offset == IO_INDEX_SUM) continue;
            long indexVal;

            if(offset==IO_INDEX_SIZE){
                indexVal = indexSize;
            }else if(offset==IO_PHYS_SIZE){
                indexVal = physSize;
            }else if(offset==IO_FREE_SIZE){
                indexVal = freeSize;
            }else
                indexVal = indexVals[offset / 8];

            ret |=  indexVal | Utils.longHash(indexVal|offset) ;
        }

        return ret;
    }

    protected boolean verifyLogFile() {
        assert(structuralLock.isHeldByCurrentThread());

        if(readOnly && log==null)
            return false;

        logSize = 0;



        //read headers
        if(log.isEmpty() || log.getLong(0)!=HEADER || log.getLong(8) !=LOG_SEAL){
            //wrong headers, discard log
            log.close();
            log.deleteFile();
            log = null;
            return false;
        }

        final CRC32 crc32 = new CRC32();

        //all good, calculate checksum
        logSize=16;
        byte ins = log.getByte(logSize);
        logSize+=1;
        int crc = 0;

        while(ins!=WAL_SEAL){
            if(ins == WAL_INDEX_LONG){
                long ioRecid = log.getLong(logSize);
                logSize+=8;
                long indexVal = log.getLong(logSize);
                logSize+=8;
                crc |= Utils.longHash((logSize-1-8-8) | WAL_INDEX_LONG | ioRecid | indexVal);
            }else if(ins == WAL_PHYS_ARRAY){
                final long offset2 = log.getLong(logSize);
                logSize+=8;
                final int size = (int) (offset2>>>48);

                byte[] b = new byte[size];
                try{
                    log.getDataInput(logSize, size).readFully(b);
                } catch (IOException e) {
                    throw new IOError(e);
                }
                crc32.reset();
                crc32.update(b);

                crc |= Utils.longHash(logSize | WAL_PHYS_ARRAY | offset2 | crc32.getValue());

                logSize+=size;
            }else if(ins == WAL_PHYS_ARRAY_ONE_LONG){
                final long offset2 = log.getLong(logSize);
                logSize+=8;
                final int size = (int) (offset2>>>48)-8;

                final long nextPageLink = log.getLong(logSize);
                logSize+=8;

                byte[] b = new byte[size];
                try {
                    log.getDataInput(logSize, size).readFully(b);
                } catch (IOException e) {
                    throw new IOError(e);
                }
                crc32.reset();
                crc32.update(b);

                crc |= Utils.longHash((logSize) | WAL_PHYS_ARRAY_ONE_LONG | offset2 | nextPageLink | crc32.getValue());

                logSize+=size;
            }else if(ins == WAL_LONGSTACK_PAGE){
                long offset = log.getLong(logSize);
                logSize+=8;
                final long origLogSize = logSize;
                final int size = (int) (offset>>>48);
                final long nextPageLink = log.getLong(logSize);
                logSize+=8;
                crc |= Utils.longHash(origLogSize | WAL_LONGSTACK_PAGE | offset | nextPageLink );
                for(;logSize<origLogSize+size;logSize+=6){
                    crc |= Utils.longHash(logSize|log.getSixLong(logSize));
                }

            }else if(ins == WAL_SKIP_REST_OF_BLOCK){
                logSize += Volume.CHUNK_SIZE -(logSize&Volume.CHUNK_SIZE_MOD_MASK);
            }else{
                throw new InternalError("unknown trans log instruction '"+ins +"' at log offset: "+(logSize-1));
            }

            ins = log.getByte(logSize);
            logSize+=1;
        }
        long indexSize = log.getSixLong(logSize);
        logSize+=6;
        long physSize = log.getSixLong(logSize);
        logSize+=6;
        long freeSize = log.getSixLong(logSize);
        logSize+=6;
        long indexSum = log.getLong(logSize);
        logSize+=8;
        crc |= Utils.longHash((logSize-1-3*6-8)|indexSize|physSize|freeSize|indexSum);

        final int realCrc = log.getInt(logSize);
        logSize+=4;

        logSize=0;
        assert(structuralLock.isHeldByCurrentThread());
        return realCrc == crc;

    }



    protected void replayLogFile(){
        assert(structuralLock.isHeldByCurrentThread());

        if(readOnly && log==null)
            return;

        logSize = 0;


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
            }else if(ins == WAL_PHYS_ARRAY||ins == WAL_LONGSTACK_PAGE || ins == WAL_PHYS_ARRAY_ONE_LONG){
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
                logSize += Volume.CHUNK_SIZE -(logSize&Volume.CHUNK_SIZE_MOD_MASK);
            }else{
                throw new InternalError("unknown trans log instruction '"+ins +"' at log offset: "+(logSize-1));
            }

            ins = log.getByte(logSize);
            logSize+=1;
        }
        index.putLong(IO_INDEX_SIZE,log.getSixLong(logSize));
        logSize+=6;
        index.putLong(IO_PHYS_SIZE,log.getSixLong(logSize));
        logSize+=6;
        index.putLong(IO_FREE_SIZE,log.getSixLong(logSize));
        logSize+=6;
        index.putLong(IO_INDEX_SUM,log.getLong(logSize));
        logSize+=8;


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
        assert(structuralLock.isHeldByCurrentThread());
    }



    @Override
    public void rollback() throws UnsupportedOperationException {
        lockAllWrite();
        try{
            //discard trans log
            if(log !=null){
                log.close();
                log.deleteFile();
                log = null;
            }

            reloadIndexFile();
        }finally {
            unlockAllWrite();
        }
    }

    private long[] getLongStackPage(final long physOffset, boolean read){
        assert(structuralLock.isHeldByCurrentThread());
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
    protected long longStackTake(long ioList, boolean recursive) {
        assert(structuralLock.isHeldByCurrentThread());
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
                if(maxUsedIoList==ioList){
                    //max value was just deleted, so find new maxima
                    while(indexVals[((int) (maxUsedIoList / 8))]==0 && maxUsedIoList>IO_FREE_RECID){
                        maxUsedIoList-=8;
                    }
                }
            }

            //put space used by this page into free list
            longStackPages.remove(dataOffset); //TODO write zeroes to phys file

            freePhysPut((size<<48)|dataOffset,true);
        }else{
            //no, it was not last record at this page, so just decrement the counter
            pos-=6;
            indexVals[ii] = (pos<<48)|dataOffset;
            indexValsModified[ii] = true;
        }
        return ret;

    }

    @Override
    protected void longStackPut(long ioList, long offset,boolean recursive) {
        assert(structuralLock.isHeldByCurrentThread());
        assert(offset>>>48==0);
        assert(ioList>=IO_FREE_RECID && ioList<=IO_USER_START): "wrong ioList: "+ioList;

//        if(recursive) throw new InternalError();
        if(offset>>>48!=0) throw new IllegalArgumentException();
        //index position was cleared, put into free index list

        final int ii = ((int) (ioList / 8));

        long dataOffset = indexVals[ii];
        long pos = dataOffset>>>48;
        dataOffset &= MASK_OFFSET;

        if(dataOffset == 0){ //empty list?
            //yes empty, create new page and fill it with values
            final long listPhysid = freePhysTake((int) LONG_STACK_PREF_SIZE,false,true) &MASK_OFFSET;
            long[] buf = getLongStackPage(listPhysid,false);
            if(listPhysid == 0) throw new InternalError();

            //set size and link to old page
            buf[0] = (LONG_STACK_PREF_SIZE<<48) | dataOffset;
            //set  record
            buf[1] = offset;
            //and update index file with new page location
            indexVals[ii] =  (8L << 48) | listPhysid;
            indexValsModified[ii] = true;
            if(maxUsedIoList<=ioList) maxUsedIoList=ioList;
        }else{
            //non empty list
            long[] buf = getLongStackPage(dataOffset,true);
            final long next = buf[0]&MASK_OFFSET;
            final long size = buf[0]>>>48;
            final int numberOfRecordsInPage = (int) (buf[0]>>>(8*7));

            if(pos+6==size){ //is current page full?
                //yes it is full, so we need to allocate new page and write our number there
                final long listPhysid = freePhysTake((int) LONG_STACK_PREF_SIZE, false,true) &MASK_OFFSET;
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
        assert(structuralLock.isHeldByCurrentThread());
    }

    protected long[] getLinkedRecordsFromLog(long ioRecid){
        assert(locks[Utils.lockPos(ioRecid)].writeLock().isHeldByCurrentThread());
        long[] ret0 = modified.get(ioRecid);
        if(ret0==PREALLOC) return ret0;

        if(ret0!=null && ret0!=TOMBSTONE){
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
        if(serializerPojo!=null && serializerPojo.hasUnsavedChanges()){
            serializerPojo.save(this);
        }

        lockAllWrite();
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
            unlockAllWrite();
        }
    }

    @Override protected void compactPreUnderLock() {
        assert(structuralLock.isLocked());
        if(log!=null && !log.isEmpty())
            throw new IllegalAccessError("WAL not empty; commit first, than compact");
    }

    @Override protected void compactPostUnderLock() {
        assert(structuralLock.isLocked());
        reloadIndexFile();
    }


    @Override
    public boolean canRollback(){
        return true;
    }

    protected void logChecksumAdd(int cs) {
        for(;;){
            int old = logChecksum.get();
            if(logChecksum.compareAndSet(old,old|cs))
                return;
        }
    }



}
