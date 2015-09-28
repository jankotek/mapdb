package org.mapdb20;

import java.io.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.logging.Level;

import static org.mapdb20.DataIO.*;

public class StoreDirect extends Store {

    /** 2 byte store version*/
    protected static final int STORE_VERSION = 100;

    /** 4 byte file header */
    protected static final int HEADER = (0xA9DB<<16) | STORE_VERSION;


    protected static final long PAGE_SIZE = 1<< CC.VOLUME_PAGE_SHIFT;
    protected static final long PAGE_MASK = PAGE_SIZE-1;
    protected static final long PAGE_MASK_INVERSE = 0xFFFFFFFFFFFFFFFFL<<CC.VOLUME_PAGE_SHIFT;


    protected static final long MOFFSET = 0x0000FFFFFFFFFFF0L;

    protected static final long MLINKED = 0x8L;
    protected static final long MUNUSED = 0x4L;
    protected static final long MARCHIVE = 0x2L;
    protected static final long MPARITY = 0x1L;


    protected static final long STORE_SIZE = 8*2;
    /** physical offset of maximal allocated recid. Parity1.
     * It is value of last allocated RECID multiplied by recid size.
     * Use {@code val/INDEX_VAL_SIZE} to get actual RECID*/
    protected static final long MAX_RECID_OFFSET = 8*3;
    protected static final long LAST_PHYS_ALLOCATED_DATA_OFFSET = 8*4; //TODO update doc
    protected static final long FREE_RECID_STACK = 8*5;

    /*following slots might be used in future */
    protected static final long UNUSED1 = 8*6;
    protected static final long UNUSED2 = 8*7;
    protected static final long UNUSED3 = 8*8;
    protected static final long UNUSED4 = 8*9;
    protected static final long UNUSED5 = 8*10;


    protected static final int MAX_REC_SIZE = 0xFFFF;
    /** number of free physical slots */
    protected static final int SLOTS_COUNT = 2+(MAX_REC_SIZE)/16; //it rounds down, plus extra slot for zeros (not really used)

    protected static final long HEAD_END = UNUSED5 + SLOTS_COUNT * 8;
//            8*RECID_LAST_RESERVED;// also include reserved recids into mix;

    protected static final long INITCRC_INDEX_PAGE = 4329042389490239043L;

    protected static final long[] EMPTY_LONGS = new long[0];


    //TODO this refs are swapped during compaction. Investigate performance implications
    protected volatile Volume vol;
    protected volatile Volume headVol;

    //TODO this only grows under structural lock, but reads are outside structural lock, does it have to be volatile?
    protected volatile long[] indexPages;

    protected final ScheduledExecutorService executor;

    protected final List<Snapshot> snapshots;

    protected static final long INDEX_VAL_SIZE = 8;

    protected final long startSize;
    protected final long sizeIncrement;
    protected final boolean recidReuseDisable;
    protected final int sliceShift;

    protected final AtomicLong freeSize = new AtomicLong(-1);

    public StoreDirect(String fileName,
                       Volume.VolumeFactory volumeFactory,
                       Cache cache,
                       int lockScale,
                       int lockingStrategy,
                       boolean checksum,
                       boolean compress,
                       byte[] password,
                       boolean readonly,
                       boolean snapshotEnable,
                       boolean fileLockDisable,
                       DataIO.HeartbeatFileLock fileLockHeartbeat,
                       ScheduledExecutorService executor,
                       long startSize,
                       long sizeIncrement,
                       boolean recidReuseDisable
                       ) {
        super(fileName, volumeFactory, cache, lockScale, lockingStrategy, checksum, compress, password, readonly,
                snapshotEnable, fileLockDisable, fileLockHeartbeat);
        this.executor = executor;
        this.snapshots = snapshotEnable?
                new CopyOnWriteArrayList<Snapshot>():
                null;

        this.sizeIncrement = Math.max(1L<<CC.VOLUME_PAGE_SHIFT, DataIO.nextPowTwo(sizeIncrement));
        this.startSize = Fun.roundUp(Math.max(1L<<CC.VOLUME_PAGE_SHIFT,startSize), this.sizeIncrement);
        this.recidReuseDisable = recidReuseDisable;
        this.sliceShift = Volume.sliceShiftFromSize(this.sizeIncrement);

        if(CC.LOG_STORE && LOG.isLoggable(Level.FINE)){
            LOG.log(Level.FINE, "StoreDirect constructed: executor={0}, snapshots={1},  " +
                            "startSize={3}, sizeIncrement={4}",
                    new Object[]{executor,snapshots, startSize,sizeIncrement});
        }
    }

    @Override
    public void init() {
        commitLock.lock();
        try {
            structuralLock.lock();
            try {
                boolean empty = Volume.isEmptyFile(fileName);

                this.vol = volumeFactory.makeVolume(fileName, readonly, fileLockDisable, sliceShift, startSize, false);

                if (empty) {
                    initCreate();
                } else {
                    initOpen();
                }
            } finally {
                structuralLock.unlock();
            }
        }catch(RuntimeException e){
            initFailedCloseFiles();
            if(vol!=null && !vol.isClosed()) {
                vol.close();
            }
            vol = null;
            throw e;
        }finally {
            commitLock.unlock();
        }
    }

    protected void initFailedCloseFiles() {

    }


    protected void storeSizeSet(long storeSize) {
        if(CC.ASSERT && storeSize<PAGE_SIZE)
            throw new AssertionError();
        if(CC.ASSERT && storeSize%PAGE_SIZE!=0)
            throw new AssertionError();

        headVol.putLong(STORE_SIZE, parity16Set(storeSize));
    }

    protected long storeSizeGet(){
        return parity16Get(headVol.getLong(STORE_SIZE));
    }


    protected void initOpen() {
        if(CC.ASSERT && !commitLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        int header = vol.getInt(0);
        if(header!=header){
            throw new DBException.WrongConfig("This is not MapDB file");
        }

        if (CC.LOG_STORE && LOG.isLoggable(Level.FINE)) {
            LOG.log(Level.FINE, "initOpen: file={0}, volLength={1}, vol={2}", new Object[]{fileName, vol.length(), vol});
        }

        if(vol.getInt(0)!=HEADER){
            //TODO handle version numbers
            throw new DBException.DataCorruption("wrong header in file: "+fileName);
        }

        //check header config
        checkFeaturesBitmap(vol.getLong(HEAD_FEATURES));


        initHeadVol();
        //check head checksum
        int expectedChecksum = vol.getInt(HEAD_CHECKSUM);
        int actualChecksum = headChecksum(vol);
        if (actualChecksum != expectedChecksum) {
            throw new DBException.HeadChecksumBroken();
        }


        //load index pages
        long[] ip = new long[]{0};
        long indexPage = parity16Get(vol.getLong(HEAD_END));
        int i=1;
        for(;indexPage!=0;i++){
            if(CC.ASSERT && indexPage%PAGE_SIZE!=0)
                throw new DBException.DataCorruption();
            if(ip.length==i){
                ip = Arrays.copyOf(ip, ip.length * 4);
            }
            ip[i] = indexPage;

            //move to next page
            indexPage = parity16Get(vol.getLong(indexPage));
        }
        indexPages = Arrays.copyOf(ip, i);

        if (CC.LOG_STORE && LOG.isLoggable(Level.FINEST)) {
            LOG.log(Level.FINEST, "indexPages: {0}", Arrays.toString(indexPages));
        }
    }

    protected void initCreate() {
        if(CC.ASSERT && !commitLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        //create initial structure

        //set features bitmap
        final long features = makeFeaturesBitmap();

        if (CC.LOG_STORE && LOG.isLoggable(Level.FINE)) {
            LOG.log(Level.FINE, "initCreate: file={0}, volLength={1}, vol={2}, features={3}",
                    new Object[]{fileName, vol.length(), vol, Long.toBinaryString(features)});
        }
        //create new store
        indexPages = new long[]{0};

        vol.ensureAvailable(PAGE_SIZE);
        vol.clear(0, PAGE_SIZE);

        //set sizes
        vol.putLong(STORE_SIZE, parity16Set(PAGE_SIZE));
        vol.putLong(MAX_RECID_OFFSET, parity1Set(RECID_LAST_RESERVED * INDEX_VAL_SIZE));
        //pointer to next index page (zero)
        vol.putLong(HEAD_END, parity16Set(0));

        vol.putLong(LAST_PHYS_ALLOCATED_DATA_OFFSET, parity3Set(0));

        //put reserved recids
        for(long recid=1;recid<RECID_FIRST;recid++){
            long indexVal = parity1Set(MLINKED | MARCHIVE);
            long indexOffset = recidToOffset(recid);
            vol.putLong(indexOffset, indexVal);
        }

        //put long stack master links
        for(long masterLinkOffset = FREE_RECID_STACK;masterLinkOffset<HEAD_END;masterLinkOffset+=8){
            vol.putLong(masterLinkOffset,parity4Set(0));
        }

        //write header
        vol.putInt(0,HEADER);

        vol.putLong(HEAD_FEATURES, features);


        //and set header checksum
        vol.putInt(HEAD_CHECKSUM, headChecksum(vol));
        vol.sync();
        initHeadVol();
    }


    protected void initHeadVol() {
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        this.headVol = vol;
    }

    public StoreDirect(String fileName) {
        this(fileName,
                fileName==null? CC.DEFAULT_MEMORY_VOLUME_FACTORY : CC.DEFAULT_FILE_VOLUME_FACTORY,
                null,
                CC.DEFAULT_LOCK_SCALE,
                0,
                false,false,null,false,false,false,null,
                null, 0L, 0L, false);
    }

    protected int headChecksum(Volume vol2) {
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        int ret = 0;
        for(int offset = 8;
            offset< HEAD_END;
            offset+=8){
            long val = vol2.getLong(offset);
            ret += DataIO.longHash(offset + val);
        }

        if (CC.LOG_STORE && LOG.isLoggable(Level.FINEST)) {
            LOG.log(Level.FINEST, "headChecksum={0}", Integer.toHexString(ret));
        }

        return ret;
    }

    @Override
    protected <A> A get2(long recid, Serializer<A> serializer) {
        if (CC.ASSERT)
            assertReadLocked(recid);

        long[] offsets = offsetsGet(lockPos(recid),indexValGet(recid));
        return getFromOffset(serializer, offsets);
    }

    protected <A> A getFromOffset(Serializer<A> serializer, long[] offsets) {
        if (CC.LOG_STORE && LOG.isLoggable(Level.FINEST)) {
            LOG.log(Level.FINEST, "serializer={0}, offsets={1}",new Object[]{serializer, Arrays.toString(offsets)});
        }
        if (offsets == null) {
            return null; //zero size
        }else if (offsets.length==0){
            return deserialize(serializer,0,new DataInputByteArray(new byte[0]));
        }else if (offsets.length == 1) {
            //not linked
            int size = (int) (offsets[0] >>> 48);
            long offset = offsets[0] & MOFFSET;
            DataInput in = vol.getDataInput(offset, size);
            return deserialize(serializer, size, in);
        } else {
            //calculate total size
            int totalSize = offsetsTotalSize(offsets);
            byte[] b = getLoadLinkedRecord(offsets, totalSize);

            DataInput in = new DataInputByteArray(b);
            return deserialize(serializer, totalSize, in);
        }
    }

    private byte[] getLoadLinkedRecord(long[] offsets, int totalSize) {
        if (CC.LOG_STORE && LOG.isLoggable(Level.FINEST)) {
            LOG.log(Level.FINEST, "totalSize={0}, offsets={1}", new Object[]{totalSize, Arrays.toString(offsets)});
        }
        //load data
        byte[] b = new byte[totalSize];
        int bpos = 0;
        for (int i = 0; i < offsets.length; i++) {
            int plus = (i == offsets.length - 1)?0:8;
            long size = (offsets[i] >>> 48) - plus;
            if(CC.ASSERT && (size&0xFFFF)!=size)
                throw new DBException.DataCorruption("size mismatch");
            long offset = offsets[i] & MOFFSET;
            //System.out.println("GET "+(offset + plus)+ " - "+size+" - "+bpos);
            vol.getData(offset + plus, b, bpos, (int) size);
            bpos += size;
        }
        if (CC.ASSERT && bpos != totalSize)
            throw new DBException.DataCorruption("size does not match");
        return b;
    }

    protected int offsetsTotalSize(long[] offsets) {
        if(offsets==null || offsets.length==0)
            return 0;
        int totalSize = 8;
        for (long l : offsets) {
            totalSize += (l >>> 48) - 8;
        }
        return totalSize;
    }


    @Override
    protected void update2(long recid, DataOutputByteArray out) {
        int pos = lockPos(recid);

        if(CC.ASSERT)
            assertWriteLocked(pos);
        long oldIndexVal = indexValGet(recid);

        boolean releaseOld = true;
        if(snapshotEnable){
            for(Snapshot snap:snapshots){
                snap.oldRecids[pos].putIfAbsent(recid,oldIndexVal);
                releaseOld = false;
            }
        }

        long[] oldOffsets = offsetsGet(pos,oldIndexVal);
        int oldSize = offsetsTotalSize(oldOffsets);
        int newSize = out==null?0:out.pos;
        long[] newOffsets;

        if (CC.LOG_STORE && LOG.isLoggable(Level.FINEST)) {
            LOG.log(Level.FINEST, "recid={0}, oldIndexVal={1}, oldSize={2}, newSize={3}, oldOffsets={4}",
                    new Object[]{recid, oldIndexVal, oldSize, newSize, Arrays.toString(oldOffsets)});
        }

        //if new version fits into old one, reuse space
        if(releaseOld && oldSize==newSize){
            //TODO more precise check of linked records
            //TODO check rounUp 16 for non-linked records
            newOffsets = oldOffsets;
        }else {
            structuralLock.lock();
            try {
                if(releaseOld && oldOffsets!=null)
                    freeDataPut(oldOffsets);
                newOffsets = newSize==0?null:freeDataTake(out.pos);

            } finally {
                structuralLock.unlock();
            }
        }

        if(CC.ASSERT)
            offsetsVerify(newOffsets);

        putData(recid, newOffsets, out == null ? null : out.buf, out == null ? 0 : out.pos);
    }

    protected void offsetsVerify(long[] ret) {
        //TODO check non tail records are mod 16
        //TODO check linkage
        if(ret==null)
            return;
        for(int i=0;i<ret.length;i++) {
            boolean last = (i==ret.length-1);
            boolean linked = (ret[i]&MLINKED)!=0;
            if(!last && !linked)
                throw new DBException.DataCorruption("body not linked");
            if(last && linked)
                throw new DBException.DataCorruption("tail is linked");

            long offset = ret[i]&MOFFSET;
            if(offset<PAGE_SIZE)
                throw new DBException.DataCorruption("offset is too small");
            if(((offset&MOFFSET)%16)!=0)
                throw new DBException.DataCorruption("offset not mod 16");

            int size = (int) (ret[i] >>>48);
            if(size<=0)
                throw new DBException.DataCorruption("size too small");
        }
    }


    /** return positions of (possibly) linked record */
    protected long[] offsetsGet(int segment, long indexVal) {;
        if(indexVal>>>48==0){

            return ((indexVal&MLINKED)!=0) ? null : EMPTY_LONGS;
        }

        long[] ret = new long[]{indexVal};
        while((ret[ret.length-1]&MLINKED)!=0){
            ret = Arrays.copyOf(ret,ret.length+1);
            ret[ret.length-1] = parity3Get(vol.getLong(ret[ret.length-2]&MOFFSET));
        }

        if(CC.ASSERT){
           offsetsVerify(ret);
        }

        if (CC.LOG_STORE && LOG.isLoggable(Level.FINEST)) {
            LOG.log(Level.FINEST, "indexVal={0}, ret={1}",
                    new Object[]{Long.toHexString(indexVal), Arrays.toString(ret)});
        }


        return ret;
    }

    protected void indexValPut(long recid, int size, long offset, boolean linked, boolean unused) {
        if(CC.ASSERT)
            assertWriteLocked(lockPos(recid));

        long indexOffset = recidToOffset(recid);
        long newval = composeIndexVal(size, offset, linked, unused, true);

        if (CC.LOG_STORE && LOG.isLoggable(Level.FINEST)) {
            LOG.log(Level.FINEST, "recid={0}, indexOffset={1}, newval={2}",
                    new Object[]{recid, indexOffset, Long.toHexString(newval)});
        }


        vol.putLong(indexOffset, newval);

    }


    @Override
    protected <A> void delete2(long recid, Serializer<A> serializer) {
        if(CC.ASSERT)
            assertWriteLocked(lockPos(recid));

        final int pos = lockPos(recid);
        long oldIndexVal = indexValGet(recid);
        long[] offsets = offsetsGet(pos,oldIndexVal);
        boolean releaseOld = true;
        if(snapshotEnable){
            for(Snapshot snap:snapshots){
                snap.oldRecids[pos].putIfAbsent(recid,oldIndexVal);
                releaseOld = false;
            }
        }


        if (CC.LOG_STORE && LOG.isLoggable(Level.FINEST)) {
            LOG.log(Level.FINEST, "recid={0}, oldIndexVal={1}, releaseOld={2}, offsets={3}",
                    new Object[]{recid, Long.toHexString(oldIndexVal), releaseOld, Arrays.toString(offsets)});
        }

        if(offsets!=null && releaseOld) {
            structuralLock.lock();
            try {
                freeDataPut(offsets);
            } finally {
                structuralLock.unlock();
            }
        }
        indexValPut(recid, 0, 0, true, true);
        if(!recidReuseDisable){
            structuralLock.lock();
            try {
                longStackPut(FREE_RECID_STACK, recid, false);
            }finally {
                structuralLock.unlock();
            }
        }

    }

    @Override
    public long getCurrSize() {
        structuralLock.lock();
        try {
            return vol.length() - lastAllocatedDataGet() % CHUNKSIZE;
        }finally {
            structuralLock.unlock();
        }
    }

    @Override
    public long getFreeSize() {
        long ret = freeSize.get();
        if(ret!=-1)
            return ret;
        structuralLock.lock();
        try{
            //try one more time under lock
            ret = freeSize.get();
            if(ret!=-1)
                return ret;

            //traverse list of recids,
            ret=
                    8* longStackCount(FREE_RECID_STACK);

            for(long stackNum = 1;stackNum<=SLOTS_COUNT;stackNum++){
                long indexOffset = FREE_RECID_STACK+stackNum*8;
                long size = stackNum*16;
                ret += size * longStackCount(indexOffset);
            }

            freeSize.set(ret);

            return ret;
        }finally {
            structuralLock.unlock();
        }
    }

    @Override
    public boolean fileLoad() {
        return vol.fileLoad();
    }

    protected void freeSizeIncrement(int increment){
        for(;;) {
            long val = freeSize.get();
            if (val == -1 || freeSize.compareAndSet(val, val + increment))
                return;
        }
    }

    @Override
    public long preallocate() {
        long recid;
        structuralLock.lock();
        try {
            //TODO possible race condition here? Can this modify existing data?
            recid = freeRecidTake();
        }finally {
            structuralLock.unlock();
        }
        Lock lock = locks[lockPos(recid)].writeLock();
        lock.lock();
        try {
            indexValPut(recid, 0, 0L, true, true);
        }finally {
            lock.unlock();
        }

        if (CC.LOG_STORE && LOG.isLoggable(Level.FINEST)) {
            LOG.log(Level.FINEST, "recid={0}",recid);
        }
        return recid;
    }


    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        long recid;
        long[] offsets;
        DataOutputByteArray out = serialize(value, serializer);
        boolean notalloc = out==null || out.pos==0;

        commitLock.lock();
        try {

            structuralLock.lock();
            try {
                recid = freeRecidTake();
            } finally {
                structuralLock.unlock();
            }

            int pos = lockPos(recid);
            Lock lock = locks[pos].writeLock();
            lock.lock();
            //TODO possible deadlock, should not lock segment under different segment lock
            //TODO investigate if this lock is necessary, recid has not been yet published, perhaps cache does not have to be updated
            try {
                if(CC.ASSERT && recidReuseDisable && vol.getLong(recidToOffset(recid))!=0){
                    throw new AssertionError("Recid not empty: "+recid);
                }

                if (caches != null) {
                    caches[pos].put(recid, value);
                }
                if (snapshotEnable) {
                    for (Snapshot snap : snapshots) {
                        snap.oldRecids[pos].putIfAbsent(recid, 0);
                    }
                }

                structuralLock.lock();
                try {
                    offsets = notalloc ? null : freeDataTake(out.pos);
                } finally {
                    structuralLock.unlock();
                }
                if (CC.ASSERT && offsets != null && (offsets[0] & MOFFSET) < PAGE_SIZE)
                    throw new DBException.DataCorruption();

                putData(recid, offsets, out == null ? null : out.buf, out == null ? 0 : out.pos);
            } finally {
                lock.unlock();
            }
        }finally {
            commitLock.unlock();
        }


        if (CC.LOG_STORE && LOG.isLoggable(Level.FINEST)) {
            LOG.log(Level.FINEST, "recid={0}, serSize={1}, serializer={2}",
                    new Object[]{recid, notalloc?0:out.pos, serializer});
        }
        return recid;
    }

    protected void putData(long recid, long[] offsets, byte[] src, int srcLen) {
        if(CC.ASSERT)
            assertWriteLocked(lockPos(recid));
        if(CC.ASSERT && offsetsTotalSize(offsets)!=(src==null?0:srcLen))
            throw new DBException.DataCorruption("size mismatch");


        if (CC.LOG_STORE && LOG.isLoggable(Level.FINEST)) {
            LOG.log(Level.FINEST, "recid={0}, srcLen={1}, offsets={2}",
                    new Object[]{recid, srcLen, Arrays.toString(offsets)});
        }

        if(offsets!=null) {
            int outPos = 0;
            for (int i = 0; i < offsets.length; i++) {
                final boolean last = (i == offsets.length - 1);
                if (CC.ASSERT && ((offsets[i] & MLINKED) == 0) != last)
                    throw new DBException.DataCorruption("linked bit set wrong way");

                long offset = (offsets[i] & MOFFSET);
                if(CC.ASSERT && offset%16!=0)
                    throw new DBException.DataCorruption("not aligned to 16");

                int plus = (last?0:8);
                int size = (int) ((offsets[i]>>>48) - plus);
                if(CC.ASSERT && ((size&0xFFFF)!=size || size==0))
                    throw new DBException.DataCorruption("size mismatch");

                int segment = lockPos(recid);
                //write offset to next page
                if (!last) {
                    putDataSingleWithLink(segment, offset,parity3Set(offsets[i + 1]), src,outPos,size);
                }else{
                    putDataSingleWithoutLink(segment, offset, src, outPos, size);
                }
                outPos += size;

            }
            if(CC.ASSERT && outPos!=srcLen)
                throw new DBException.DataCorruption("size mismatch");
        }
        //update index val
        boolean firstLinked =
                (offsets!=null && offsets.length>1) || //too large record
                (src==null); //null records
        boolean empty = offsets==null || offsets.length==0;
        int firstSize = (int) (empty ? 0L : offsets[0]>>>48);
        long firstOffset =  empty? 0L : offsets[0]&MOFFSET;
        indexValPut(recid, firstSize, firstOffset, firstLinked, false);
    }

    protected void putDataSingleWithoutLink(int segment, long offset, byte[] buf, int bufPos, int size) {
        vol.putData(offset, buf, bufPos, size);
    }

    protected void putDataSingleWithLink(int segment, long offset, long link, byte[] buf, int bufPos, int size) {
        vol.putLong(offset, link);
        vol.putData(offset + 8, buf, bufPos, size);
    }

    protected void freeDataPut(long[] linkedOffsets) {
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        for(long v:linkedOffsets){
            int size = round16Up((int) (v >>> 48));
            v &= MOFFSET;
            freeDataPut(v,size);
        }
    }


    protected void freeDataPut(long offset, int size) {
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.ASSERT && size%16!=0 )
            throw new DBException.DataCorruption("unalligned size");
        if(CC.ASSERT && (offset%16!=0 || offset<PAGE_SIZE))
            throw new DBException.DataCorruption("wrong offset");


        if (CC.LOG_STORE && LOG.isLoggable(Level.FINEST)) {
            LOG.log(Level.FINEST, "offset={0}, size={1}",
                    new Object[]{offset, size});
        }

        if(!(this instanceof  StoreWAL)) //TODO WAL needs to handle record clear, perhaps WAL instruction?
            vol.clear(offset,offset+size);

        //shrink store if this is last record
        if(offset+size== lastAllocatedDataGet()){
            if(offset%PAGE_SIZE==0){
                //shrink current page
                if(CC.ASSERT && offset+PAGE_SIZE!=storeSizeGet())
                    throw new AssertionError();
                storeSizeSet(offset);
                lastAllocatedDataSet(0);
            }else {
                lastAllocatedDataSet(offset);
            }
            return;
        }

        freeSizeIncrement(size);

        longStackPut(
                longStackMasterLinkOffset(size),
                offset >>> 4, //offset is multiple of 16, save some space
                false);
    }


    protected long[] freeDataTake(int size) {
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.ASSERT && size<=0)
            throw new DBException.DataCorruption("size too small");

        //compose of multiple single records
        long[] ret = EMPTY_LONGS;
        while(size>MAX_REC_SIZE){
            ret = Arrays.copyOf(ret,ret.length+1);
            ret[ret.length-1] = (((long)MAX_REC_SIZE)<<48) | freeDataTakeSingle(round16Up(MAX_REC_SIZE)) | MLINKED;
            size = size-MAX_REC_SIZE+8;
        }
        //allocate last section
        ret = Arrays.copyOf(ret,ret.length+1);
        ret[ret.length-1] = (((long)size)<<48) | freeDataTakeSingle(round16Up(size)) ;


        if (CC.LOG_STORE && LOG.isLoggable(Level.FINEST)) {
            LOG.log(Level.FINEST, "size={0}, ret={1}",
                    new Object[]{size, Arrays.toString(ret)});
        }

        return ret;
    }

    protected long freeDataTakeSingle(int size) {
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.ASSERT && size%16!=0)
            throw new DBException.DataCorruption("unalligned size");
        if(CC.ASSERT && size>round16Up(MAX_REC_SIZE))
            throw new DBException.DataCorruption("size too big");

        long ret = longStackTake(longStackMasterLinkOffset(size),false) <<4; //offset is multiple of 16, save some space
        if(ret!=0) {
            if(CC.ASSERT && ret<PAGE_SIZE)
                throw new DBException.DataCorruption("wrong ret");
            if(CC.ASSERT && ret%16!=0)
                throw new DBException.DataCorruption("unalligned ret");

            if (CC.LOG_STORE && LOG.isLoggable(Level.FINEST)) {
                LOG.log(Level.FINEST, "size={0}, ret={1}",
                        new Object[]{size, Long.toHexString(ret)});
            }

            freeSizeIncrement(-size);

            return ret;
        }

        if(lastAllocatedDataGet()==0){
            //allocate new data page
            long page = pageAllocate();
            lastAllocatedDataSet(page+size);
            if(CC.ASSERT && page<PAGE_SIZE)
                throw new DBException.DataCorruption("wrong page");
            if(CC.ASSERT && page%16!=0)
                throw new DBException.DataCorruption("unalligned page");

            if (CC.LOG_STORE && LOG.isLoggable(Level.FINEST)) {
                LOG.log(Level.FINEST, "size={0}, ret={1}",
                        new Object[]{size, Long.toHexString(ret)});
            }

            return page;
        }

        //does record fit into rest of the page?
        if((lastAllocatedDataGet()%PAGE_SIZE + size)/PAGE_SIZE !=0){
            long offsetToFree = lastAllocatedDataGet();
            long sizeToFree = Fun.roundUp(offsetToFree,PAGE_SIZE) - offsetToFree;
            if(CC.ASSERT && (offsetToFree%16!=0 || sizeToFree%16!=0))
                throw new AssertionError();

            //now reset, this will force new page start
            lastAllocatedDataSet(0);

            //mark space at end of this page as free
            freeDataPut(offsetToFree, (int) sizeToFree);
            return freeDataTakeSingle(size);
        }
        //yes it fits here, increase pointer
        long lastAllocatedData = lastAllocatedDataGet();
        ret = lastAllocatedData;
        lastAllocatedDataSet(lastAllocatedData+size);

        if(CC.ASSERT && ret%16!=0)
            throw new DBException.DataCorruption();
        if(CC.ASSERT && lastAllocatedData%16!=0)
            throw new  DBException.DataCorruption();
        if(CC.ASSERT && ret<PAGE_SIZE)
            throw new  DBException.DataCorruption();

        if (CC.LOG_STORE && LOG.isLoggable(Level.FINEST)) {
            LOG.log(Level.FINEST, "size={0}, ret={1}",
                    new Object[]{size, Long.toHexString(ret)});
        }

        if(CC.PARANOID && CC.VOLUME_ZEROUT) {
            long offset = ret&MOFFSET;
            long size2 = ret>>>48;
            assertZeroes(offset,offset+size2);
        }

        return ret;
    }


    //TODO use var size
    protected final static long CHUNKSIZE = 100*16;

    protected void longStackPut(final long masterLinkOffset, final long value, boolean recursive){
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.ASSERT && (masterLinkOffset<=0 || masterLinkOffset>PAGE_SIZE || masterLinkOffset % 8!=0)) //TODO perhaps remove the last check
            throw new DBException.DataCorruption("wrong master link");

        long masterLinkVal = parity4Get(headVol.getLong(masterLinkOffset));
        long pageOffset = masterLinkVal&MOFFSET;

        if(masterLinkVal==0L){
            longStackNewPage(masterLinkOffset, 0L, value);
            return;
        }

        long currSize = masterLinkVal>>>48;

        long prevLinkVal = parity4Get(vol.getLong(pageOffset));
        long pageSize = prevLinkVal>>>48;
        //is there enough space in current page?
        if(currSize+8>=pageSize){ // +8 is just to make sure and is worse case scenario, perhaps make better check based on actual packed size
            //no there is not enough space
            //first zero out rest of the page
            vol.clear(pageOffset+currSize, pageOffset+pageSize);
            //allocate new page
            longStackNewPage(masterLinkOffset,pageOffset,value);
            return;
        }

        //there is enough space, so just write new value
        currSize += vol.putLongPackBidi(pageOffset + currSize, longParitySet(value));
        //and update master pointer
        headVol.putLong(masterLinkOffset, parity4Set(currSize<<48 | pageOffset));
    }


    protected void longStackNewPage(long masterLinkOffset, long prevPageOffset, long value) {
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        long newPageOffset = freeDataTakeSingle((int) CHUNKSIZE);
        //write size of current chunk with link to prev page
        vol.putLong(newPageOffset, parity4Set((CHUNKSIZE<<48) | prevPageOffset));
        //put value
        long currSize = 8 + vol.putLongPackBidi(newPageOffset + 8, longParitySet(value));
        //update master pointer
        headVol.putLong(masterLinkOffset, parity4Set((currSize<<48)|newPageOffset));
    }


    protected long longStackTake(long masterLinkOffset, boolean recursive){
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.ASSERT && (masterLinkOffset<FREE_RECID_STACK ||
                masterLinkOffset>longStackMasterLinkOffset(round16Up(MAX_REC_SIZE)) ||
                masterLinkOffset % 8!=0))
            throw new DBException.DataCorruption("wrong master link");

        long masterLinkVal = parity4Get(headVol.getLong(masterLinkOffset));
        if(masterLinkVal==0 ){
            return 0;
        }
        long currSize = masterLinkVal>>>48;
        final long pageOffset = masterLinkVal&MOFFSET;

        //read packed link from stack
        long ret = vol.getLongPackBidiReverse(pageOffset+currSize);
        //extract number of read bytes
        long oldCurrSize = currSize;
        currSize-= ret >>>60;
        //clear bytes occupied by prev value
        vol.clear(pageOffset+currSize, pageOffset+oldCurrSize);
        //and finally set return value
        ret = longParityGet(ret & DataIO.PACK_LONG_RESULT_MASK);

        if(CC.ASSERT && currSize<8)
            throw new DBException.DataCorruption();

        //is there space left on current page?
        if(currSize>8){
            //yes, just update master link
            headVol.putLong(masterLinkOffset, parity4Set(currSize << 48 | pageOffset));
            return ret;
        }

        //there is no space at current page, so delete current page and update master pointer
        long prevPageOffset = parity4Get(vol.getLong(pageOffset));
        final int currPageSize = (int) (prevPageOffset>>>48);
        prevPageOffset &= MOFFSET;

        //does previous page exists?
        if(prevPageOffset!=0) {
            //yes previous page exists

            //find pointer to end of previous page
            // (data are packed with var size, traverse from end of page, until zeros

            //first read size of current page
            currSize = parity4Get(vol.getLong(prevPageOffset)) >>> 48;

            //now read bytes from end of page, until they are zeros
            while (vol.getUnsignedByte(prevPageOffset + currSize-1) == 0) {
                currSize--;
            }

            if (CC.ASSERT && currSize < 10)
                throw new DBException.DataCorruption();
        }else{
            //no prev page does not exist
            currSize=0;
        }

        //update master link with curr page size and offset
        headVol.putLong(masterLinkOffset, parity4Set(currSize<<48 | prevPageOffset));

        //release old page, size is stored as part of prev page value
        freeDataPut(pageOffset, currPageSize);

        return ret;
    }


    protected long longStackCount(final long masterLinkOffset){
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        if (CC.ASSERT && (masterLinkOffset <= 0 || masterLinkOffset > PAGE_SIZE || masterLinkOffset % 8 != 0))
            throw new DBException.DataCorruption("wrong master link");


        long nextLinkVal = DataIO.parity4Get(
                headVol.getLong(masterLinkOffset));
        long ret = 0;
        while(true){

            final long pageOffset = nextLinkVal&MOFFSET;

            if(pageOffset==0)
                break;

            long currSize = parity4Get(vol.getLong(pageOffset))>>>48;

            //now read bytes from end of page, until they are zeros
            while (vol.getUnsignedByte(pageOffset + currSize-1) == 0) {
                currSize--;
            }

            //iterate from end of page until start of page is reached
            while(currSize>8){
                long read = vol.getLongPackBidiReverse(pageOffset+currSize);
                //extract number of read bytes
                currSize-= read >>>60;
                ret++;
            }

            nextLinkVal = DataIO.parity4Get(
                    vol.getLong(pageOffset));
        }
        return ret;
    }

    @Override
    public void close() {
        if(closed==true)
            return;
        
        commitLock.lock();
        try {
            if(closed==true)
                return;
            flush();
            vol.close();
            vol = null;
            if(this instanceof StoreCached)
                headVol.close();

            if (caches != null) {
                for (Cache c : caches) {
                    c.close();
                }
                Arrays.fill(caches,null);
            }
            if(fileLockHeartbeat !=null) {
                fileLockHeartbeat.unlock();
                fileLockHeartbeat = null;
            }
            closed = true;
        }finally{
            commitLock.unlock();
        }
    }


    @Override
    public void commit() {
        commitLock.lock();
        try {
            flush();
        }finally{
            commitLock.unlock();
        }
    }

    protected void flush() {
        if(isReadOnly())
            return;
        structuralLock.lock();
        try{
            //and set header checksum
            vol.putInt(HEAD_CHECKSUM, headChecksum(vol));
        }finally {
            structuralLock.unlock();
        }
        vol.sync();
    }

    @Override
    public void rollback() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean canRollback() {
        return false;
    }

    @Override
    public Engine snapshot() throws UnsupportedOperationException {
        if(!snapshotEnable)
            throw new UnsupportedOperationException();
        return new Snapshot(StoreDirect.this);
    }

    @Override
    public void clearCache() {

    }

    @Override
    public void backup(OutputStream out, boolean incremental) {
        //lock everything
        for(ReadWriteLock lock:locks){
            lock.writeLock().lock();
        }
        try {
            long maxRecid = maxRecidGet();
            recidLoop:
            for (long recid = 1; recid <= maxRecid; recid++) {
                long indexOffset = recidToOffset(recid);
                long indexVal = vol.getLong(indexOffset);

                //check if was discarded
                if((indexVal&MUNUSED)!=0||indexVal == 0){
                    continue recidLoop;
                }

                //check if recid was modified since last incrementa thingy
                if(incremental && (indexVal&MARCHIVE)==0){
                    continue recidLoop;
                }

                //TODO we need write lock to do this, there could be setting make backup without archive marker, but only under readlock
                //mark value as not modified
                indexVal = DataIO.parity1Get(indexVal);
                indexValPut(recid, (int) (indexVal>>>48), indexVal&MOFFSET,
                        (indexVal&MLINKED)!=0, false);

                //write recid
                DataIO.packLong(out, recid);

                //load record
                long[] offsets = offsetsGet(lockPos(recid),indexVal);
                int totalSize = offsetsTotalSize(offsets);
                if(offsets!=null) {
                    byte[] b = getLoadLinkedRecord(offsets, totalSize);

                    //write size and data
                    DataIO.packLong(out, b.length+1);
                    out.write(b);
                }else{
                    DataIO.packLong(out, 0);
                }
                //TODO checksums
            }
            //EOF mark
            DataIO.packLong(out,-1);
        }catch (IOException e){
            throw new DBException.VolumeIOError(e);
        }finally {
            //unlock everything in reverse order to prevent deadlocks
            for(int i=locks.length-1;i>=0;i--){
                locks[i].writeLock().unlock();
            }
        }
    }



    @Override
    public void backupRestore(InputStream[] ins) {
        //check we are empty
        if(RECID_LAST_RESERVED+1!=maxRecidGet()){
            throw new DBException.WrongConfig("Can not restore backup, this store is not empty!");
        }

        for(ReadWriteLock lock:locks){
            lock.writeLock().lock();
        }
        structuralLock.lock();
        try {
            BitSet usedRecid = new BitSet();

            streamsLoop:
            for(int i=ins.length-1;i>=0;i--) {
                InputStream in = ins[i];
                recidLoop:
                for (; ; ) {
                    long recid = DataIO.unpackLong(in);
                    if (recid == -1) { // EOF
                        continue streamsLoop;
                    }

                    long len = DataIO.unpackLong(in);

                    if(ins.length!=1) {
                        if(recid>Integer.MAX_VALUE)
                            throw new AssertionError(); //TODO support bigger recids

                        if (usedRecid.get((int) recid)) {
                            //recid was already addressed in other incremental backup
                            //so skip length and continue
                            long toSkip = len - 1;
                            if (toSkip > 0) {
                                DataIO.skipFully(in, toSkip);
                            }
                            continue recidLoop;
                        }
                        usedRecid.set((int) recid);
                    }

                    if (len == 0) {
                        //null record
                        indexValPut(recid, 0, 0, true, false);
                    } else {
                        byte[] data = new byte[(int) (len - 1)];
                        DataIO.readFully(in, data);
                        long[] newOffsets = freeDataTake(data.length);
                        pageIndexEnsurePageForRecidAllocated(recid);
                        putData(recid, newOffsets, data, data.length);
                    }
                }
            }
        }catch (IOException e){
            throw new DBException.VolumeIOError(e);
        }finally {
            structuralLock.unlock();
            //unlock everything in reverse order to prevent deadlocks
            for(int i=locks.length-1;i>=0;i--){
                locks[i].writeLock().unlock();
            }
        }
    }

    @Override
    public void compact() {
        //check for some file used during compaction, if those exists, refuse to compact
        if(compactOldFilesExists()){
            return;
        }

        final boolean isStoreCached = this instanceof StoreCached;
        commitLock.lock();

        try{
            for(int i=0;i<locks.length;i++){
                Lock lock = locks[i].writeLock();
                lock.lock();
            }

            try {

                //clear caches, so freed recids throw an exception, instead of returning null
                if(caches!=null) {
                    for (Cache c : caches) {
                        c.clear();
                    }
                }
                snapshotCloseAllOnCompact();


                String compactedFile = vol.getFile()==null? null : fileName+".compact";
                final StoreDirect target = new StoreDirect(compactedFile,
                        volumeFactory,
                        null,lockScale,
                        executor==null?LOCKING_STRATEGY_NOLOCK:LOCKING_STRATEGY_WRITELOCK,
                        checksum,compress,null,false,false,
                        true, //locking is disabled on compacted file
                        null,
                        null, startSize, sizeIncrement, false);
                target.init();

                final AtomicLong maxRecid = new AtomicLong(
                        maxRecidGet());

                //TODO what about recids which are already in freeRecidLongStack?
                // I think it gets restored by traversing index table,
                // so there is no need to traverse and copy freeRecidLongStack
                // TODO same problem in StoreWAL
                compactIndexPages(target, maxRecid);


                //update some stuff
                structuralLock.lock();
                try {

                    target.maxRecidSet(maxRecid.get());
                    this.indexPages = target.indexPages;

                    //compaction done, swap target with current
                    if(compactedFile==null) {
                        //in memory vol without file, just swap everything
                        Volume oldVol = this.vol;
                        if(this instanceof StoreCached)
                            headVol.close();
                        this.headVol = this.vol = target.vol;
                        //TODO update variables
                        oldVol.close();
                    }else{
                        File compactedFileF = new File(compactedFile);
                        //close everything
                        target.vol.sync();
                        target.close();
                        //TODO manipulation with `vol` must be under write segment lock. Find way to swap under read lock
                        this.vol.sync();
                        this.vol.close();
                        //rename current file
                        File currFile = new File(this.fileName);
                        File currFileRenamed = new File(currFile.getPath()+".compact_orig");
                        if(!currFile.renameTo(currFileRenamed)){
                            //failed to rename file, perhaps still open
                            //TODO recovery here. Perhaps copy data from one file to other, instead of renaming it
                            throw new AssertionError("failed to rename file "+currFile+" - "+currFile.exists()+" - "+currFileRenamed.exists());
                        }

                        //rename compacted file to current file
                        if(!compactedFileF.renameTo(currFile)) {
                            //TODO recovery here.
                            throw new AssertionError("failed to rename file " + compactedFileF);
                        }

                        //and reopen volume
                        if(this instanceof StoreCached)
                            this.headVol.close();
                        this.vol = volumeFactory.makeVolume(this.fileName, readonly, fileLockDisable);
                        this.headVol = vol;
                        if(isStoreCached){
                            ((StoreCached)this).dirtyStackPages.clear();
                        }

                        //delete old file
                        if(!currFileRenamed.delete()){
                            LOG.warning("Could not delete old compaction file: "+currFileRenamed);
                        }

                    }

                    //reset free size
                    freeSize.set(-1);
                }finally {
                    structuralLock.unlock();
                }
            }finally {
                for(int i=locks.length-1;i>=0;i--) {
                    Lock lock = locks[i].writeLock();
                    lock.unlock();
                }
            }
        }finally{
            commitLock.unlock();
        }

    }

    protected boolean compactOldFilesExists() {
        if(fileName!=null){
            for(String s:new String[]{".compact_orig",".compact",".wal.c" ,".wal.c.compact" }) {
                File oldData = new File(fileName + s);
                if (oldData.exists()) {
                    LOG.warning("Old compaction data exists, compaction not started: " + oldData);
                    return true;
                }
            }

        }
        return false;
    }

    protected void snapshotCloseAllOnCompact() {
        //close all snapshots
        if(snapshotEnable){
            boolean someClosed = false;
            for(Snapshot snap:snapshots){
                someClosed = true;
                snap.close();
            }
            if(someClosed)
                LOG.log(Level.WARNING, "Compaction closed existing snapshots.");
        }
    }

    protected void compactIndexPages(final StoreDirect target, final AtomicLong maxRecid) {
        int lastIndexPage = indexPages.length;

        // make maxRecid lower if possible
        // decrement maxRecid until non-empty recid is found
        recidLoop: for(;;){
            if(maxRecid.get()<=RECID_LAST_RESERVED){
                //some recids are reserved, so break if we reach those
                break recidLoop;
            }

            long indexVal = indexValGetRaw(maxRecid.get());
            if ((indexVal & MUNUSED) == 0 && indexVal != 0) {
                //non empty recid found, so break this loop
                break recidLoop;
            }
            // maxRecid is empty, so decrement and move on
            maxRecid.decrementAndGet();
        }

        //iterate over index pages
        long maxRecidOffset = recidToOffset(maxRecid.get());
        if(executor == null) {
            for (int indexPageI = 0;
                 indexPageI < lastIndexPage && indexPages[indexPageI]<=maxRecidOffset;
                 indexPageI++) {

                compactIndexPage(target, indexPageI, maxRecid.get());
            }
        }else {
            //compact pages in multiple threads.
            //there are N tasks (index pages) running in parallel.
            //main thread checks number of tasks in interval, if one is finished it will
            //schedule next one
            final List<Future> tasks = new ArrayList();
            for (int indexPageI = 0;
                 indexPageI < lastIndexPage && indexPages[indexPageI]<=maxRecidOffset;
                 indexPageI++) {
                final int indexPageI2 = indexPageI;
                //now submit tasks to executor, it will compact single page
                //TODO handle RejectedExecutionException?
                Future f = executor.submit(new Runnable() {
                    @Override
                    public void run() {
                      compactIndexPage(target, indexPageI2, maxRecid.get());
                    }
                });
                tasks.add(f);
            }
            //all index pages are running or were scheduled
            //wait for all index pages to finish
            for(Future f:tasks){
                try {
                    f.get();
                } catch (InterruptedException e) {
                    throw new DBException.Interrupted(e);
                } catch (ExecutionException e) {
                    //TODO check cause and rewrap it
                    throw new RuntimeException(e);
                }
            }

        }
    }

    protected void compactIndexPage(StoreDirect target, int indexPageI, long maxRecid) {
        final long indexPage = indexPages[indexPageI];

        long recid = (indexPageI==0? 0 : indexPageI * (PAGE_SIZE-8)/ INDEX_VAL_SIZE - HEAD_END/ INDEX_VAL_SIZE);
        final long indexPageStart = (indexPage==0?HEAD_END+8 : indexPage+8);

        final long indexPageEnd = indexPage+PAGE_SIZE;

       //iterate over indexOffset values
        //TODO check if preloading and caching of all indexVals on this index page would improve performance
        indexVal:
        for( long indexOffset=indexPageStart;
                indexOffset<indexPageEnd;
                indexOffset+= INDEX_VAL_SIZE){
            recid++;

            if(CC.ASSERT && indexOffset!=recidToOffset(recid))
                throw new AssertionError("Recid to offset conversion failed: indexOffset:"+indexOffset+
                        ", recidToOffset: "+recidToOffset(recid)+", recid:"+recid);

            if(recid>maxRecid)
                break indexVal;


            final long indexVal = vol.getLong(indexOffset);

            //check if was discarded
            if((indexVal&MUNUSED)!=0||indexVal == 0){
                //mark rec id as free, so it can be reused
                target.structuralLock.lock();
                target.longStackPut(FREE_RECID_STACK, recid, false);
                target.structuralLock.unlock();
                continue indexVal;
            }


            //deal with linked record non zero record
            if((indexVal & MLINKED)!=0 && indexVal>>>48!=0){
                //load entire linked record into byte[]
                long[] offsets = offsetsGet(lockPos(recid),indexValGet(recid));
                int totalSize = offsetsTotalSize(offsets);
                byte[] b = getLoadLinkedRecord(offsets, totalSize);

                //now put into new store, acquire locks
                target.locks[lockPos(recid)].writeLock().lock();
                target.structuralLock.lock();
                //allocate space
                long[] newOffsets = target.freeDataTake(totalSize);

                target.pageIndexEnsurePageForRecidAllocated(recid);
                target.putData(recid,newOffsets,b, totalSize);

                target.structuralLock.unlock();
                target.locks[lockPos(recid)].writeLock().unlock();


                continue indexVal;
            }

            target.locks[lockPos(recid)].writeLock().lock();
            target.structuralLock.lock();
            target.pageIndexEnsurePageForRecidAllocated(recid);
            //TODO preserver archive flag
            target.updateFromCompact(recid, indexVal, vol);
            target.structuralLock.unlock();
            target.locks[lockPos(recid)].writeLock().unlock();

        }
    }


    private void updateFromCompact(long recid, long indexVal, Volume oldVol) {
        //allocate new space
        int size = (int) (indexVal>>>48);
        long newOffset[];
        if(size>0) {
            newOffset=freeDataTake(size);
            if (newOffset.length != 1)
                throw new DBException.DataCorruption();

            //transfer data
            oldVol.transferInto(indexVal & MOFFSET, this.vol, newOffset[0]&MOFFSET, size);
        }else{
            newOffset = new long[1];
        }

        //update index val
        //TODO preserver archive flag
        indexValPut(recid, size, newOffset[0]&MOFFSET, (indexVal&MLINKED)!=0, false);
    }


    protected long indexValGet(long recid) {
        if(CC.ASSERT)
            assertReadLocked(recid);

        long offset = recidToOffset(recid);
        long indexVal = vol.getLong(offset);
        if(indexVal == 0)
            throw new DBException.EngineGetVoid();

        //check parity and throw recid does not exist if broken
        return DataIO.parity1Get(indexVal);
    }


    protected long indexValGetRaw(long recid) {
        if(CC.ASSERT)
            assertReadLocked(recid);

        long offset = recidToOffset(recid);
        return vol.getLong(offset);
    }

    protected final long recidToOffset(long recid) {
        if(CC.ASSERT && recid<=0)
            throw new AssertionError();
        if(CC.ASSERT && recid>>>48 !=0)
            throw new AssertionError();
        //there is no zero recid, but that position will be used for zero Index Page checksum

        //convert recid to offset
        recid = HEAD_END + recid * 8 ;

        //compensate for 16 bytes at start of each index page (next page link and checksum)
        recid+= Math.min(1, recid/PAGE_SIZE)*    //min servers as replacement for if(recid>=PAGE_SIZE)
                (16 + ((recid-PAGE_SIZE)/(PAGE_SIZE-16))*16);

        //look up real offset
        recid = indexPages[(int) (recid / PAGE_SIZE)] + recid%PAGE_SIZE;
        return recid;
    }
    private long recidToOffsetChecksum(long recid) {
        //convert recid to offset
        recid = (recid-1) * INDEX_VAL_SIZE + HEAD_END + 8;

        if(recid+ INDEX_VAL_SIZE >PAGE_SIZE){
            //align from zero page
            recid+=2+8;
        }

        //align for every other page
        //TODO optimize away loop
        for(long page=PAGE_SIZE*2;recid+ INDEX_VAL_SIZE >page;page+=PAGE_SIZE){
            recid+=8+(PAGE_SIZE-8)% INDEX_VAL_SIZE;
        }

        //look up real offset
        recid = indexPages[((int) (recid / PAGE_SIZE))] + recid%PAGE_SIZE;
        return recid;

    }

    /** check if recid offset fits into current allocated structure */
    protected boolean recidTooLarge(long recid) {
        try{
            recidToOffset(recid);
            return false;
        }catch(ArrayIndexOutOfBoundsException e){
            //TODO hack
            return true;
        }
    }


    protected static long composeIndexVal(int size, long offset,
        boolean linked, boolean unused, boolean archive){
        if(CC.ASSERT && (size&0xFFFF)!=size)
            throw new DBException.DataCorruption("size too large");
        if(CC.ASSERT && (offset&MOFFSET)!=offset)
            throw new DBException.DataCorruption("offset too large");
        offset = (((long)size)<<48) |
                offset |
                (linked?MLINKED:0L)|
                (unused?MUNUSED:0L)|
                (archive?MARCHIVE:0L);
        return parity1Set(offset);
    }


    /** returns new recid, recid slot is allocated and ready to use */
    protected long freeRecidTake() {
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        //try to reuse recid from free list
        long currentRecid = longStackTake(FREE_RECID_STACK,false);
        if(currentRecid!=0) {
            return currentRecid;
        }

        currentRecid = maxRecidGet()*INDEX_VAL_SIZE;
        currentRecid+= INDEX_VAL_SIZE;
        maxRecidSet(currentRecid/INDEX_VAL_SIZE);

        currentRecid/= INDEX_VAL_SIZE;
        //check if new index page has to be allocated
        if(recidTooLarge(currentRecid)){
            pageIndexExtend();
        }

        return currentRecid;
    }

    protected void indexLongPut(long offset, long val){
        vol.putLong(offset,val);
    }

    protected void pageIndexEnsurePageForRecidAllocated(long recid) {
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        //convert recid into Index Page number
        //TODO is this correct?
        recid = recid * INDEX_VAL_SIZE + HEAD_END;
        recid = recid / (PAGE_SIZE-8);

        while(indexPages.length<=recid)
            pageIndexExtend();
    }

    protected void pageIndexExtend() {
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        //allocate new index page
        long indexPage = pageAllocate();

        //add link to previous page
        long nextPagePointerOffset = indexPages[indexPages.length-1];
        //if zero page, put offset to end of page
        nextPagePointerOffset = Math.max(nextPagePointerOffset, HEAD_END);
        indexLongPut(nextPagePointerOffset, parity16Set(indexPage));

        //set zero link on next page
        indexLongPut(indexPage, parity16Set(0));
        //zero out checksum
        indexLongPut(indexPage+8, 0L);

        //put into index page array
        long[] indexPages2 = Arrays.copyOf(indexPages,indexPages.length+1);
        indexPages2[indexPages.length]=indexPage;
        indexPages = indexPages2;
    }

    protected long pageAllocate() {
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        long storeSize = storeSizeGet();
        vol.ensureAvailable(storeSize + PAGE_SIZE);
        vol.clear(storeSize,storeSize+PAGE_SIZE);
        storeSizeSet(storeSize + PAGE_SIZE);

        if(CC.ASSERT && storeSize%PAGE_SIZE!=0)
            throw new DBException.DataCorruption();

        return storeSize;
    }

    protected static int round16Up(int pos) {
        return (pos+15)/16*16;
    }

    public static final class Snapshot extends ReadOnly{

        protected StoreDirect engine;
        protected LongLongMap[] oldRecids;

        public Snapshot(StoreDirect engine){
            this.engine = engine;
            oldRecids = new LongLongMap[engine.lockScale];
            for(int i=0;i<oldRecids.length;i++){
                oldRecids[i] = new LongLongMap();
            }
            engine.snapshots.add(Snapshot.this);
        }

        @Override
        public <A> A get(long recid, Serializer<A> serializer) {
            StoreDirect engine = this.engine;
            int pos = engine.lockPos(recid);
            Lock lock = engine.locks[pos].readLock();
            lock.lock();
            try{
                long indexVal = oldRecids[pos].get(recid);
                if(indexVal==-1)
                    return null; //null or deleted object
                if(indexVal==-2)
                    return null; //TODO deserialize empty object

                if(indexVal!=0){
                    long[] offsets = engine.offsetsGet(pos, indexVal);
                    return engine.getFromOffset(serializer,offsets);
                }

                return engine.get2(recid,serializer);
            }finally {
                lock.unlock();
            }
        }

        @Override
        public void close() {
            //TODO lock here?
            engine.snapshots.remove(Snapshot.this);
            engine = null;
            oldRecids = null;
            //TODO put oldRecids into free space
        }

        @Override
        public boolean isClosed() {
            return engine!=null;
        }

        @Override
        public boolean canRollback() {
            return false;
        }

        @Override
        public boolean canSnapshot() {
            return true;
        }

        @Override
        public Engine snapshot() throws UnsupportedOperationException {
            return this;
        }

        @Override
        public Engine getWrappedEngine() {
            return engine;
        }

        @Override
        public void clearCache() {

        }
    }

    Map<Long,List<Long>> longStackDumpAll(){
        Map<Long,List<Long>> ret = new LinkedHashMap<Long, List<Long>>();
        masterLoop: for(long masterSize = 0; masterSize<64*1024; masterSize+=16){
            long masterLinkOffset = masterSize==0? FREE_RECID_STACK : longStackMasterLinkOffset(masterSize);
            List<Long> l = longStackDump(masterLinkOffset);
            if(!l.isEmpty())
                ret.put(masterSize, l);
        }
        return ret;
    }

    protected long longStackMasterLinkOffset(long masterSize) {
        if(CC.ASSERT && masterSize%16!=0)
            throw new AssertionError();
        return masterSize/2 + FREE_RECID_STACK; // really is size*8/16
    }

    List<Long> longStackDump(long masterLinkOffset) {
        List<Long> ret = new ArrayList<Long>();

        long nextLinkVal = DataIO.parity4Get(
                headVol.getLong(masterLinkOffset));

        pageLoop:
        while(true){

            final long pageOffset = nextLinkVal&MOFFSET;

            if(pageOffset==0)
                break pageLoop;

            long currSize = parity4Get(vol.getLong(pageOffset))>>>48;

            //now read bytes from end of page, until they are zeros
            while (vol.getUnsignedByte(pageOffset + currSize-1) == 0) {
                currSize--;
            }

            //iterate from end of page until start of page is reached
            while(currSize>8){
                long read = vol.getLongPackBidiReverse(pageOffset+currSize);
                long val = read&DataIO.PACK_LONG_RESULT_MASK;
                val = longParityGet(val);
                ret.add(val);
                //extract number of read bytes
                currSize-= read >>>60;
            }

            nextLinkVal = DataIO.parity4Get(
                    vol.getLong(pageOffset));
        }
        return ret;
    }

    /** paranoid store check. Check for overlaps, empty space etc... */
    void storeCheck(){
        structuralLock.lock();
        try {
            long storeSize = storeSizeGet();
            /**
             * This BitSet contains 1 for bytes which are accounted for (part of data, or marked as free)
             * At end there should be no unaccounted bytes, and this BitSet is completely filled
             */
            BitSet b = new BitSet((int) storeSize); // TODO limited to 2GB, add BitSet methods to Volume
            b.set(0, (int) (HEAD_END + 8), true); // +8 is zero Index Page checksum


            if (vol.length() < storeSize)
                throw new AssertionError("Store too small, need " + storeSize + ", got " + vol.length());

            vol.assertZeroes(storeSize, vol.length());


            /**
             * Check free data by traversing Long Stack Pages
             */
            //iterate over Long Stack Pages
            masterSizeLoop:
            for (long masterSize = 16; masterSize <= 64 * 1024; masterSize += 16) {
                long masterOffset = longStackMasterLinkOffset(masterSize);
                long nextLinkVal = parity4Get(headVol.getLong(masterOffset));

                pageLoop:
                while (true) {
                    final long pageOffset = nextLinkVal & MOFFSET;

                    if (pageOffset == 0)
                        break pageLoop;

                    long pageSize = parity4Get(vol.getLong(pageOffset)) >>> 48;

                    //mark this Long Stack Page occupied
                    storeCheckMark(b, true, pageOffset, pageSize);

                    //now read bytes from end of page, until they are zeros
                    while (vol.getUnsignedByte(pageOffset + pageSize - 1) == 0) {
                        pageSize--;
                    }

                    //iterate from end of page until start of page is reached
                    valuesLoop:
                    while (pageSize > 8) {
                        long read = vol.getLongPackBidiReverse(pageOffset + pageSize);
                        long val = read & DataIO.PACK_LONG_RESULT_MASK;
                        val = longParityGet(val)<<4;
                        //content of Long Stack should be free, so mark it
                        storeCheckMark(b, false, val & MOFFSET, masterSize);

                        //extract number of read bytes
                        pageSize -= read >>> 60;
                    }

                    nextLinkVal = DataIO.parity4Get(
                            vol.getLong(pageOffset));
                }
            }

            /**
             * Iterate over Free Recids an mark them as used
             */

            //iterate over recids
            final long maxRecid = maxRecidGet();


            freeRecidLongStack:
            for (long nextLinkVal = parity4Get(headVol.getLong(FREE_RECID_STACK)); ; ) {

                final long pageOffset = nextLinkVal & MOFFSET;

                if (pageOffset == 0)
                    break freeRecidLongStack;

                long currSize = parity4Get(vol.getLong(pageOffset))>>>48;

                //mark this Long Stack Page occupied
                storeCheckMark(b, true, pageOffset, currSize);

                //now read bytes from end of page, until they are zeros
                while (vol.getUnsignedByte(pageOffset + currSize - 1) == 0) {
                    currSize--;
                }

                //iterate from end of page until start of page is reached
                while (currSize > 8) {
                    long read = vol.getLongPackBidiReverse(pageOffset + currSize);
                    long recid = longParityGet(read & DataIO.PACK_LONG_RESULT_MASK);
                    if (recid > maxRecid)
                        throw new AssertionError("Recid too big");

                    long indexVal = vol.getLong(recidToOffset(recid));
                    if(indexVal!=0){
                        indexVal = parity1Get(indexVal);
                        if(indexVal>>>48!=0)
                            throw new AssertionError();
                        if((indexVal&MOFFSET)!=0)
                            throw new AssertionError();
                        if((indexVal&MUNUSED)==0)
                            throw new AssertionError();
                    }

                    //extract number of read bytes
                    currSize -= read >>> 60;
                }

                nextLinkVal = DataIO.parity4Get(
                        vol.getLong(pageOffset));
            }

            recidLoop:
            for (long recid = 1; recid <= maxRecid; recid++) {
                long recidVal = 0;
                try {
                    recidVal = indexValGet(recid);
                } catch (DBException.EngineGetVoid e) {
                }

                storeCheckMark(b,true,recidToOffset(recid), 8);

                linkedRecLoop:
                for(;;) {
                    long offset = recidVal & MOFFSET;
                    long size = round16Up((int) (recidVal >>> 48));

                    if (size == 0) {
                        continue recidLoop;
                    }
                    storeCheckMark(b, true, offset, size);

                    if((recidVal&MLINKED)==0)
                        break linkedRecLoop;

                    recidVal = parity3Get(vol.getLong(offset));
                }
            }
            //mark unused recid before end of current page;
            {
                long offset = recidToOffset(maxRecidGet())+8;
                if (offset % PAGE_SIZE != 0) {
                    //mark rest of this Index Page as used
                    long endOffset = Fun.roundUp(offset, PAGE_SIZE);
                    vol.assertZeroes(offset, endOffset);
                    b.set((int) offset, (int) endOffset);
                }
            }



            indexTableLoop:
            for(long pageOffset:indexPages){
                if(pageOffset==0)
                    continue  indexTableLoop;
                storeCheckMark(b,true, pageOffset,16);
            }

            //mark unused data et EOF
            long lastAllocated = lastAllocatedDataGet();
            if (lastAllocated != 0) {
                storeCheckMark(b, false, lastAllocated, Fun.roundUp(lastAllocated, PAGE_SIZE)-lastAllocated);
            }

            //assert that all data are accounted for
            for (int offset = 0; offset < storeSize; offset++) {
                if (!b.get(offset))
                    throw new AssertionError("zero at " + offset + " - "+lastAllocatedDataGet());
            }
        }finally {
            structuralLock.unlock();
        }
    }

    private void storeCheckMark(BitSet b, boolean used, long pageOffset, long pageSize) {
        //check it was not previously marked by something else, there could be cyclic reference otherwise etc
        for(int o= (int) pageOffset;o<pageOffset+pageSize;o++){
            if(b.get(o))
                throw new AssertionError("Offset is marked twice: "+o);
        }
        b.set((int)pageOffset, (int)(pageOffset+pageSize),true);

        if(!used){
            //this section is not used, so should be zero
            vol.assertZeroes(pageOffset, pageOffset+pageSize);
        }
    }

    /** will try to close opened files. If an exception is thrown, it is logged and ignored */
    protected void closeFilesIgnoreException() {
        try {
            if (vol != null && !vol.isClosed()) {
                vol.close();
                vol = null;
                headVol = null;
            }
        }catch(Exception e){
            LOG.log(Level.WARNING, "Could not close file: " + fileName, e);
        }
    }

    void assertZeroes(long startOffset, long endOffset) {
        vol.assertZeroes(startOffset, endOffset);
    }



    protected void maxRecidSet(long maxRecid) {
        headVol.putLong(MAX_RECID_OFFSET, parity1Set(maxRecid * 8));
    }

    protected long maxRecidGet(){
        long val = parity1Get(headVol.getLong(MAX_RECID_OFFSET));
        if(CC.ASSERT && val%8!=0)
            throw new DBException.DataCorruption();
        return val/8;
    }

    protected void lastAllocatedDataSet(long offset){
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.ASSERT && offset%PAGE_SIZE==0 && offset>0)
            throw new AssertionError();

        headVol.putLong(LAST_PHYS_ALLOCATED_DATA_OFFSET,parity3Set(offset));
    }

    protected long lastAllocatedDataGet(){
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        return parity3Get(headVol.getLong(LAST_PHYS_ALLOCATED_DATA_OFFSET));
    }

}
