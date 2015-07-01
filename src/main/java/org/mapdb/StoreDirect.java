package org.mapdb;

import java.io.DataInput;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;

import static org.mapdb.DataIO.*;

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
    /** offset of maximal allocated recid. It is {@code <<3 parity1}*/
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

    private static final long[] EMPTY_LONGS = new long[0];


    //TODO this refs are swapped during compaction. Investigate performance implications
    protected volatile Volume vol;
    protected volatile Volume headVol;

    //TODO this only grows under structural lock, but reads are outside structural lock, does it have to be volatile?
    protected volatile long[] indexPages;

    protected volatile long lastAllocatedData=0; //TODO this is under structural lock, does it have to be volatile?

    protected final ScheduledExecutorService executor;

    protected final List<Snapshot> snapshots;

    protected final long indexValSize;

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
                       int freeSpaceReclaimQ,
                       boolean commitFileSyncDisable,
                       int sizeIncrement,
                       ScheduledExecutorService executor
                       ) {
        super(fileName,volumeFactory, cache, lockScale, lockingStrategy, checksum,compress,password,readonly, snapshotEnable);
        this.vol = volumeFactory.makeVolume(fileName, readonly);
        this.executor = executor;
        this.snapshots = snapshotEnable?
                new CopyOnWriteArrayList<Snapshot>():
                null;
        this.indexValSize = checksum ? 10 : 8;
    }

    @Override
    public void init() {
        commitLock.lock();
        try {
            structuralLock.lock();
            try {
                if (vol.isEmpty()) {
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

    protected void initOpen() {
        if(CC.ASSERT && !commitLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        int header = vol.getInt(0);
        if(header!=header){
            throw new DBException.WrongConfig("This is not MapDB file");
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
        indexPages = Arrays.copyOf(ip,i);
        lastAllocatedData = parity3Get(vol.getLong(LAST_PHYS_ALLOCATED_DATA_OFFSET));
    }

    protected void initCreate() {
        if(CC.ASSERT && !commitLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        //create initial structure

        //create new store
        indexPages = new long[]{0};

        vol.ensureAvailable(PAGE_SIZE);
        vol.clear(0, PAGE_SIZE);

        //set sizes
        vol.putLong(STORE_SIZE, parity16Set(PAGE_SIZE));
        vol.putLong(MAX_RECID_OFFSET, parity1Set(RECID_LAST_RESERVED * indexValSize));
        //pointer to next index page (zero)
        vol.putLong(HEAD_END, parity16Set(0));

        lastAllocatedData = 0L;
        vol.putLong(LAST_PHYS_ALLOCATED_DATA_OFFSET,parity3Set(lastAllocatedData));

        //put reserved recids
        for(long recid=1;recid<RECID_FIRST;recid++){
            long indexVal = parity1Set(MLINKED | MARCHIVE);
            long indexOffset = recidToOffset(recid);
            vol.putLong(indexOffset, indexVal);
            if(checksum) {
                vol.putUnsignedShort(indexOffset + 8, DataIO.longHash(indexVal)&0xFFFF);
            }
        }

        //put long stack master links
        for(long masterLinkOffset = FREE_RECID_STACK;masterLinkOffset<HEAD_END;masterLinkOffset+=8){
            vol.putLong(masterLinkOffset,parity4Set(0));
        }

        //write header
        vol.putInt(0,HEADER);

        //set features bitmap
        long features = makeFeaturesBitmap();

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
                false,false,null,false,false,0,
                false,0,
                null);
    }

    protected int headChecksum(Volume vol2) {
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        int ret = 0;
        for(int offset = 8;
            offset< HEAD_END;
            offset+=8){
            long val = vol2.getLong(offset);
            ret += DataIO.longHash(offset+val);
        }
        return ret;
    }

    @Override
    protected <A> A get2(long recid, Serializer<A> serializer) {
        if (CC.ASSERT)
            assertReadLocked(recid);

        long[] offsets = offsetsGet(indexValGet(recid));
        return getFromOffset(serializer, offsets);
    }

    protected <A> A getFromOffset(Serializer<A> serializer, long[] offsets) {
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

        long[] oldOffsets = offsetsGet(oldIndexVal);
        int oldSize = offsetsTotalSize(oldOffsets);
        int newSize = out==null?0:out.pos;
        long[] newOffsets;

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

        putData(recid, newOffsets, out==null?null:out.buf, out==null?0:out.pos);
    }

    protected void offsetsVerify(long[] linkedOffsets) {
        //TODO check non tail records are mod 16
        //TODO check linkage
    }


    /** return positions of (possibly) linked record */
    protected long[] offsetsGet(long indexVal) {;
        if(indexVal>>>48==0){

            return ((indexVal&MLINKED)!=0) ? null : EMPTY_LONGS;
        }

        long[] ret = new long[]{indexVal};
        while((ret[ret.length-1]&MLINKED)!=0){
            ret = Arrays.copyOf(ret,ret.length+1);
            ret[ret.length-1] = parity3Get(vol.getLong(ret[ret.length-2]&MOFFSET));
        }

        if(CC.ASSERT){
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

        return ret;
    }

    protected void indexValPut(long recid, int size, long offset, boolean linked, boolean unused) {
        if(CC.ASSERT)
            assertWriteLocked(lockPos(recid));

        long indexOffset = recidToOffset(recid);
        long newval = composeIndexVal(size, offset, linked, unused, true);
        vol.putLong(indexOffset, newval);
        if(checksum){
            vol.putUnsignedShort(indexOffset+8, DataIO.longHash(newval)&0xFFFF);
        }
    }


    @Override
    protected <A> void delete2(long recid, Serializer<A> serializer) {
        if(CC.ASSERT)
            assertWriteLocked(lockPos(recid));

        long oldIndexVal = indexValGet(recid);
        long[] offsets = offsetsGet(oldIndexVal);
        boolean releaseOld = true;
        if(snapshotEnable){
            int pos = lockPos(recid);
            for(Snapshot snap:snapshots){
                snap.oldRecids[pos].putIfAbsent(recid,oldIndexVal);
                releaseOld = false;
            }
        }

        if(offsets!=null && releaseOld) {
            structuralLock.lock();
            try {
                freeDataPut(offsets);
            } finally {
                structuralLock.unlock();
            }
        }
        indexValPut(recid,0,0,true,true);
    }

    @Override
    public long getCurrSize() {
        return vol.length() - lastAllocatedData % CHUNKSIZE;
    }

    @Override
    public long getFreeSize() {
        return -1; //TODO freesize
    }

    @Override
    public long preallocate() {
        long recid;
        structuralLock.lock();
        try {
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
        return recid;
    }


    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        long recid;
        long[] offsets;
        DataOutputByteArray out = serialize(value,serializer);
        boolean notalloc = out==null || out.pos==0;
        structuralLock.lock();
        try {
            recid = freeRecidTake();
            offsets = notalloc?null:freeDataTake(out.pos);
        }finally {
            structuralLock.unlock();
        }
        if(CC.ASSERT && offsets!=null && (offsets[0]&MOFFSET)<PAGE_SIZE)
            throw new DBException.DataCorruption();

        int pos = lockPos(recid);
        Lock lock = locks[pos].writeLock();
        lock.lock();
        try {
            if(caches!=null) {
                caches[pos].put(recid, value);
            }
            if(snapshotEnable){
                for(Snapshot snap:snapshots){
                    snap.oldRecids[pos].putIfAbsent(recid,0);
                }
            }

            putData(recid, offsets, out==null?null:out.buf, out==null?0:out.pos);
        }finally {
            lock.unlock();
        }

        return recid;
    }

    protected void putData(long recid, long[] offsets, byte[] src, int srcLen) {
        if(CC.ASSERT)
            assertWriteLocked(lockPos(recid));
        if(CC.ASSERT && offsetsTotalSize(offsets)!=(src==null?0:srcLen))
            throw new DBException.DataCorruption("size mismatch");

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
        vol.putData(offset,buf,bufPos,size);
    }

    protected void putDataSingleWithLink(int segment, long offset, long link, byte[] buf, int bufPos, int size) {
        vol.putLong(offset,link);
        vol.putData(offset+8, buf,bufPos,size);
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

        if(!(this instanceof  StoreWAL)) //TODO WAL needs to handle record clear, perhaps WAL instruction?
            vol.clear(offset,offset+size);

        //shrink store if this is last record
        if(offset+size==lastAllocatedData){
            lastAllocatedData-=size;
            return;
        }

        long masterPointerOffset = size/2 + FREE_RECID_STACK; // really is size*8/16
        longStackPut(
                masterPointerOffset,
                offset>>>4, //offset is multiple of 16, save some space
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
        return ret;
    }

    protected long freeDataTakeSingle(int size) {
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.ASSERT && size%16!=0)
            throw new DBException.DataCorruption("unalligned size");
        if(CC.ASSERT && size>round16Up(MAX_REC_SIZE))
            throw new DBException.DataCorruption("size too big");

        long masterPointerOffset = size/2 + FREE_RECID_STACK; // really is size*8/16
        long ret = longStackTake(masterPointerOffset,false) <<4; //offset is multiple of 16, save some space
        if(ret!=0) {
            if(CC.ASSERT && ret<PAGE_SIZE)
                throw new DBException.DataCorruption("wrong ret");
            if(CC.ASSERT && ret%16!=0)
                throw new DBException.DataCorruption("unalligned ret");

            return ret;
        }

        if(lastAllocatedData==0){
            //allocate new data page
            long page = pageAllocate();
            lastAllocatedData = page+size;
            if(CC.ASSERT && page<PAGE_SIZE)
                throw new DBException.DataCorruption("wrong page");
            if(CC.ASSERT && page%16!=0)
                throw new DBException.DataCorruption("unalligned page");
            return page;
        }

        //does record fit into rest of the page?
        if((lastAllocatedData%PAGE_SIZE + size)/PAGE_SIZE !=0){
            //throw away rest of the page and allocate new
            lastAllocatedData=0;
            freeDataTakeSingle(size);
            //TODO i thing return! should be here, but not sure.

            //TODO it could be possible to recycle data here.
            // save pointers and put them into free list after new page was allocated.
        }
        //yes it fits here, increase pointer
        ret = lastAllocatedData;
        lastAllocatedData+=size;

        if(CC.ASSERT && ret%16!=0)
            throw new DBException.DataCorruption();
        if(CC.ASSERT && lastAllocatedData%16!=0)
            throw new  DBException.DataCorruption();
        if(CC.ASSERT && ret<PAGE_SIZE)
            throw new  DBException.DataCorruption();

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
        currSize += vol.putLongPackBidi(pageOffset+currSize, longParitySet(value));
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
        long currSize = 8 + vol.putLongPackBidi(newPageOffset+8, longParitySet(value));
        //update master pointer
        headVol.putLong(masterLinkOffset, parity4Set((currSize<<48)|newPageOffset));
    }


    protected long longStackTake(long masterLinkOffset, boolean recursive){
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.ASSERT && (masterLinkOffset<FREE_RECID_STACK ||
                masterLinkOffset>FREE_RECID_STACK+round16Up(MAX_REC_SIZE)/2 ||
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
        currSize-= ret >>>56;
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
            headVol.putLong(LAST_PHYS_ALLOCATED_DATA_OFFSET, parity3Set(lastAllocatedData));
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
    public void compact() {
        //check for some file used during compaction, if those exists, refuse to compact
        if(compactOldFilesExists()){
            return;
        }

        final boolean isStoreCached = this instanceof StoreCached;
        for(int i=0;i<locks.length;i++){
            Lock lock = isStoreCached?locks[i].readLock():locks[i].writeLock();
            lock.lock();
        }

        try{
            commitLock.lock();
            try {

                //clear caches, so freed recids throw an exception, instead of returning null
                if(caches!=null) {
                    for (Cache c : caches) {
                        c.clear();
                    }
                }
                snapshotCloseAllOnCompact();


                final long maxRecidOffset = parity1Get(headVol.getLong(MAX_RECID_OFFSET));

                String compactedFile = vol.getFile()==null? null : fileName+".compact";
                final StoreDirect target = new StoreDirect(compactedFile,
                        volumeFactory,
                        null,lockScale,
                        executor==null?LOCKING_STRATEGY_NOLOCK:LOCKING_STRATEGY_WRITELOCK,
                        checksum,compress,null,false,false,0,false,0,
                        null);
                target.init();
                final AtomicLong maxRecid = new AtomicLong(RECID_LAST_RESERVED);

                //TODO what about recids which are already in freeRecidLongStack?
                // I think it gets restored by traversing index table,
                // so there is no need to traverse and copy freeRecidLongStack
                // TODO same problem in StoreWAL
                compactIndexPages(maxRecidOffset, target, maxRecid);


                //update some stuff
                structuralLock.lock();
                try {

                    target.vol.putLong(MAX_RECID_OFFSET, parity1Set(maxRecid.get() * indexValSize));
                    this.indexPages = target.indexPages;
                    this.lastAllocatedData = target.lastAllocatedData;


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
                        this.headVol = this.vol = volumeFactory.makeVolume(this.fileName, readonly);

                        if(isStoreCached){
                            ((StoreCached)this).dirtyStackPages.clear();
                        }

                        //delete old file
                        if(!currFileRenamed.delete()){
                            LOG.warning("Could not delete old compaction file: "+currFileRenamed);
                        }

                    }
                }finally {
                    structuralLock.unlock();
                }
            }finally{
                commitLock.unlock();
            }
        }finally {
            for(int i=locks.length-1;i>=0;i--) {
                Lock lock = isStoreCached ? locks[i].readLock() : locks[i].writeLock();
                lock.unlock();
            }
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

    protected void compactIndexPages(final long maxRecidOffset, final StoreDirect target, final AtomicLong maxRecid) {
        //iterate over index pages
        if(executor == null) {
            for (int indexPageI = 0; indexPageI < indexPages.length; indexPageI++) {
                compactIndexPage(maxRecidOffset, target, maxRecid, indexPageI);
            }
        }else {
            //compact pages in multiple threads.
            //there are N tasks (index pages) running in parallel.
            //main thread checks number of tasks in interval, if one is finished it will
            //schedule next one
            final List<Future> tasks = new ArrayList();
            for (int indexPageI = 0; indexPageI < indexPages.length; indexPageI++) {
                final int indexPageI2 = indexPageI;
                //now submit tasks to executor, it will compact single page
                //TODO handle RejectedExecutionException?
                Future f = executor.submit(new Runnable() {
                    @Override
                    public void run() {
                      compactIndexPage(maxRecidOffset, target, maxRecid, indexPageI2);
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

    protected void compactIndexPage(long maxRecidOffset, StoreDirect target, AtomicLong maxRecid, int indexPageI) {
        final long indexPage = indexPages[indexPageI];

        long recid = (indexPageI==0? 0 : indexPageI * PAGE_SIZE/indexValSize - HEAD_END/indexValSize);
        final long indexPageStart = (indexPage==0?HEAD_END+8 : indexPage);
        final long indexPageEnd = indexPage+PAGE_SIZE;

        //iterate over indexOffset values
        //TODO check if preloading and caching of all indexVals on this index page would improve performance
        indexVal:
        for( long indexOffset=indexPageStart;
                indexOffset<indexPageEnd;
                indexOffset+= indexValSize){
            recid++;

            if(CC.ASSERT && indexOffset!=recidToOffset(recid))
                throw new AssertionError();

            if(recid*indexValSize>maxRecidOffset)
                break indexVal;

            //update maxRecid in thread safe way
            for(long oldMaxRecid=maxRecid.get();
                !maxRecid.compareAndSet(oldMaxRecid, Math.max(recid,oldMaxRecid));
                oldMaxRecid=maxRecid.get()){
            }

            final long indexVal = vol.getLong(indexOffset);
            if(checksum &&
                    vol.getUnsignedShort(indexOffset+8)!=
                            (DataIO.longHash(indexVal)&0xFFFF)){
                throw new DBException.ChecksumBroken();
            }

            //check if was discarted
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
                long[] offsets = offsetsGet(indexValGet(recid));
                int totalSize = offsetsTotalSize(offsets);
                byte[] b = getLoadLinkedRecord(offsets, totalSize);

                //now put into new store, ecquire locks
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
        long offset = recidToOffset(recid);
        long indexVal = vol.getLong(offset);
        if(indexVal == 0)
            throw new DBException.EngineGetVoid();
        if(checksum){
            int checksum = vol.getUnsignedShort(offset+8);
            if(checksum!=(DataIO.longHash(indexVal)&0xFFFF)){
                throw new DBException.ChecksumBroken();
            }
        }
        //check parity and throw recid does not exist if broken
        return DataIO.parity1Get(indexVal);
    }

    protected final long recidToOffset(long recid){
        if(CC.ASSERT && recid<=0)
            throw new DBException.DataCorruption("negative recid: "+recid);
        if(checksum){
            return recidToOffsetChecksum(recid);
        }

        //convert recid to offset
        recid = (recid-1) * indexValSize + HEAD_END + 8;

        recid+= Math.min(1, recid/PAGE_SIZE)*    //if(recid>=PAGE_SIZE)
                (8 + ((recid-PAGE_SIZE)/(PAGE_SIZE-8))*8);

        //look up real offset
        recid = indexPages[((int) (recid / PAGE_SIZE))] + recid%PAGE_SIZE;
        return recid;
    }

    private long recidToOffsetChecksum(long recid) {
        //convert recid to offset
        recid = (recid-1) * indexValSize + HEAD_END + 8;

        if(recid+ indexValSize >PAGE_SIZE){
            //align from zero page
            recid+=2+8;
        }

        //align for every other page
        //TODO optimize away loop
        for(long page=PAGE_SIZE*2;recid+ indexValSize >page;page+=PAGE_SIZE){
            recid+=8+(PAGE_SIZE-8)% indexValSize;
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
        if(currentRecid!=0)
            return currentRecid;

        currentRecid = parity1Get(headVol.getLong(MAX_RECID_OFFSET));
        currentRecid+=indexValSize;
        headVol.putLong(MAX_RECID_OFFSET, parity1Set(currentRecid));

        currentRecid/=indexValSize;
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
        recid = recid * indexValSize + HEAD_END;
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
        indexLongPut(indexPage,parity16Set(0));

        //put into index page array
        long[] indexPages2 = Arrays.copyOf(indexPages,indexPages.length+1);
        indexPages2[indexPages.length]=indexPage;
        indexPages = indexPages2;
    }

    protected long pageAllocate() {
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        long storeSize = parity16Get(headVol.getLong(STORE_SIZE));
        vol.ensureAvailable(storeSize+PAGE_SIZE);
        vol.clear(storeSize,storeSize+PAGE_SIZE);
        headVol.putLong(STORE_SIZE, parity16Set(storeSize + PAGE_SIZE));

        if(CC.ASSERT && storeSize%PAGE_SIZE!=0)
            throw new DBException.DataCorruption();

        return storeSize;
    }

    protected static int round16Up(int pos) {
        //TODO optimize this, no conditions
        int rem = pos&15;  // modulo 16
        if(rem!=0) pos +=16-rem;
        return pos;
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
                    long[] offsets = engine.offsetsGet(indexVal);
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
}
