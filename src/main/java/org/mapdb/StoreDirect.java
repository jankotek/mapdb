package org.mapdb;

import java.io.DataInput;
import java.io.File;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import static org.mapdb.DataIO.*;

public class StoreDirect extends Store {

    /** 4 byte file header */
    protected static final int HEADER = 234243482;

    /** 2 byte store version*/
    protected static final short STORE_VERSION = 10000;


    protected static final long PAGE_SIZE = 1<< CC.VOLUME_PAGE_SHIFT;
    protected static final long PAGE_MASK = PAGE_SIZE-1;
    protected static final long PAGE_MASK_INVERSE = 0xFFFFFFFFFFFFFFFFL<<CC.VOLUME_PAGE_SHIFT;

    protected static final long PAGE_SIZE_M16 = PAGE_SIZE-16;

    protected static final long MOFFSET = 0x0000FFFFFFFFFFF0L;

    protected static final long MLINKED = 0x8L;
    protected static final long MUNUSED = 0x4L;
    protected static final long MARCHIVE = 0x2L;
    protected static final long MPARITY = 0x1L;


    protected static final long HEAD_CHECKSUM = 4;
    protected static final long FORMAT_FEATURES = 8*1;
    protected static final long STORE_SIZE = 8*2;
    /** offset of maximal allocated recid. It is {@code <<3 parity1}*/
    protected static final long MAX_RECID_OFFSET = 8*3;
    protected static final long LAST_PHYS_ALLOCATED_DATA_OFFSET = 8*4; //TODO update doc
    protected static final long INDEX_PAGE = 8*5;
    protected static final long FREE_RECID_STACK = 8*6;


    protected static final int MAX_REC_SIZE = 0xFFFF;
    /** number of free physical slots */
    protected static final int SLOTS_COUNT = 5+(MAX_REC_SIZE)/16;
        //TODO check exact number of slots +5 is just to be sure

    protected static final long HEAD_END = INDEX_PAGE + SLOTS_COUNT * 8;

    protected static final long INITCRC_INDEX_PAGE = 4329042389490239043L;

    private static final long[] EMPTY_LONGS = new long[0];


    //TODO this refs are swapped during compaction. Investigate performance implications
    protected volatile Volume vol;
    protected volatile Volume headVol;

    //TODO this only grows under structural lock, but reads are outside structural lock, does it have to be volatile?
    protected volatile long[] indexPages;

    protected volatile long lastAllocatedData=0; //TODO this is under structural lock, does it have to be volatile?

    protected final ScheduledExecutorService executor;

    public StoreDirect(String fileName,
                       Fun.Function1<Volume, String> volumeFactory,
                       Cache cache,
                       int lockScale,
                       int lockingStrategy,
                       boolean checksum,
                       boolean compress,
                       byte[] password,
                       boolean readonly,
                       int freeSpaceReclaimQ,
                       boolean commitFileSyncDisable,
                       int sizeIncrement,
                       ScheduledExecutorService executor
                       ) {
        super(fileName,volumeFactory, cache, lockScale, lockingStrategy, checksum,compress,password,readonly);
        this.vol = volumeFactory.run(fileName);
        this.executor = executor;
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
        }finally {
            commitLock.unlock();
        }
    }

    protected void initOpen() {
        if(CC.PARANOID && !commitLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        //TODO header
        //TODO feature bit field
        initHeadVol();
        //check head checksum
        int expectedChecksum = vol.getInt(HEAD_CHECKSUM);
        int actualChecksum = headChecksum(vol);
        if (actualChecksum != expectedChecksum) {
            throw new DBException.HeadChecksumBroken();
        }

        //load index pages
        long[] ip = new long[]{0};
        long indexPage = parity16Get(vol.getLong(INDEX_PAGE));
        int i=1;
        for(;indexPage!=0;i++){
            if(CC.PARANOID && indexPage%PAGE_SIZE!=0)
                throw new AssertionError();
            if(ip.length==i){
                ip = Arrays.copyOf(ip, ip.length * 4);
            }
            ip[i] = indexPage;
            //checksum
            if(CC.STORE_INDEX_CRC){
                long res = INITCRC_INDEX_PAGE;
                for(long j=0;j<PAGE_SIZE-8;j+=8){
                    res+=vol.getLong(indexPage+j);
                }
                if(res!=vol.getLong(indexPage+PAGE_SIZE-8)) {
                    //throw new InternalError("Page CRC error at offset: "+indexPage);
                    throw new DBException.ChecksumBroken();
                }
            }

            //move to next page
            indexPage = parity16Get(vol.getLong(indexPage+PAGE_SIZE_M16));
        }
        indexPages = Arrays.copyOf(ip,i);
        lastAllocatedData = parity3Get(vol.getLong(LAST_PHYS_ALLOCATED_DATA_OFFSET));
    }

    protected void initCreate() {
        if(CC.PARANOID && !commitLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        //create initial structure

        //create new store
        indexPages = new long[]{0};

        vol.ensureAvailable(PAGE_SIZE);
        vol.clear(0, PAGE_SIZE);

        //set sizes
        vol.putLong(STORE_SIZE, parity16Set(PAGE_SIZE));
        vol.putLong(MAX_RECID_OFFSET, parity3Set(RECID_LAST_RESERVED * 8));
        vol.putLong(INDEX_PAGE, parity16Set(0));

        lastAllocatedData = 0L;
        vol.putLong(LAST_PHYS_ALLOCATED_DATA_OFFSET,parity3Set(lastAllocatedData));

        //put reserved recids
        for(long recid=1;recid<RECID_FIRST;recid++){
            vol.putLong(recidToOffset(recid),parity1Set(MLINKED | MARCHIVE));
        }

        //put long stack master links
        for(long masterLinkOffset = FREE_RECID_STACK;masterLinkOffset<HEAD_END;masterLinkOffset+=8){
            vol.putLong(masterLinkOffset,parity4Set(0));
        }

        //and set header checksum
        vol.putInt(HEAD_CHECKSUM, headChecksum(vol));
        vol.sync();
        initHeadVol();
    }


    protected void initHeadVol() {
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        this.headVol = vol;
    }

    public StoreDirect(String fileName) {
        this(fileName,
                fileName==null? Volume.memoryFactory() : Volume.fileFactory(),
                null,
                CC.DEFAULT_LOCK_SCALE,
                0,
                false,false,null,false,0,
                false,0,
                null);
    }

    protected int headChecksum(Volume vol2) {
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        int ret = 0;
        for(int offset = 8;offset<HEAD_END;offset+=8){
            //TODO include some recids in checksum
            ret = ret*31 + DataIO.longHash(vol2.getLong(offset));
            ret = ret*31 + DataIO.intHash(offset);
        }
        return ret;
    }

    @Override
    protected <A> A get2(long recid, Serializer<A> serializer) {
        if (CC.PARANOID)
            assertReadLocked(recid);

        long[] offsets = offsetsGet(recid);
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
            if(CC.PARANOID && (size&0xFFFF)!=size)
                throw new AssertionError("size mismatch");
            long offset = offsets[i] & MOFFSET;
            //System.out.println("GET "+(offset + plus)+ " - "+size+" - "+bpos);
            vol.getData(offset + plus, b, bpos, (int) size);
            bpos += size;
        }
        if (CC.PARANOID && bpos != totalSize)
            throw new AssertionError("size does not match");
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
        if(CC.PARANOID)
            assertWriteLocked(lockPos(recid));

        long[] oldOffsets = offsetsGet(recid);
        int oldSize = offsetsTotalSize(oldOffsets);
        int newSize = out==null?0:out.pos;
        long[] newOffsets;

        //if new version fits into old one, reuse space
        if(oldSize==newSize){
            //TODO more precise check of linked records
            //TODO check rounUp 16 for non-linked records
            newOffsets = oldOffsets;
        }else {
            structuralLock.lock();
            try {
                if(oldOffsets!=null)
                    freeDataPut(oldOffsets);
                newOffsets = newSize==0?null:freeDataTake(out.pos);

            } finally {
                structuralLock.unlock();
            }
        }

        if(CC.PARANOID)
            offsetsVerify(newOffsets);

        putData(recid, newOffsets, out==null?null:out.buf, out==null?0:out.pos);
    }

    protected void offsetsVerify(long[] linkedOffsets) {
        //TODO check non tail records are mod 16
        //TODO check linkage
    }


    /** return positions of (possibly) linked record */
    protected long[] offsetsGet(long recid) {
        long indexVal = indexValGet(recid);
        if(indexVal>>>48==0){

            return ((indexVal&MLINKED)!=0) ? null : EMPTY_LONGS;
        }

        long[] ret = new long[]{indexVal};
        while((ret[ret.length-1]&MLINKED)!=0){
            ret = Arrays.copyOf(ret,ret.length+1);
            ret[ret.length-1] = parity3Get(vol.getLong(ret[ret.length-2]&MOFFSET));
        }

        if(CC.PARANOID){
            for(int i=0;i<ret.length;i++) {
                boolean last = (i==ret.length-1);
                boolean linked = (ret[i]&MLINKED)!=0;
                if(!last && !linked)
                    throw new AssertionError("body not linked");
                if(last && linked)
                    throw new AssertionError("tail is linked");

                long offset = ret[i]&MOFFSET;
                if(offset<PAGE_SIZE)
                    throw new AssertionError("offset is too small");
                if(((offset&MOFFSET)%16)!=0)
                    throw new AssertionError("offset not mod 16");

                int size = (int) (ret[i] >>>48);
                if(size<=0)
                    throw new AssertionError("size too small");
            }

        }

        return ret;
    }

    protected void indexValPut(long recid, int size, long offset, boolean linked, boolean unused) {
        if(CC.PARANOID)
            assertWriteLocked(lockPos(recid));

        long indexOffset = recidToOffset(recid);
        long newval = composeIndexVal(size,offset,linked,unused,true);
        if(CC.STORE_INDEX_CRC){
            //update crc by substracting old value and adding new value
            long oldval = vol.getLong(indexOffset);
            long crcOffset = (indexOffset&PAGE_MASK_INVERSE)+PAGE_SIZE-8;
            //TODO crc at end of zero page?
            if(CC.PARANOID && crcOffset<HEAD_END)
                throw new AssertionError();
            long crc = vol.getLong(crcOffset);
            crc-=oldval;
            crc+=newval;
            vol.putLong(crcOffset,crc);
        }
        vol.putLong(indexOffset, newval);
    }


    @Override
    protected <A> void delete2(long recid, Serializer<A> serializer) {
        if(CC.PARANOID)
            assertWriteLocked(lockPos(recid));

        long[] offsets = offsetsGet(recid);
        if(offsets!=null) {
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
        if(CC.PARANOID && offsets!=null && (offsets[0]&MOFFSET)<PAGE_SIZE)
            throw new AssertionError();

        int lockPos = lockPos(recid);
        Lock lock = locks[lockPos].writeLock();
        lock.lock();
        try {
            if(caches!=null) {
                caches[lockPos].put(recid, value);
            }
            putData(recid, offsets, out==null?null:out.buf, out==null?0:out.pos);
        }finally {
            lock.unlock();
        }

        return recid;
    }

    protected void putData(long recid, long[] offsets, byte[] src, int srcLen) {
        if(CC.PARANOID)
            assertWriteLocked(lockPos(recid));
        if(CC.PARANOID && offsetsTotalSize(offsets)!=(src==null?0:srcLen))
            throw new AssertionError("size mismatch");

        if(offsets!=null) {
            int outPos = 0;
            for (int i = 0; i < offsets.length; i++) {
                final boolean last = (i == offsets.length - 1);
                if (CC.PARANOID && ((offsets[i] & MLINKED) == 0) != last)
                    throw new AssertionError("linked bit set wrong way");

                long offset = (offsets[i] & MOFFSET);
                if(CC.PARANOID && offset%16!=0)
                    throw new AssertionError("not alligned to 16");

                int plus = (last?0:8);
                int size = (int) ((offsets[i]>>>48) - plus);
                if(CC.PARANOID && ((size&0xFFFF)!=size || size==0))
                    throw new AssertionError("size mismatch");

                int segment = lockPos(recid);
                //write offset to next page
                if (!last) {
                    putDataSingleWithLink(segment, offset,parity3Set(offsets[i + 1]), src,outPos,size);
                }else{
                    putDataSingleWithoutLink(segment, offset, src, outPos, size);
                }
                outPos += size;

            }
            if(CC.PARANOID && outPos!=srcLen)
                throw new AssertionError("size mismatch");
        }
        //update index val
        boolean firstLinked =
                (offsets!=null && offsets.length>1) || //too large record
                (src==null); //null records
        boolean empty = offsets==null || offsets.length==0;
        int firstSize = (int) (empty ? 0L : offsets[0]>>>48);
        long firstOffset =  empty? 0L : offsets[0]&MOFFSET;
        indexValPut(recid,firstSize,firstOffset,firstLinked,false);
    }

    protected void putDataSingleWithoutLink(int segment, long offset, byte[] buf, int bufPos, int size) {
        vol.putData(offset,buf,bufPos,size);
    }

    protected void putDataSingleWithLink(int segment, long offset, long link, byte[] buf, int bufPos, int size) {
        vol.putLong(offset,link);
        vol.putData(offset+8, buf,bufPos,size);
    }

    protected void freeDataPut(long[] linkedOffsets) {
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        for(long v:linkedOffsets){
            int size = round16Up((int) (v >>> 48));
            v &= MOFFSET;
            freeDataPut(v,size);
        }
    }


    protected void freeDataPut(long offset, int size) {
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.PARANOID && size%16!=0 )
            throw new AssertionError();
        if(CC.PARANOID && (offset%16!=0 || offset<PAGE_SIZE))
            throw new AssertionError();

        if(!(this instanceof  StoreWAL)) //TODO WAL needs to handle record clear, perhaps WAL instruction?
            vol.clear(offset,offset+size);

        //shrink store if this is last record
        if(offset+size==lastAllocatedData){
            lastAllocatedData-=size;
            return;
        }

        long masterPointerOffset = size/2 + FREE_RECID_STACK; // really is size*8/16
        longStackPut(masterPointerOffset, offset, false);
    }


    protected long[] freeDataTake(int size) {
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.PARANOID && size<=0)
            throw new AssertionError();

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
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.PARANOID && size%16!=0)
            throw new AssertionError();
        if(CC.PARANOID && size>round16Up(MAX_REC_SIZE))
            throw new AssertionError();

        long masterPointerOffset = size/2 + FREE_RECID_STACK; // really is size*8/16
        long ret = longStackTake(masterPointerOffset,false);
        if(ret!=0) {
            if(CC.PARANOID && ret<PAGE_SIZE)
                throw new AssertionError();
            if(CC.PARANOID && ret%16!=0)
                throw new AssertionError();

            return ret;
        }

        if(lastAllocatedData==0){
            //allocate new data page
            long page = pageAllocate();
            lastAllocatedData = page+size;
            if(CC.PARANOID && page<PAGE_SIZE)
                throw new AssertionError();
            if(CC.PARANOID && page%16!=0)
                throw new AssertionError();
            return page;
        }

        //does record fit into rest of the page?
        if((lastAllocatedData%PAGE_SIZE + size)/PAGE_SIZE !=0){
            //throw away rest of the page and allocate new
            lastAllocatedData=0;
            freeDataTakeSingle(size);
        }
        //yes it fits here, increase pointer
        ret = lastAllocatedData;
        lastAllocatedData+=size;

        if(CC.PARANOID && ret%16!=0)
            throw new AssertionError();
        if(CC.PARANOID && lastAllocatedData%16!=0)
            throw new AssertionError();
        if(CC.PARANOID && ret<PAGE_SIZE)
            throw new AssertionError();

        return ret;
    }


    //TODO use var size
    protected final static long CHUNKSIZE = 100*16;

    protected void longStackPut(final long masterLinkOffset, final long value, boolean recursive){
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.PARANOID && (masterLinkOffset<=0 || masterLinkOffset>PAGE_SIZE || masterLinkOffset % 8!=0))
            throw new AssertionError();

        long masterLinkVal = parity4Get(headVol.getLong(masterLinkOffset));
        long pageOffset = masterLinkVal&MOFFSET;

        if(masterLinkVal==0L){
            longStackNewPage(masterLinkOffset, 0L, value);
            return;
        }

        long currSize = masterLinkVal>>>48;

        long prevLinkVal = parity4Get(vol.getLong(pageOffset + 4));
        long pageSize = prevLinkVal>>>48;
        //is there enough space in current page?
        if(currSize+8>=pageSize){
            //no there is not enough space
            //first zero out rest of the page
            vol.clear(pageOffset+currSize, pageOffset+pageSize);
            //allocate new page
            longStackNewPage(masterLinkOffset,pageOffset,value);
            return;
        }

        //there is enough space, so just write new value
        currSize += vol.putLongPackBidi(pageOffset+currSize,parity1Set(value<<1));
        //and update master pointer
        headVol.putLong(masterLinkOffset, parity4Set(currSize<<48 | pageOffset));
    }

    protected void longStackNewPage(long masterLinkOffset, long prevPageOffset, long value) {
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        long newPageOffset = freeDataTakeSingle((int) CHUNKSIZE);
        //write size of current chunk with link to prev page
        vol.putLong(newPageOffset+4, parity4Set((CHUNKSIZE<<48) | prevPageOffset));
        //put value
        long currSize = 12 + vol.putLongPackBidi(newPageOffset+12, parity1Set(value<<1));
        //update master pointer
        headVol.putLong(masterLinkOffset, parity4Set((currSize<<48)|newPageOffset));
    }


    protected long longStackTake(long masterLinkOffset, boolean recursive){
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.PARANOID && (masterLinkOffset<FREE_RECID_STACK ||
                masterLinkOffset>FREE_RECID_STACK+round16Up(MAX_REC_SIZE)/2 ||
                masterLinkOffset % 8!=0))
            throw new AssertionError();

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
        ret = parity1Get(ret &DataIO.PACK_LONG_BIDI_MASK)>>>1;

        if(CC.PARANOID && currSize<12)
            throw new AssertionError();

        //is there space left on current page?
        if(currSize>12){
            //yes, just update master link
            headVol.putLong(masterLinkOffset, parity4Set(currSize << 48 | pageOffset));
            return ret;
        }

        //there is no space at current page, so delete current page and update master pointer
        long prevPageOffset = parity4Get(vol.getLong(pageOffset + 4));
        final int currPageSize = (int) (prevPageOffset>>>48);
        prevPageOffset &= MOFFSET;

        //does previous page exists?
        if(prevPageOffset!=0) {
            //yes previous page exists

            //find pointer to end of previous page
            // (data are packed with var size, traverse from end of page, until zeros

            //first read size of current page
            currSize = parity4Get(vol.getLong(prevPageOffset + 4)) >>> 48;

            //now read bytes from end of page, until they are zeros
            while (vol.getUnsignedByte(prevPageOffset + currSize-1) == 0) {
                currSize--;
            }

            if (CC.PARANOID && currSize < 14)
                throw new AssertionError();
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
        commitLock.lock();
        try {
            closed = true;
            flush();
            vol.close();
            vol = null;

            if (caches != null) {
                for (Cache c : caches) {
                    c.close();
                }
                Arrays.fill(caches,null);
            }

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
    public boolean canSnapshot() {
        return false;
    }

    @Override
    public Engine snapshot() throws UnsupportedOperationException {
        return null;
    }

    @Override
    public void clearCache() {

    }

    @Override
    public void compact() {
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


                final long maxRecidOffset = parity3Get(headVol.getLong(MAX_RECID_OFFSET));

                String compactedFile = vol.getFile()==null? null : fileName+".compact";
                final StoreDirect target = new StoreDirect(compactedFile,
                        volumeFactory,
                        null,lockScale,
                        executor==null?LOCKING_STRATEGY_NOLOCK:LOCKING_STRATEGY_WRITELOCK,
                        checksum,compress,null,false,0,false,0,
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

                    target.vol.putLong(MAX_RECID_OFFSET, parity3Set(maxRecid.get() * 8));
                    this.indexPages = target.indexPages;
                    this.lastAllocatedData = target.lastAllocatedData;


                    //compaction done, swap target with current
                    if(compactedFile==null) {
                        //in memory vol without file, just swap everything
                        Volume oldVol = this.vol;
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
                            throw new AssertionError("failed to rename file "+currFile);
                        }

                        //rename compacted file to current file
                        if(!compactedFileF.renameTo(currFile)) {
                            //TODO recovery here.
                            throw new AssertionError("failed to rename file " + compactedFileF);
                        }

                        //and reopen volume
                        this.headVol = this.vol = volumeFactory.run(this.fileName);

                        if(isStoreCached){
                            ((StoreCached)this).dirtyStackPages.clear();
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
        long recid = (indexPageI==0? 0 : indexPageI * PAGE_SIZE/8 - HEAD_END/8);
        final long indexPageStart = (indexPage==0?HEAD_END+8 : indexPage);
        final long indexPageEnd = indexPage+PAGE_SIZE_M16;

        //iterate over indexOffset values
        //TODO check if preloading and caching of all indexVals on this index page would improve performance
        indexVal:
        for( long indexOffset=indexPageStart;
                indexOffset<indexPageEnd;
                indexOffset+=8){
            recid++;

            if(recid*8>maxRecidOffset)
                break indexVal;

            //update maxRecid in thread safe way
            for(long oldMaxRecid=maxRecid.get();
                !maxRecid.compareAndSet(oldMaxRecid, Math.max(recid,oldMaxRecid));
                oldMaxRecid=maxRecid.get()){
            }

            final long indexVal = vol.getLong(indexOffset);


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
                long[] offsets = offsetsGet(recid);
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
                throw new AssertionError();

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
        long indexVal = vol.getLong(recidToOffset(recid));
        if(indexVal == 0)
            throw new DBException.EngineGetVoid();
        //check parity and throw recid does not exist if broken
        return DataIO.parity1Get(indexVal);
    }

    protected final long recidToOffset(long recid){
        if(CC.PARANOID && recid<=0)
            throw new AssertionError("negative recid: "+recid);
        recid = recid * 8 + HEAD_END;
        //TODO add checksum to beginning of each page
        return indexPages[((int) (recid / PAGE_SIZE_M16))] + //offset of index page
                (recid % PAGE_SIZE_M16); // offset on page
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
        if(CC.PARANOID && (size&0xFFFF)!=size)
            throw new AssertionError("size too large");
        if(CC.PARANOID && (offset&MOFFSET)!=offset)
            throw new AssertionError("offset too large");
        offset = (((long)size)<<48) |
                offset |
                (linked?MLINKED:0L)|
                (unused?MUNUSED:0L)|
                (archive?MARCHIVE:0L);
        return parity1Set(offset);
    }


    /** returns new recid, recid slot is allocated and ready to use */
    protected long freeRecidTake() {
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        //try to reuse recid from free list
        long currentRecid = longStackTake(FREE_RECID_STACK,false);
        if(currentRecid!=0)
            return currentRecid;

        currentRecid = parity3Get(headVol.getLong(MAX_RECID_OFFSET));
        currentRecid+=8;
        headVol.putLong(MAX_RECID_OFFSET, parity3Set(currentRecid));

        currentRecid/=8;
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
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        //convert recid into Index Page number
        recid = recid * 8 + HEAD_END;
        recid = recid / PAGE_SIZE_M16;

        while(indexPages.length<=recid)
            pageIndexExtend();
    }

    protected void pageIndexExtend() {
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        //allocate new index page
        long indexPage = pageAllocate();

        //add link to previous page
        if(indexPages.length==1){
            //first index page
            headVol.putLong(INDEX_PAGE, parity16Set(indexPage));
        }else{
            //update link on previous page
            long nextPagePointerOffset = indexPages[indexPages.length-1]+PAGE_SIZE_M16;
            indexLongPut(nextPagePointerOffset, parity16Set(indexPage));
            if(CC.STORE_INDEX_CRC){
                //update crc by increasing crc value
                long crc = vol.getLong(nextPagePointerOffset+8); //TODO read both longs from TX
                crc-=vol.getLong(nextPagePointerOffset);
                crc+=parity16Set(indexPage);
                indexLongPut(nextPagePointerOffset+8,crc);
            }
        }

        //set zero link on next page
        indexLongPut(indexPage+PAGE_SIZE_M16,parity16Set(0));

        //set init crc value on new page
        if(CC.STORE_INDEX_CRC){
            indexLongPut(indexPage+PAGE_SIZE-8,INITCRC_INDEX_PAGE+parity16Set(0));
        }

        //put into index page array
        long[] indexPages2 = Arrays.copyOf(indexPages,indexPages.length+1);
        indexPages2[indexPages.length]=indexPage;
        indexPages = indexPages2;
    }

    protected long pageAllocate() {
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        long storeSize = parity16Get(headVol.getLong(STORE_SIZE));
        vol.ensureAvailable(storeSize+PAGE_SIZE);
        vol.clear(storeSize,storeSize+PAGE_SIZE);
        headVol.putLong(STORE_SIZE, parity16Set(storeSize + PAGE_SIZE));

        if(CC.PARANOID && storeSize%PAGE_SIZE!=0)
            throw new AssertionError();

        return storeSize;
    }

    protected static int round16Up(int pos) {
        int rem = pos&15;  // modulo 16
        if(rem!=0) pos +=16-rem;
        return pos;
    }
}
