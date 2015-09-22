package org.mapdb;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import static org.mapdb.DataIO.*;

/**
 *
 */
public class StoreDirect2 extends Store{

    protected static final long STACK_COUNT = 1+1024*64/16;


    /**
     * Contains current size of store. It is offset beyond which all bytes are zero. Parity 4.
     */
    protected static final long O_STORE_SIZE = 16;

    /** Maxinal recid returned by allocator. All smaller recids are either occupied or stored in Free Recid Long Stack. Parity4 with shift*/
    protected static final long O_MAX_RECID=  24;

    /** Pointer to first Index Page. Parity 16 */
    protected static final long O_FIRST_INDEX_PAGE = 32;


    protected static final long O_STACK_FREE_RECID = 72;
    protected static final long HEADER_SIZE = O_STACK_FREE_RECID + STACK_COUNT*8;


    /** store will not use Long Stack Pages smaller than this (unless it absolutely has to*/
    protected static final long STACK_MIN_PAGE_SIZE = 64;

    /** store will not use Long Stack Pages larger than this*/
    protected static final long STACK_MAX_PAGE_SIZE = 128*6;

    protected static final long MOFFSET = 0x0000FFFFFFFFFFF0L;

    protected static final long MLINKED = 0x8L;
    protected static final long MUNUSED = 0x4L;
    protected static final long MARCHIVE = 0x2L;
    protected static final long MPARITY = 0x1L;

    protected static final long PAGE_SIZE = 1L<<CC.VOLUME_PAGE_SHIFT;


    protected Volume vol;
    protected Volume headVol;

    protected final List<Long> indexPages = new CopyOnWriteArrayList<Long>();


    public StoreDirect2(String fileName,
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
                           DataIO.HeartbeatFileLock fileLockHeartbeat) {
        super(fileName,
                volumeFactory,
                cache,
                lockScale,
                lockingStrategy,
                checksum,
                compress,
                password,
                readonly,
                snapshotEnable,
                fileLockDisable,
                fileLockHeartbeat);
    }

    public StoreDirect2(String fileName) {
        this(
                fileName,
                fileName == null ? CC.DEFAULT_MEMORY_VOLUME_FACTORY : CC.DEFAULT_FILE_VOLUME_FACTORY,
                null,
                CC.DEFAULT_LOCK_SCALE,
                0,
                false, false, null, false, false, false, null
        );
    }


    @Override
    public void init() {
        if(CC.ASSERT && indexPages.size()!=0)
            throw new AssertionError();

        try {
            boolean empty = Volume.isEmptyFile(fileName);

            vol = volumeFactory.makeVolume(fileName,readonly,fileLockDisable);

            if (empty) {
                //create new store

                //allocate space and set sizes
                vol.ensureAvailable(PAGE_SIZE);
                headVol = initHeadVolOpen();
                storeSizeSet(PAGE_SIZE);

                //prepare index page
                indexPages.add(0L);
                maxRecidSet(RECID_LAST_RESERVED);
                headVol.putLong(O_FIRST_INDEX_PAGE, parity16Set(0));
                //preallocate recids
                for(long recid = 1; recid<=RECID_LAST_RESERVED; recid++){
                    indexValSet(recid, 0);
                }

                //initialize Long Stack
                final long masterLinkVal = parity4Set(0L);
                for (long offset = O_STACK_FREE_RECID; offset < HEADER_SIZE; offset += 8) {
                    headVol.putLong(offset, masterLinkVal);
                }
            } else {
                //reopen existing
                long volLen = vol.length();
                if (volLen < PAGE_SIZE){
                    closeFilesIgnoreException();
                    throw new DBException.DataCorruption("Store is too small");
                }
                headVol = initHeadVolOpen();

                //some basic assertions
                if (storeSizeGet() > volLen) {
                    closeFilesIgnoreException();
                    throw new DBException.DataCorruption("Store is too small");
                }

                //load index pages
                indexPages.add(0L);
                long nextIndexPage = parity16Get(headVol.getLong(O_FIRST_INDEX_PAGE));
                while(nextIndexPage!=0){
                    if(nextIndexPage%PAGE_SIZE!=0)
                        throw new DBException.DataCorruption("Index page pointer wrong");

                    if(nextIndexPage%PAGE_SIZE!=0)
                        throw new DBException.DataCorruption("index page not div by PAGE_SIZE");

                    indexPages.add(nextIndexPage);
                    long oldPage = nextIndexPage;
                    nextIndexPage = parity16Get(vol.getLong(nextIndexPage));
                    if(oldPage>=nextIndexPage && nextIndexPage!=0)
                        throw new DBException.DataCorruption("Index page offset not bigger");
                }


            }

            initFinalize();

        }catch(RuntimeException e){
            closeFilesIgnoreException();
            throw e;
        }
    }

    /** called by {@link #init() at end of method, to finalize storage closure */
    protected void initFinalize() {
        vol.sync();
    }

    protected Volume initHeadVolOpen() {
        if(CC.ASSERT && vol==null)
            throw new AssertionError();
        if(CC.ASSERT && vol.length()<O_STORE_SIZE)
            throw new AssertionError();
        return vol;
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

    protected void storeSizeSet(long storeSize) {
        headVol.putLong(O_STORE_SIZE, parity4Set(storeSize));
    }

    protected long storeSizeGet(){
       return parity4Get(headVol.getLong(O_STORE_SIZE));
    }

    protected void indexValSet(long recid, long indexVal) {
        long offset = recidToOffset(recid);
        if(offset>=PAGE_SIZE)
            throw new UnsupportedOperationException();
        vol.putLong(offset, parity1Set(indexVal));
    }

    protected final long recidToOffset(long recid) {
        if(CC.ASSERT && recid<=0)
            throw new AssertionError();
        if(CC.ASSERT && recid>>>48 !=0)
            throw new AssertionError();
        //there is no zero recid, but that position will be used for zero Index Page checksum

        //convert recid to offset
        recid = HEADER_SIZE + recid * 8 ;

        //compensate for 16 bytes at start of each index page (next page link and checksum)
        recid+= Math.min(1, recid/PAGE_SIZE)*    //min servers as replacement for if(recid>=PAGE_SIZE)
                (16 + ((recid-PAGE_SIZE)/(PAGE_SIZE-16))*16);

        //look up real offset
        recid = indexPages.get((int) (recid / PAGE_SIZE)) + recid%PAGE_SIZE;
        return recid;

    }


    protected void maxRecidSet(long maxRecid) {
        headVol.putLong(O_MAX_RECID, parity4Set(maxRecid<<4));
    }

    protected long maxRecidGet(){
        return parity4Get(headVol.getLong(O_MAX_RECID))>>>4;
    }


    @Override
    protected <A> A get2(long recid, Serializer<A> serializer) {
        return null;
    }

    @Override
    protected void update2(long recid, DataIO.DataOutputByteArray out) {

    }

    @Override
    protected <A> void delete2(long recid, Serializer<A> serializer) {

    }

    @Override
    public long getCurrSize() {
        return 0;
    }

    @Override
    public long getFreeSize() {
        return 0;
    }

    @Override
    public boolean fileLoad() {
        return false;
    }

    @Override
    public void backup(OutputStream out, boolean incremental) {

    }

    @Override
    public void backupRestore(InputStream[] in) {

    }

    @Override
    public long preallocate() {
        return 0;
    }

    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        return 0;
    }

    @Override
    public void close() {
        commitLock.lock();
        try {
            closed = true;
            Volume vol = this.vol;
            if(vol!=null && !readonly)
                vol.sync();
            closeFilesIgnoreException();
        }finally {
            commitLock.unlock();
        }
    }

    @Override
    public void commit() {
        if(readonly)
            return;
        commitLock.lock();
        try {
            Volume vol = this.vol;
            if(vol!=null)
                vol.sync();
        }finally {
            commitLock.unlock();
        }
    }

    @Override
    public void rollback() throws UnsupportedOperationException {

    }

    @Override
    public boolean canRollback() {
        return false;
    }

    @Override
    public Engine snapshot() throws UnsupportedOperationException {
        return null;
    }

    @Override
    public void compact() {

    }


    /**
     * Takes single value from top of long stack page. It does not modify page, except that returned value is zeroed out.
     * Return value is combination of two values, first 2 bytes is new stack tail, after entry was taken. Last 6 bytes is long stack value.
     * If this stack is empty, it returns zero.
     *
     * @return New Stack Tail (2 bytes) and value from stack (6 bytes)
     */
    long longStackTakeFromPage(long pageOffset, long stackTail){
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        //check this is really stack page and check boundaries
        if(CC.ASSERT){
            long header = DataIO.parity4Get(vol.getLong(pageOffset));
            long size = header>>>48;
            if(stackTail>size)
                throw new AssertionError();
            if(stackTail<8 && stackTail!=0)
                throw new AssertionError();
        }

        if(stackTail==0){
            stackTail = longStackPageFindTail(pageOffset);
        }

        if(stackTail==8)
            return 0L;


        long newTail = stackTail-2;

        // packed long always ends with byte&0x80==0
        // so we need to decrement newTail, until we hit end of previous long, or 7

        //decrement position, until read value is found
        while((vol.getUnsignedByte(pageOffset+newTail)&0x80)==0 && newTail>7){
            if(CC.ASSERT && stackTail-newTail>7)
                throw new AssertionError(); //too far, data corrupted
            newTail--;
        }
        // once we hit previous Packed Value, increment tail again to move to current value, packed long at this offset will be taken
        newTail++;
        if(CC.ASSERT && newTail<8)
            throw new AssertionError();

        //read return value
        long ret = vol.getPackedLongReverse(pageOffset+newTail);

        if(CC.VOLUME_ZEROUT){
            vol.clear(pageOffset+newTail, pageOffset+stackTail);
        }
        //check that position is the same as number of bytes read
        if(CC.ASSERT && newTail+(ret>>>60)!=stackTail)
            throw new AssertionError();
        //remove number of bytes read
        ret &= DataIO.PACK_LONG_RESULT_MASK;
        //verify bit parity and normalize parity bit
        ret = DataIO.parity1Get(ret) >>> 1;

        //combine both return values and return
        return (newTail<<48) + ret;
    }

    long longStackPageFindTail(long pageOffset) {
        //get size of the previous page
        long pageHeader =  DataIO.parity4Get(vol.getLong(pageOffset));
        long pageSize = pageHeader>>>48;

        long stackTail = pageSize-1;
        //iterate down until non zero byte, that is tail
        while(vol.getUnsignedByte(pageOffset+stackTail)==0){
            stackTail--;
        }
        stackTail++;


        if(CC.ASSERT && CC.VOLUME_ZEROUT){
            ensureAllZeroes(pageOffset+stackTail, pageOffset+pageSize);
        }
        return stackTail;
    }

    /**
     * Puts long value into long stack.
     *
     * @param pageOffset base offset of Long Stack page
     * @param stackTail current tail of page
     * @return new tail offset, or -1 if value does not fit into this stack
     */
    long longStackPutIntoPage(long value, long pageOffset, long stackTail){
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        //add parity and shift
        value = DataIO.parity1Set(value << 1);

        //check that new value fits into this Long Stack
        int bytesWritten = DataIO.packLongSize(value);
        long stackSize = DataIO.parity4Get(vol.getLong(pageOffset)) >>> 48; //page size is first 2 bytes in long stack page

        if(stackTail==0){
            stackTail = longStackPageFindTail(pageOffset);
        }

        if(stackTail+(bytesWritten)>stackSize) {
            //it does not fit, so return -1
            return -1;
        }

        //ensure that rest of the long stack only contains zeroes
        if(CC.ASSERT && CC.VOLUME_ZEROUT){
            long upperLimit = pageOffset + (CC.PARANOID? stackSize : bytesWritten);
            ensureAllZeroes(pageOffset+stackTail, upperLimit);
        }

        //increment stack tail and return new tail
        stackTail+= vol.putPackedLongReverse(pageOffset + stackTail, value);

        return stackTail;
    }

    void ensureAllZeroes(long startOffset, long endOffset) {
        vol.assertZeroes(startOffset, endOffset);
    }

    /**
     * Take value from Long Stack. It modifies Master Link Value
     *
     * @param masterLinkOffset of Long Stack
     * @return value taken from Long Stack
     */
    long longStackTake(long masterLinkOffset){
        if(CC.ASSERT && masterLinkOffset< O_STACK_FREE_RECID)
            throw new AssertionError();
        if(CC.ASSERT && masterLinkOffset>=HEADER_SIZE)
            throw new AssertionError();

        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        final long masterLinkVal = DataIO.parity4Get(headVol.getLong(masterLinkOffset));
        if(masterLinkVal==0)
            return 0;

        long stackTail = masterLinkVal>>>48;
        long pageOffset = masterLinkVal & StoreDirect.MOFFSET;

        //take value and normalize
        long ret = longStackTakeFromPage(pageOffset, stackTail);
        if(ret==0) {
            // stack is empty, so returns
            return 0;
        }

        //normalize return value
        long newStackTail = ret >>> 48;
        ret &= 0xFFFFFFFFFFFFL;

        long freePageToRelease =0;

        if(newStackTail==8){
            //this page becomes empty, so delete it
            long pageHeader = DataIO.parity4Get(vol.getLong(pageOffset));
            long pageSize = pageHeader>>>48;
            //zero out current page
            vol.clear(pageOffset, pageOffset+pageSize);
            ensureAllZeroes(pageOffset, pageOffset + pageSize);
            freePageToRelease = (pageSize<<48)+pageOffset;

            //move to previous page if it exists
            pageOffset = pageHeader & StoreDirect.MOFFSET;
            newStackTail=0;
        }

        //update masterLink with new value
        long newMasterLinkVal = parity4Set((newStackTail << 48) + pageOffset);
        headVol.putLong(masterLinkOffset, newMasterLinkVal);

        if(freePageToRelease!=0){
            //if this is last page in file, decrease file size
            long freePageSize=  freePageToRelease>>>48;
            long freePageOffset = freePageToRelease & StoreDirect.MOFFSET;
            if(CC.ASSERT && CC.VOLUME_ZEROUT){
                ensureAllZeroes(freePageOffset, freePageOffset + freePageSize);
            }

            if(storeSizeGet()==freePageOffset+freePageSize){
                storeSizeSet(freePageOffset);
            }else{
                longStackPut(longStackMasterLinkOffset(freePageSize), freePageOffset);
            }
        }

        return ret;
    }

    /**
     * Put value into Long Stack. It modifies Master Link Value and creates new pages
     * @param masterLinkOffset
     * @param value
     */
    void longStackPut(long masterLinkOffset, long value){
        if(CC.ASSERT && masterLinkOffset< O_STACK_FREE_RECID)
            throw new AssertionError();
        if(CC.ASSERT && masterLinkOffset>=HEADER_SIZE)
            throw new AssertionError();

        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.ASSERT & value==0)
            throw new IllegalArgumentException();

        final long masterLinkVal = DataIO.parity4Get(headVol.getLong(masterLinkOffset));
        if(masterLinkVal==0) {
            longStackPageCreate(value,masterLinkOffset,0L);
            return;
        }

        final long stackTail = masterLinkVal >>> 48;
        final long pageOffset = masterLinkVal & StoreDirect.MOFFSET;

        long newStackTail = longStackPutIntoPage(value, pageOffset, stackTail);
        if(newStackTail==-1){
            longStackPageCreate(value,masterLinkOffset,pageOffset);
            return;
        }

        //update masterLink with new value
        long newMasterLinkVal = parity4Set((newStackTail << 48) + pageOffset);
        headVol.putLong(masterLinkOffset, newMasterLinkVal);
    }

    long longStackPageCreate(long value, long masterLinkOffset, long previousPage){
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        long pageSize = 0;
        long pageOffset = 0;

        //try to reuse existing space
        pageLoop:
        for(long pageSize2 = STACK_MIN_PAGE_SIZE;pageSize2<=STACK_MAX_PAGE_SIZE; pageSize2+=16){
            long pageOffset2 = longStackTake(StoreDirect2.longStackMasterLinkOffset(pageSize2));
            if(pageOffset2!=0) {
                pageSize = pageSize2;
                pageOffset = pageOffset2;
                break pageLoop;
            }
        }


        //there is no existing space to reuse, so append to EOF
        if(pageOffset==0) {
            //expand file
            pageSize = 160;
            pageOffset = storeSizeGet();

            if ((pageOffset >>> CC.VOLUME_PAGE_SHIFT) != ((pageOffset + pageSize - 1) >>> CC.VOLUME_PAGE_SHIFT)) {
                //crossing page boundaries
                long sizeUntilBoundary = PAGE_SIZE - pageOffset % PAGE_SIZE;

                //there are two options, if remaining size is enough for long stack, just use it
                if (sizeUntilBoundary > 9) {
                    pageSize = sizeUntilBoundary;
                } else {
                    //there is not enough space for new page, so just drop the space (do not even add it to free list)
                    //to create page beyond 1MB overlap
                    pageOffset += sizeUntilBoundary;
                    storeSizeSet(pageOffset);//pageOffset helds old store size
                }
            }

            long storeSize = storeSizeGet();
            storeSizeSet(storeSize + pageSize);
            vol.ensureAvailable(storeSize + pageSize);
        }

        if(CC.VOLUME_ZEROUT && CC.PARANOID){
            ensureAllZeroes(pageOffset, pageOffset + pageSize);
        }
        //ensure it is not crossing boundary
        if(CC.ASSERT && (pageOffset >>> CC.VOLUME_PAGE_SHIFT) != ((pageOffset+pageSize-1) >>> CC.VOLUME_PAGE_SHIFT)){
            throw new AssertionError();
        }

        //set page header
        vol.putLong(pageOffset, parity4Set((pageSize << 48) + previousPage));
        //put new value
        long tail = 8 + vol.putPackedLongReverse(pageOffset + 8, DataIO.parity1Set(value << 1));
        //update master pointer
        headVol.putLong(masterLinkOffset, parity4Set((tail << 48) + pageOffset));

        return (pageSize<<48) + pageOffset;
    }

    Map<Long,List<Long>> longStackDumpAll(){
        Map<Long,List<Long>> ret = new LinkedHashMap<Long, List<Long>>();
        masterLoop: for(long masterSize = 0; masterSize<64*1024; masterSize+=16){
            long masterLinkOffset = masterSize==0? O_STACK_FREE_RECID : longStackMasterLinkOffset(masterSize);
            ret.put(masterSize, longStackDump(masterLinkOffset));
        }
        return ret;
    }

    List<Long> longStackDump(long masterLinkOffset) {
        List<Long> ret = new ArrayList<Long>();
        long masterLinkVal = headVol.getLong(masterLinkOffset);
        if(masterLinkVal==0)
            return ret;
        masterLinkVal = DataIO.parity4Get(masterLinkVal);

        long pageOffset = masterLinkVal&StoreDirect.MOFFSET;
        if(pageOffset==0)
            return ret;

        pageLoop: for(;;) {
            long pageHeader = DataIO.parity4Get(vol.getLong(pageOffset));
            long nextPage = pageHeader&StoreDirect.MOFFSET;
            long pageSize = pageHeader>>>48;

            long end = pageSize-1;
            //iterate down until non zero byte, that is tail
            while(vol.getUnsignedByte(pageOffset+end)==0){
                end--;
            }
            end++;

            long tail = 8;
            findTailLoop: for (; ; ) {
                if (tail == end)
                    break findTailLoop;
                long r = vol.getPackedLongReverse(pageOffset + tail);
                if ((r & DataIO.PACK_LONG_RESULT_MASK) == 0) {
                    //tail found
                    break findTailLoop;
                }
                if (CC.ASSERT) {
                    //verify that this is dividable by zero
                    DataIO.parity1Get(r & DataIO.PACK_LONG_RESULT_MASK);
                }
                //increment tail pointer with number of bytes read
                tail += r >>> 60;
                long val = DataIO.parity1Get(r & DataIO.PACK_LONG_RESULT_MASK) >>> 1;
                ret.add(val);
            }

            //move to next page
            if(nextPage==0)
                break pageLoop;
            pageOffset = nextPage;
        }
        return ret;
    }




    protected static long round16Up(long pos) {
        return (pos+15)/16*16;
    }

    static long longStackMasterLinkOffset(long pageSize){
        if(CC.ASSERT && pageSize<=0)
            throw new AssertionError();
        if(CC.ASSERT && pageSize>64*1024)
            throw new AssertionError();
        if(CC.ASSERT && pageSize % 16!=0)
            throw new AssertionError();

        return O_STACK_FREE_RECID + pageSize/2;  //(2==16/8)
    }

    protected void freeRecidPut(long recid){

    }

    protected long freeRecidTake(){
        return 0L;
    }


    /** paranoid store check. Check for overlaps, empty space etc... */
    void storeCheck(){
        long storeSize = storeSizeGet();
        /**
         * This BitSet contains 1 for bytes which are accounted for (part of data, or marked as free)
         * At end there should be no unaccounted bytes, and this BitSet is completely filled
         */
        BitSet b = new BitSet((int) storeSize); // TODO limited to 2GB, add BitSet methods to Volume
        b.set(0, (int) (HEADER_SIZE+ 8), true); // +8 is zero Index Page checksum

        //mark unused recid before end of current page;
        {
            long maxRecid = maxRecidGet();
            long offset = 0;
            for(long recid = 1; recid<=maxRecid; recid++){
                offset = recidToOffset(recid);
                long indexVal = vol.getLong(offset);
                if(indexVal==0)
                    continue; // unused recid
                b.set((int)offset,(int)offset+8);
            }

            offset +=8;
            if(offset%PAGE_SIZE!=0){
                //mark rest of this Index Page as used
                long endOffset = Fun.roundUp(offset, PAGE_SIZE);
                vol.assertZeroes(offset,endOffset);
                b.set((int)offset,(int)endOffset);
            }

        }

        if(vol.length()<storeSize)
            throw new AssertionError("Store too small, need "+storeSize+", got "+vol.length());

        vol.assertZeroes(storeSize, vol.length());

        //TODO do accounting for recid once pages are implemented


        /**
         * Check free data by traversing Long Stack Pages
         */
        //iterate over Long Stack Pages
        masterSizeLoop:
        for(long masterSize = 16; masterSize<=64*1024;masterSize+=16) {
            long masterOffset = StoreDirect2.longStackMasterLinkOffset(masterSize);
            long masterLinkVal = parity4Get(headVol.getLong(masterOffset));

            long pageOffset = masterLinkVal&StoreDirect.MOFFSET;
            if(pageOffset==0)
                continue masterSizeLoop;

            pageLoop: for(;;) {
                long pageHeader = DataIO.parity4Get(vol.getLong(pageOffset));
                long nextPage = pageHeader&StoreDirect.MOFFSET;
                long pageSize = pageHeader>>>48;

                //mark this Long Stack Page empty
                storeCheckMark(b, true, pageOffset, pageSize);

                long end = pageSize-1;
                //iterate down until non zero byte, that is tail
                while(vol.getUnsignedByte(pageOffset+end)==0){
                    end--;
                }
                end++;

                long tail = 8;
                findTailLoop: for (; ; ) {
                    if (tail == end)
                        break findTailLoop;
                    long r = vol.getPackedLongReverse(pageOffset + tail);
                    if ((r & DataIO.PACK_LONG_RESULT_MASK) == 0) {
                        //tail found
                        break findTailLoop;
                    }
                    if (CC.ASSERT) {
                        //verify that this is dividable by zero
                        DataIO.parity1Get(r & DataIO.PACK_LONG_RESULT_MASK);
                    }
                    //increment tail pointer with number of bytes read
                    tail += r >>> 60;
                    long val = DataIO.parity1Get(r & DataIO.PACK_LONG_RESULT_MASK) >>> 1;

                    //content of Long Stack should be free, so mark it as used
                    storeCheckMark(b, false, val & MOFFSET, masterSize);
                }

                //move to next page
                if(nextPage==0)
                    break pageLoop;
                pageOffset = nextPage;
            }
        }

        //assert that all data are accounted for
        for(int offset = 0; offset<storeSize;offset++){
            if(!b.get(offset))
                throw new AssertionError("zero at "+offset);
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

}
