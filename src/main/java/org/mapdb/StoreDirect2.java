package org.mapdb;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

/**
 *
 */
public class StoreDirect2 extends Store{

    protected static final long STACK_COUNT = 1024*64/16;

    protected static final long O_STACK_FREE_RECID = 64;
    protected static final long HEADER_SIZE = O_STACK_FREE_RECID + STACK_COUNT*8;



    protected Volume vol;
    protected Volume headVol;

    protected long storeSize;

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

    public StoreDirect2(String fileName){
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
        vol = headVol = volumeFactory.makeVolume(fileName,readonly,fileLockDisable);
        vol.ensureAvailable(HEADER_SIZE);
        storeSize = HEADER_SIZE;
        final long masterLinkVal = DataIO.parity4Set(0L);
        for(long offset= O_STACK_FREE_RECID;offset<HEADER_SIZE;offset+=8){
            headVol.putLong(offset,masterLinkVal);
        }
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

    }

    @Override
    public void commit() {

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
        for(long offset = startOffset; offset<endOffset; offset++){
            if(vol.getUnsignedByte(offset)!=0)
                throw new AssertionError();
        }
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
            ensureAllZeroes(pageOffset, pageOffset+pageSize);
            freePageToRelease = (pageSize<<48)+pageOffset;

            //move to previous page if it exists
            pageOffset = pageHeader & StoreDirect.MOFFSET;
            newStackTail=0;
        }

        //update masterLink with new value
        long newMasterLinkVal = DataIO.parity4Set((newStackTail<<48) + pageOffset);
        headVol.putLong(masterLinkOffset, newMasterLinkVal);

        if(freePageToRelease!=0){
            //if this is last page in file, decrease file size
            long freePageSize=  freePageToRelease>>>48;
            long freePageOffset = freePageToRelease & StoreDirect.MOFFSET;
            if(CC.ASSERT && CC.VOLUME_ZEROUT){
                ensureAllZeroes(freePageOffset, freePageOffset+freePageSize);
            }

            if(storeSize==freePageOffset+freePageSize){
                storeSize-=freePageSize;
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

        final long stackTail = masterLinkVal>>>48;
        final long pageOffset = masterLinkVal & StoreDirect.MOFFSET;

        long newStackTail = longStackPutIntoPage(value, pageOffset, stackTail);
        if(newStackTail==-1){
            longStackPageCreate(value,masterLinkOffset,pageOffset);
            return;
        }

        //update masterLink with new value
        long newMasterLinkVal = DataIO.parity4Set((newStackTail<<48) + pageOffset);
        headVol.putLong(masterLinkOffset, newMasterLinkVal);
    }

    long longStackPageCreate(long value, long masterLinkOffset, long previousPage){
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        long pageSize = 160;
        long pageOffset = storeSize;

        if((pageOffset >>> CC.VOLUME_PAGE_SHIFT) != ((pageOffset+pageSize-1) >>> CC.VOLUME_PAGE_SHIFT)){
            //crossing page boundaries
            long sizeUntilBoundary = CC.VOLUME_PAGE_SIZE-pageOffset%CC.VOLUME_PAGE_SIZE;

            //there are two options, if remaining size is enough for long stack, just use it
            if(sizeUntilBoundary>9){
                pageSize =sizeUntilBoundary;
            }else{
                //there is not enough space for new page, so just drop the space (do not even add it to free list)
                //and create page beyond boundary
                storeSize +=sizeUntilBoundary;
                pageOffset = storeSize;
            }
        }

        storeSize+=pageSize;
        vol.ensureAvailable(storeSize);

        if(CC.VOLUME_ZEROUT && CC.PARANOID){
            ensureAllZeroes(pageOffset, pageOffset + pageSize);
        }
        //ensure it is not crossing boundary
        if(CC.ASSERT && (pageOffset >>> CC.VOLUME_PAGE_SHIFT) != ((pageOffset+pageSize-1) >>> CC.VOLUME_PAGE_SHIFT)){
            throw new AssertionError();
        }

        //set page header
        vol.putLong(pageOffset, DataIO.parity4Set((pageSize << 48) + previousPage));
        //put new value
        long tail = 8 + vol.putPackedLongReverse(pageOffset + 8, DataIO.parity1Set(value << 1));
        //update master pointer
        headVol.putLong(masterLinkOffset, DataIO.parity4Set((tail<<48)+pageOffset));

        return (pageSize<<48) + pageOffset;
    }

    Map<Long,List<Long>> longStackDumpAll(){
        Map<Long,List<Long>> ret = new LinkedHashMap<Long, List<Long>>();
        masterLoop: for(long masterSize = 0; masterSize<64*1024; masterSize+=16){
            long masterLinkOffset = masterSize==0? O_STACK_FREE_RECID : longStackMasterLinkOffset(masterSize);
            long masterLinkVal = headVol.getLong(masterLinkOffset);
            if(masterLinkVal==0)
                continue masterLoop;
            masterLinkVal = DataIO.parity4Get(masterLinkVal);

            long pageOffset = masterLinkVal&StoreDirect.MOFFSET;
            if(pageOffset==0)
                continue masterLoop;

            pageLoop: for(;;) {
                List<Long> l = new ArrayList<Long>();
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
                    l.add(val);
                }
                Collections.reverse(l);
                ret.put(masterSize, l);

                //move to next page
                if(nextPage==0)
                    break pageLoop;
                pageOffset = nextPage;
            }
        }
        return ret;
    }


    protected static long round16Up(long pos) {
        return (pos+15)/16*16;
    }

    static long longStackMasterLinkOffset(long pageSize){
        if(CC.ASSERT && pageSize<=0)
            throw new AssertionError();
        if(CC.ASSERT && pageSize>=64*1024)
            throw new AssertionError();

        return O_STACK_FREE_RECID + round16Up(pageSize)/2;  //(2==16/8)
    }

}
