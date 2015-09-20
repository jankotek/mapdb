package org.mapdb;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class StoreCached2 extends StoreDirect2{

    /*
     * Modified Long Stack Pages.
     *
     * Real size if this is `byte[].length-1`. Last byte contains flags which indicate origin of page.
     * - Lowest bit indicates if page was modified (content of storage is different from `byte[]`, if bit is set page was modifed.
     * - Second lowest bit indicates if page was created and not loaded, if this bit is zero,
     *   real content of storage is all zeroes.
     */
    protected final Map<Long,byte[]> longStackPages = new HashMap<Long,byte[]>();

    public StoreCached2(String fileName,
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

    public StoreCached2(String fileName){
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
    protected Volume initHeadVolOpen() {
        if(CC.ASSERT && vol==null)
            throw new AssertionError();
        if(CC.ASSERT && vol.length()<O_STORE_SIZE)
            throw new AssertionError();
        Volume.SingleByteArrayVol ret = new Volume.SingleByteArrayVol((int)HEADER_SIZE);
        vol.getData(0, ret.data,0,ret.data.length);
        return ret;
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
            flush();
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
        commitLock.lock();
        try{
            flush();
        }finally {
            commitLock.unlock();
        }
    }

    private void flush() {
        if(CC.ASSERT && !commitLock.isHeldByCurrentThread())
            throw new AssertionError();

        structuralLock.lock();
        try{
            if(CC.PARANOID){
                assertNoOverlaps(longStackPages);
            }
            vol.ensureAvailable(storeSizeGet());

            pagesLoop:  for(Map.Entry<Long,byte[]> e:longStackPages.entrySet()){
                long pageOffset = e.getKey();
                byte[] page = e.getValue();
                //if page was not modified continue
                if((page[page.length-1]&1)==0){
                    continue pagesLoop;
                }

                // write page into store
                // last byte in `page` stores bit flags, is not part of storage
                vol.putData(pageOffset,page,0,page.length-1);
            }
            longStackPages.clear();

            //copy headVol into main store
            byte[] headVolBytes = ((Volume.SingleByteArrayVol)headVol).data;
            vol.putData(0, headVolBytes, 0, headVolBytes.length);
        }finally {
            structuralLock.unlock();
        }

        vol.sync();
    }

    protected void assertNoOverlaps(Map<Long, byte[]> pages) {
        //put all keys into sorted array
        long[] sorted = new long[pages.size()];

        int c = 0;
        for(Long key:pages.keySet()){
            sorted[c++] = key;
        }

        Arrays.sort(sorted);

        for(int i=0;i<sorted.length-1;i++){
            long offset = sorted[i];
            long pageSize = pages.get(offset).length-1;
            long offsetNext = sorted[i+1];

            if(offset+pageSize<offsetNext)
                throw new AssertionError();
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

    byte[] longStackLoadPage(long pageOffset){
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        // get page from cache, if it was already loaded
        byte[] page= longStackPages.get(pageOffset);
        if(page!=null)
            return page;

        // not loaded yet, load it
        int pageSize = (int) (DataIO.parity4Get(vol.getLong(pageOffset))>>>48);
        if(CC.ASSERT && pageSize<8)
            throw new AssertionError();

        page = new byte[pageSize+1];
        vol.getData(pageOffset, page, 0, pageSize);
        page[page.length-1] = 1<<1;
        longStackPages.put(pageOffset, page);
        return page;
    }

    byte[] longStackNewPage(long pageOffset, long pageSize){
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        if(CC.ASSERT && CC.VOLUME_ZEROUT){
            ensureAllZeroes(pageOffset, pageOffset+pageSize);
        }

        byte[] page = new byte[(int) (pageSize+1)];
        page[page.length-1] = 0;
        if(CC.ASSERT && !(this instanceof StoreWAL2) && longStackPages.get(pageOffset)!=null) {
            throw new AssertionError();
        }
        longStackPages.put(pageOffset, page);
        return page;
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

        long stackTail = (int) (masterLinkVal>>>48);
        long pageOffset = masterLinkVal & StoreDirect.MOFFSET;

        if(pageOffset==0) {
            //long stack is empty
            return 0;
        }


        byte[] page = longStackLoadPage(pageOffset);
        if(stackTail==0){
            //stack tail is unknown, find end of this page
            stackTail = page.length-2;
            //iterate down until non zero byte, that is tail
            while(page[((int) stackTail)]==0){
                stackTail--;
            }
            stackTail++;
        }

        longStackPageSetModified(page);

        // packed long always ends with byte&0x80==0
        // so we need to decrement newTail, until we hit end of previous long, or 7

        //decrement position, until read value is found
        stackTail-=2;
        while(page[((int) stackTail)]>=0 && stackTail>7){
            if(CC.ASSERT && stackTail-stackTail>7)
                throw new AssertionError(); //too far, data corrupted
            stackTail--;
        }
        stackTail++;

        if(CC.ASSERT && stackTail<8)
            throw new AssertionError();

        //read return value
        long ret = DataIO.unpackLongReverseReturnSize(page, (int) stackTail);

        //zeroout old value
        for(long pos = stackTail, limit=stackTail+(ret>>>60);pos<limit;pos++){
            page[(int)pos] = 0;
        }

        if(CC.ASSERT && (ret>>>60)+stackTail != masterLinkVal>>>48 && masterLinkVal>>>48!=0){
            //number of bytes read is not matching old stackTail
            throw new AssertionError();
        }
        ret &= 0xFFFFFFFFFFFFL;
        ret = DataIO.parity1Get(ret)>>>1;

        long pageToDelete = 0;
        if(stackTail==8){
            pageToDelete = pageOffset;
            //move page to previous link
            pageOffset = DataIO.parity4Get(DataIO.getLong(page,0)) & StoreDirect.MOFFSET;
            //tail is not known on previous page, so will have to find it on next take
            stackTail = 0;
        }

        //update masterLink with new value
        long newMasterLinkVal = DataIO.parity4Set((stackTail<<48) + pageOffset);
        headVol.putLong(masterLinkOffset, newMasterLinkVal);

        if(pageToDelete!=0)
            longStackPageDelete(pageToDelete);

        return ret;
    }

    /** deletes long stack page, it must be loaded into `longStackPages` cache */
    protected void longStackPageDelete(long pageOffset) {
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        //this page becomes empty, so delete it
        byte[] old = longStackPages.remove(pageOffset);
        long pageSize = old.length-1;
        if(storeSizeGet()==pageOffset+pageSize) {
            //decrement file size, if at end of the storage
            storeSizeSet(pageOffset);
        }else{
            longStackPut(longStackMasterLinkOffset(pageSize), pageOffset);
        }

        if(CC.PARANOID && CC.VOLUME_ZEROUT) {
            for (int i = 8; i < old.length - 1; i++) {
                if (old[i] != 0)
                    throw new AssertionError();
            }
        }
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
        long stackTail = masterLinkVal>>>48;
        final long pageOffset = masterLinkVal & StoreDirect.MOFFSET;

        if(stackTail==0)
            stackTail = longStackPageFindTail(pageOffset);

        if(CC.ASSERT && stackTail<8){
            throw new AssertionError();
        }

        byte[] page = longStackLoadPage(pageOffset);


        //does current page fit the data?
        long pageSize = DataIO.parity4Get(DataIO.getLong(page, 0))>>>48;
        if(stackTail+DataIO.packLongSize(DataIO.parity1Set(value<<1)) > pageSize) {
            //start new page
            longStackPageCreate(value,masterLinkOffset,pageOffset);
            return;
        }

        longStackPageSetModified(page);
        stackTail += DataIO.packLongReverseReturnSize(page, (int) stackTail, DataIO.parity1Set(value << 1));

        if(CC.ASSERT && CC.VOLUME_ZEROUT){
            for(int i=(int)stackTail;i<page.length-1;i++){
                if(page[i]!=0)
                    throw new AssertionError();
            }
        }

        if(CC.ASSERT && stackTail<8){
            throw new AssertionError();
        }

        //update masterLink with new value
        long newMasterLinkVal = DataIO.parity4Set((stackTail<<48) + pageOffset);
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
            pageSize = 160;
            pageOffset = storeSizeGet();

            if ((pageOffset >>> CC.VOLUME_PAGE_SHIFT) != ((pageOffset + pageSize - 1) >>> CC.VOLUME_PAGE_SHIFT)) {
                //crossing page boundaries
                long sizeUntilBoundary = CC.VOLUME_PAGE_SIZE - pageOffset % CC.VOLUME_PAGE_SIZE;

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
        }


        byte[] page = longStackNewPage(pageOffset, pageSize);
        longStackPageSetModified(page);
        //ensure it is not crossing boundary
        if(CC.ASSERT && (pageOffset >>> CC.VOLUME_PAGE_SHIFT) != ((pageOffset+pageSize-1) >>> CC.VOLUME_PAGE_SHIFT)){
            throw new AssertionError();
        }

        //set page header
        DataIO.putLong(page,0, DataIO.parity4Set((pageSize << 48) + previousPage));
        //put new value
        long valueWithParity = DataIO.parity1Set(value << 1);
        long tail = 8+ DataIO.packLongReverseReturnSize(page, 8, valueWithParity);
        //update master pointer
        headVol.putLong(masterLinkOffset, DataIO.parity4Set((tail<<48)+pageOffset));

        return (pageSize<<48) + pageOffset;
    }

    @Override
    long longStackPageFindTail(long pageOffset) {
        byte[] page = longStackLoadPage(pageOffset);
        //get size of the previous page
        long pageHeader =  DataIO.parity4Get(DataIO.getLong(page,0));
        long pageSize = pageHeader>>>48;
        if(CC.ASSERT && pageSize!=page.length-1)
            throw new AssertionError();
        //in order to find tail of this page, read all packed longs on this page
        long stackTail = pageSize-1;
        //iterate down until non zero byte, that is tail
        while(page[((int) stackTail)]==0){
            stackTail--;
        }
        stackTail++;


        if(CC.ASSERT && CC.VOLUME_ZEROUT){
            for(int i= (int) stackTail; i<page.length-1; i++) {
                if (page[i] != 0)
                    throw new AssertionError();
            }
        }
        return stackTail;
    }

    @Override
    void ensureAllZeroes(long startOffset, long endOffset) {
        startOffset = Math.min(startOffset, vol.length());
        endOffset = Math.min(endOffset, vol.length());
        super.ensureAllZeroes(startOffset, endOffset);
    }

    protected void longStackPageSetModified(byte[] page) {
        int pos = page.length-1;
        page[pos] |= 1;
    }
}
