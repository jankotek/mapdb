package org.mapdb;

import java.io.InputStream;
import java.io.OutputStream;

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
    protected final LongObjectMap<byte[]> longStackPages = new LongObjectMap<byte[]>();

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
    public void init() {
        vol = volumeFactory.makeVolume(fileName,readonly,fileLockDisable);
        vol.ensureAvailable(HEADER_SIZE);
        headVol = new Volume.SingleByteArrayVol(HEADER_SIZE);
        storeSize = HEADER_SIZE;
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
        commitLock.lock();
        try{
            structuralLock.lock();
            try{
                if(CC.PARANOID){
                    //ensure there is no overlap in modified pages

                    l1: for(int i=0;i<longStackPages.set.length;i++){
                        long pageOffset = longStackPages.set[i];
                        if(pageOffset==0)
                            continue l1;
                        long pageSize = ((byte[])longStackPages.values[i]).length-1;

                        l2: for(int j=0;j<longStackPages.set.length;j++){
                            long pageOffset2 = longStackPages.set[i];
                            if(pageOffset2==0 ||  pageOffset==pageOffset2)
                                continue l2;

                            if(pageOffset<=pageOffset2 && pageOffset2<=pageOffset+pageSize){
                                throw new AssertionError();
                            }
                        }

                    }
                }
                vol.ensureAvailable(storeSize);

                pagesLoop: for(int i=0;i<longStackPages.set.length;i++){
                    long pageOffset = longStackPages.set[i];
                    if(pageOffset==0) {
                        continue pagesLoop;
                    }
                    byte[] page = (byte[]) longStackPages.values[i];
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
            stackTail = 8;
            findTailLoop: for(;;){
                if(stackTail>=page.length-1)
                    break findTailLoop;
                long r = DataIO.unpackLongReturnSize(page, (int) stackTail);
                if((r&DataIO.PACK_LONG_RESULT_MASK)==0) {
                    //tail found
                    break findTailLoop;
                }
                if(CC.ASSERT){
                    //verify that this is dividable by zero
                    DataIO.parity1Get(r&DataIO.PACK_LONG_RESULT_MASK);
                }
                //increment tail pointer with number of bytes read
                stackTail+= r>>>60;
            }

            if(CC.ASSERT && CC.VOLUME_ZEROUT){
                for(int i= (int) stackTail;i<page.length-2;i++){
                    if(page[i]!=0)
                        throw new AssertionError();
                }
            }
        }

        longStackPageSetModified(page);

        // packed long always ends with byte&0x80==0
        // so we need to decrement newTail, until we hit end of previous long, or 7

        //decrement position, until read value is found
        stackTail-=2;
        while(page[((int) stackTail)]<0 && stackTail>7){
            if(CC.ASSERT && stackTail-stackTail>7)
                throw new AssertionError(); //too far, data corrupted
            stackTail--;
        }
        stackTail++;

        if(CC.ASSERT && stackTail<8)
            throw new AssertionError();

        //read return value
        long ret = DataIO.unpackLongReturnSize(page, (int) stackTail);

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

        if(stackTail==8){
            long origPageOffset = pageOffset;
            //move page to previous link
            pageOffset = DataIO.parity4Get(DataIO.getLong(page,0)) & StoreDirect.MOFFSET;
            //tail is not known on previous page, so will have to find it on next take
            stackTail = 0;
            longStackPageDelete(origPageOffset);
        }

        //update masterLink with new value
        long newMasterLinkVal = DataIO.parity4Set((stackTail<<48) + pageOffset);
        headVol.putLong(masterLinkOffset, newMasterLinkVal);

        return ret;
    }

    protected void longStackPageDelete(long pageOffset) {
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        //this page becomes empty, so delete it
        byte[] old = longStackPages.remove(pageOffset);

        if(storeSize==pageOffset+old.length-1)
            storeSize -= old.length-1;

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
        stackTail += DataIO.packLongReturnSize(page, (int) stackTail, DataIO.parity1Set(value<<1));

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
        long tail = 8+ DataIO.packLongReturnSize(page, 8, valueWithParity);
        //update master pointer
        headVol.putLong(masterLinkOffset, DataIO.parity4Set((tail<<48)+pageOffset));

        return (pageSize<<48) + pageOffset;
    }

    @Override
    long longStackPageFindTail(long pageOffset) {
        byte[] page = longStackLoadPage(pageOffset);
        long stackTail;
        //get size of the previous page
        long pageHeader =  DataIO.parity4Get(DataIO.getLong(page,0));
        long pageSize = pageHeader>>>48;
        if(CC.ASSERT && pageSize!=page.length-1)
            throw new AssertionError();
        //in order to find tail of this page, read all packed longs on this page
        stackTail = 8;
        findTailLoop: for(;;){
            if(stackTail==pageSize)
                break findTailLoop;
            long r = DataIO.unpackLongReturnSize(page, (int) stackTail);
            if((r&DataIO.PACK_LONG_RESULT_MASK)==0) {
                //tail found
                break findTailLoop;
            }
            if(CC.ASSERT){
                //verify that this is dividable by zero
                DataIO.parity1Get(r&DataIO.PACK_LONG_RESULT_MASK);
            }
            //increment tail pointer with number of bytes read
            stackTail+= r>>>60;
        }


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
