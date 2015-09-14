package org.mapdb;


public class StoreWAL2 extends StoreCached2{

    /*
     * Committed Long Stack Pages, but not yet replayed into store.
     *
     * Real size if this is `byte[].length-1`. Last byte contains flags which indicate origin of page.
     * - Lowest bit indicates if page was modified (content of storage is different from `byte[]`, if bit is set page was modifed.
     * - Second lowest bit indicates if page was created and not loaded, if this bit is zero,
     *   real content of storage is all zeroes.
     */
    protected final LongObjectMap<byte[]> longStackCommited = new LongObjectMap<byte[]>();

    protected byte[] headVolBackup = new byte[(int) HEADER_SIZE];

    public StoreWAL2(String fileName,
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


    public StoreWAL2(String fileName){
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
    public void rollback() {
        commitLock.lock();
        try{
            structuralLock.lock();
            try{
                longStackPages.clear();
                headVol.putData(0, headVolBackup, 0, headVolBackup.length);
            }finally {
                structuralLock.unlock();
            }
        }finally {
            commitLock.unlock();
        }
    }
    @Override
    public void commit() {
        commitLock.lock();
        try{
            structuralLock.lock();
            try{
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

                    longStackCommited.put(pageOffset,page);
                }
                longStackPages.clear();
                headVol.getData(0, headVolBackup, 0, headVolBackup.length);
            }finally {
                structuralLock.unlock();
            }

            vol.sync();
        }finally {
            commitLock.unlock();
        }
    }

    public void commitFullReplay() {
        commitLock.lock();
        try{
            structuralLock.lock();
            try{
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

                    //make sure that it has the same size as prev pages
                    byte[] page2 = longStackCommited.get(pageOffset);
                    if(page2!=null){
                        if(page.length!=page2.length)
                            throw new AssertionError();
                        longStackCommited.remove(pageOffset);
                    }
                    //write page
                    vol.putData(pageOffset,page,0,page.length-1);
                }
                longStackPages.clear();

                pagesLoop: for(int i=0;i<longStackCommited.set.length;i++){
                    long pageOffset = longStackCommited.set[i];
                    if(pageOffset==0) {
                        continue pagesLoop;
                    }
                    byte[] page = (byte[]) longStackCommited.values[i];
                    //if page was not modified continue
                    if((page[page.length-1]&1)==0){
                        continue pagesLoop;
                    }
                    vol.putData(pageOffset,page,0,page.length-1);
                }
                longStackCommited.clear();

                //copy headVol into main store
                headVol.getData(0, headVolBackup, 0, headVolBackup.length);
                vol.putData(0, headVolBackup, 0, headVolBackup.length);

            }finally {
                structuralLock.unlock();
            }

            vol.sync();
        }finally {
            commitLock.unlock();
        }

    }


    @Override
    protected void longStackPageDelete(long pageOffset) {
        byte[] page = longStackPages.get(pageOffset);
        for(int i=0;i<page.length-1;i++){
            page[i]=0;
        }
        if(storeSize==pageOffset+page.length-1)
            storeSize -= page.length-1;
        longStackPageSetModified(page);
        if(CC.PARANOID && CC.VOLUME_ZEROUT) {
            for (int i = 8; i < page.length - 1; i++) {
                if (page[i] != 0)
                    throw new AssertionError();
            }
        }
    }


    @Override
    byte[] longStackLoadPage(long pageOffset){
        if(CC.ASSERT && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        //get this page from committed pages
        byte[] page= longStackCommited.get(pageOffset);
        if(page!=null){
            //clone page, so changes does do not modify already commited data
            page = page.clone();
            longStackPages.put(pageOffset,page);
            return page;
        }


        // get page from cache, if it was already loaded
        page = longStackPages.get(pageOffset);
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

}
