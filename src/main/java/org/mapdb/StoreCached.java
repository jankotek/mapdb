package org.mapdb;

import java.util.Arrays;

import static org.mapdb.DataIO.*;

/**
 * Extends {@link StoreDirect} with Write Cache
 */
public class StoreCached extends StoreDirect{


    /** stores modified stack pages. */
    //TODO only accessed under structural lock, should be LongConcurrentHashMap?
    protected final LongMap<byte[]> dirtyStackPages = new LongHashMap<byte[]>();


    public StoreCached(String fileName, Fun.Function1<Volume, String> volumeFactory, boolean checksum,
                       boolean compress, byte[] password, boolean readonly, boolean deleteFilesAfterClose,
                       int freeSpaceReclaimQ, boolean commitFileSyncDisable, int sizeIncrement) {
        super(fileName, volumeFactory, checksum, compress, password, readonly, deleteFilesAfterClose,
                freeSpaceReclaimQ, commitFileSyncDisable, sizeIncrement);
    }


    public StoreCached(String fileName) {
        super(fileName);
    }


    @Override
    protected void longStackPut(long masterLinkOffset, long value, boolean recursive) {
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

        byte[] page = loadLongStackPage(pageOffset);

        long currSize = masterLinkVal>>>48;

        long prevLinkVal = parity4Get(DataIO.getLong(page,4));
        long pageSize = prevLinkVal>>>48;
        //is there enough space in current page?
        if(currSize+8>=pageSize){
            //no there is not enough space
            //first zero out rest of the page
            Arrays.fill(page,(int)currSize,(int)pageSize,(byte)0);
            //allocate new page
            longStackNewPage(masterLinkOffset,pageOffset,value);
            return;
        }

        //there is enough space, so just write new value
        currSize += DataIO.packLongBidi(page, (int) currSize,parity1Set(value<<1));

        //and update master pointer
        headVol.putLong(masterLinkOffset, parity4Set(currSize<<48 | pageOffset));
    }

    @Override
    protected long longStackTake(long masterLinkOffset, boolean recursive) {
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

        byte[] page = loadLongStackPage(pageOffset);

        //read packed link from stack
        long ret = DataIO.unpackLongBidiReverse(page, (int) currSize);
        //extract number of read bytes
        long oldCurrSize = currSize;
        currSize-= ret >>>56;
        //clear bytes occupied by prev value
        Arrays.fill(page,(int)currSize,(int)oldCurrSize,(byte)0);
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
        long prevPageOffset = parity4Get(DataIO.getLong(page,4));
        final int currPageSize = (int) (prevPageOffset>>>48);
        prevPageOffset &= MOFFSET;

        //does previous page exists?
        if(prevPageOffset!=0) {
            //yes previous page exists

            byte[] page2 = loadLongStackPage(prevPageOffset);

            //find pointer to end of previous page
            // (data are packed with var size, traverse from end of page, until zeros

            //first read size of current page
            currSize = parity4Get(DataIO.getLong(page2, 4)) >>> 48;

            //now read bytes from end of page, until they are zeros
            while (page2[((int) (currSize - 1))] == 0) {
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
        dirtyStackPages.remove(pageOffset);
        freeDataPut(pageOffset, currPageSize);
        //TODO how TX should handle this

        return ret;
    }

    protected byte[] loadLongStackPage(long pageOffset) {
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        byte[] page = dirtyStackPages.get(pageOffset);
        if(page==null) {
            int pageSize = (int) (parity4Get(vol.getLong(pageOffset + 4))>>>48);
            page = new byte[pageSize];
            vol.getData(pageOffset,page,0,pageSize);
            dirtyStackPages.put(pageOffset,page);
        }
        return page;
    }

    @Override
    protected void longStackNewPage(long masterLinkOffset, long prevPageOffset, long value) {
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        long newPageOffset = freeDataTakeSingle((int) CHUNKSIZE);
        byte[] page = new byte[(int) CHUNKSIZE];
        vol.getData(newPageOffset,page,0,page.length);
        dirtyStackPages.put(newPageOffset,page);
        //write size of current chunk with link to prev page
        DataIO.putLong(page,4,parity4Set((CHUNKSIZE<<48) | prevPageOffset));
        //put value
        long currSize = 12 + DataIO.packLongBidi(page, 12, parity1Set(value << 1));
        //update master pointer
        headVol.putLong(masterLinkOffset, parity4Set((currSize<<48)|newPageOffset));
    }

    @Override
    protected void flush() {
        if(isReadOnly())
            return;
        structuralLock.lock();
        try{
            //flush modified Long Stack pages
            LongMap.LongMapIterator<byte[]> iter =dirtyStackPages.longMapIterator();
            while(iter.moveToNext()){
                long offset = iter.key();
                byte[] val = iter.value();

                if(CC.PARANOID && offset<PAGE_SIZE)
                    throw new AssertionError();
                if(CC.PARANOID && val.length%16!=0)
                    throw new AssertionError();
                if(CC.PARANOID && val.length<=0||val.length>MAX_REC_SIZE)
                    throw new AssertionError();

                vol.putData(offset,val,0,val.length);
                iter.remove();
            }

            //set header checksum
            headVol.putInt(HEAD_CHECKSUM, headChecksum(headVol));
            //and flush head
            byte[] buf = new byte[(int) HEAD_END]; //TODO copy directly
            headVol.getData(0,buf,0,buf.length);
            vol.putData(0,buf,0,buf.length);
        }finally {
            structuralLock.unlock();
        }
        vol.sync();
    }

    @Override
    protected void initHeadVol() {
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        this.headVol = new Volume.ByteArrayVol(CC.VOLUME_PAGE_SHIFT);
        //TODO limit size
        //TODO introduce SingleByteArrayVol which uses only single byte[]

        byte[] buf = new byte[(int) HEAD_END]; //TODO copy directly
        vol.getData(0,buf,0,buf.length);
        headVol.ensureAvailable(buf.length);
        headVol.putData(0,buf,0,buf.length);
    }
}
