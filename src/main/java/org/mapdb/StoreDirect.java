package org.mapdb;

import java.io.DataInput;
import java.io.IOError;
import java.io.IOException;
import java.util.Arrays;

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
    /** offset of maximal allocated recid. It is <<3 parity1*/
    protected static final long MAX_RECID_OFFSET = 8*3;
    protected static final long INDEX_PAGE = 8*4;


    protected static final int MAX_REC_SIZE = 0xFFFF;
    /** number of free physical slots */
    protected static final int SLOTS_COUNT = (MAX_REC_SIZE+1)/16;

    protected static final long HEAD_END = INDEX_PAGE + SLOTS_COUNT * 8;

    protected static final long INITCRC_INDEX_PAGE = 4329042389490239043L;


    protected Volume vol;

    //TODO this only grows under structural lock, but reads are outside structural lock, perhaps volatile?
    protected long[] indexPages;

    protected long lastAllocatedData=0;

    public StoreDirect(String fileName,
                       Fun.Function1<Volume, String> volumeFactory,
                       boolean checksum,
                       boolean compress,
                       byte[] password,
                       boolean readonly,
                       boolean deleteFilesAfterClose,
                       int freeSpaceReclaimQ,
                       boolean commitFileSyncDisable,
                       int sizeIncrement
                       ) {
        super(fileName,volumeFactory,checksum,compress,password,readonly);
        this.vol = volumeFactory.run(fileName);
        structuralLock.lock();
        try{
            if(vol.isEmpty()) {
                //create initial structure

                //create new store
                indexPages = new long[]{0};

                vol.ensureAvailable(PAGE_SIZE);
                vol.clear(0, PAGE_SIZE);

                //set sizes
                vol.putLong(STORE_SIZE, parity16Set(PAGE_SIZE));
                vol.putLong(MAX_RECID_OFFSET, parity3Set(RECID_LAST_RESERVED * 8));
                vol.putLong(INDEX_PAGE, parity16Set(0));

                //put reserved recids
                for(long recid=1;recid<RECID_FIRST;recid++){
                    indexValPut(recid,0,0,true,false);
                }

                //and set header checksum
                vol.putInt(HEAD_CHECKSUM, headChecksum());
                vol.sync();

                lastAllocatedData = 0L;
            }else {
                //TODO header
                //TOOD feature bit field

                //check head checksum
                int expectedChecksum = vol.getInt(HEAD_CHECKSUM);
                int actualChecksum = headChecksum();
                if (actualChecksum != expectedChecksum) {
                    throw new InternalError("Head checksum broken");
                }

                //load index pages
                long[] ip = new long[]{0};
                long indexPage = parity16Get(vol.getLong(INDEX_PAGE));
                int i=1;
                for(;indexPage!=0;i++){
                    if(CC.PARANOID && indexPage%PAGE_SIZE!=0)
                        throw new AssertionError();
                    if(ip.length==i){
                        ip = Arrays.copyOf(ip,ip.length*4);
                    }
                    ip[i] = indexPage;
                    //checksum
                    if(CC.STORE_INDEX_CRC){
                        long res = INITCRC_INDEX_PAGE;
                        for(long j=0;j<PAGE_SIZE-8;j+=8){
                            res+=vol.getLong(indexPage+j);
                        }
                        if(res!=vol.getLong(indexPage+PAGE_SIZE-8))
                            throw new InternalError("Page CRC error at offset: "+indexPage);
                    }

                    //move to next page
                    indexPage = parity16Get(vol.getLong(indexPage+PAGE_SIZE_M16));
                }
                indexPages = Arrays.copyOf(ip,i);

            }
        } finally {
            structuralLock.unlock();
        }

    }

    public StoreDirect(String fileName) {
        this(fileName, fileName==null? Volume.memoryFactory() : Volume.fileFactory(),
                false,false,null,false,false,0,
                false,0);
    }

    protected int headChecksum() {
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        int ret = 0;
        for(int offset = 8;offset<HEAD_END;offset+=8){
            //TODO include some recids in checksum
            ret = ret*31 + DataIO.longHash(vol.getLong(offset));
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

            DataInput in = new DataInputByteArray(b);
            return deserialize(serializer, totalSize, in);
        }
    }

    protected int offsetsTotalSize(long[] offsets) {
        if(offsets==null)
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
            assertWriteLocked(recid);

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
                freeDataPut(oldOffsets);
                newOffsets = newSize==0?null:freeDataTake(out.pos);

            } finally {
                structuralLock.unlock();
            }
        }

        if(CC.PARANOID)
            offsetsVerify(newOffsets);

        putData(recid, newOffsets,out);
    }

    protected void offsetsVerify(long[] linkedOffsets) {
        //TODO check non tail records are mod 16
        //TODO check linkage
    }


    /** return positions of (possibly) linked record */
    protected long[] offsetsGet(long recid) {
        long indexVal = indexValGet(recid);
        if(indexVal>>>48==0){

            return ((indexVal&MLINKED)!=0) ? null : new long[0];
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

    private void indexValPut(long recid, int size, long offset, boolean linked, boolean unused) {
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
        long[] offsets = offsetsGet(recid);
        structuralLock.lock();
        try {
            freeDataPut(offsets);
        }finally {
            structuralLock.unlock();
        }
        indexValPut(recid,0,0,false,false);
    }

    @Override
    public long getCurrSize() {
        return -1; //TODO currsize
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
        indexValPut(recid,0,0L,false,true);
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

        putData(recid,offsets, out);

        return recid;
    }

    protected void putData(long recid, long[] offsets, DataOutputByteArray out) {
        if(CC.PARANOID && offsetsTotalSize(offsets)!=(out==null?0:out.pos))
            throw new AssertionError("size mismatch");

        if(offsets!=null) {
            int outPos = 0;
            for (int i = 0; i < offsets.length; i++) {
                boolean last = (i == offsets.length - 1);
                if (CC.PARANOID && ((offsets[i] & MLINKED) == 0) != last)
                    throw new AssertionError("linked bit set wrong way");

                long offset = (offsets[i] & MOFFSET);
                if(CC.PARANOID && offset%16!=0)
                    throw new AssertionError("not alligned to 16");

                //write offset to next page
                if (!last) {
                    vol.putLong(offset, parity3Set(offsets[i + 1]));
                }

                int plus = (last?0:8);
                long size =  (offsets[i]>>>48) - plus;
                if(CC.PARANOID && ((size&0xFFFF)!=size || size==0))
                    throw new AssertionError("size mismatch");

                //System.out.println("SET "+(offset + plus)+ " - "+size + " - "+outPos);
                vol.putData(offset + plus, out.buf,outPos, (int)size);
                outPos += size;
            }
            if(CC.PARANOID && outPos!=out.pos)
                throw new AssertionError("size mismatch");
        }
        //update index val
        boolean firstLinked =
                (offsets!=null && offsets.length>1) || //too large record
                (out==null); //null records
        int firstSize = (int) (offsets==null? 0L : offsets[0]>>>48);
        long firstOffset =  offsets==null? 0L : offsets[0]&MOFFSET;
        indexValPut(recid,firstSize,firstOffset,firstLinked,false);
    }

    protected void freeDataPut(long[] linkedOffsets) {
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        //TODO add assertions here
        //TODO not yet implemented
    }


    protected void freeDataPut(long offset, int size) {
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.PARANOID && size%16!=0)
            throw new AssertionError();

        //TODO not implemented
    }


    protected long[] freeDataTake(int size) {
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();
        if(CC.PARANOID && size<=0)
            throw new AssertionError();

        //compose of multiple single records
        long[] ret = new long[0];
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


        //TODO free space reuse

        if(lastAllocatedData==0){
            //allocate new data page
            long page = pageAllocate();
            lastAllocatedData = page+size;
            return page;
        }

        //does record fit into rest of the page?
        if((lastAllocatedData%PAGE_SIZE + size)/PAGE_SIZE !=0){
            //throw away rest of the page and allocate new
            lastAllocatedData=0;
            freeDataTakeSingle(size);
        }
        //yes it fits here, increase pointer
        long ret = lastAllocatedData;
        lastAllocatedData+=size;

        if(CC.PARANOID && ret%16!=0)
            throw new AssertionError();
        if(CC.PARANOID && lastAllocatedData%16!=0)
            throw new AssertionError();

        return ret;
    }

    @Override
    public void close() {
        closed = true;
        commit();
        vol.close();
        vol = null;
    }


    @Override
    public void commit() {
        if(isReadOnly())
            return;
        structuralLock.lock();
        try{
            //and set header checksum
            vol.putInt(HEAD_CHECKSUM, headChecksum());
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

    }


    protected long indexValGet(long recid) {
        long indexVal = vol.getLong(recidToOffset(recid));
        //check parity and throw recid does not exist if broken
        try {
            return DataIO.parity1Get(indexVal);
        }catch(InternalError e){
            //TODO do not throw/catch exception
            throw new DBException(DBException.Code.ENGINE_GET_VOID);
        }
    }

    protected final long recidToOffset(long recid){
        if(CC.PARANOID && recid<=0)
            throw new AssertionError();
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
        offset = ((((long)size))<<48) |
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
        long currentRecid = parity3Get(vol.getLong(MAX_RECID_OFFSET));
        currentRecid+=8;
        vol.putLong(MAX_RECID_OFFSET,parity3Set(currentRecid));

        currentRecid/=8;
        //check if new index page has to be allocated
        if(recidTooLarge(currentRecid)){
            pageIndexExtend();
        }

        return currentRecid;
    }

    protected void pageIndexExtend() {
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        //allocate new index page
        long indexPage = pageAllocate();

        //add link to this page
        long nextPagePointerOffset =
                indexPages.length==1? INDEX_PAGE : //first index page
                indexPages[indexPages.length-1]+PAGE_SIZE_M16; //update link on previous page

        if(CC.STORE_INDEX_CRC && indexPages.length!=1){
            //update crc by increasing crc value
            long crc = vol.getLong(nextPagePointerOffset+8);
            crc-=vol.getLong(nextPagePointerOffset);
            crc+=parity16Set(indexPage);
            vol.putLong(nextPagePointerOffset+8,crc);
        }

        vol.putLong(nextPagePointerOffset, parity16Set(indexPage));

        //set zero link on next page
        vol.putLong(indexPage+PAGE_SIZE_M16,parity16Set(0));

        //set init crc value on new page
        if(CC.STORE_INDEX_CRC){
            vol.putLong(indexPage+PAGE_SIZE-8,INITCRC_INDEX_PAGE+parity16Set(0));
        }

        //put into index page array
        long[] indexPages2 = Arrays.copyOf(indexPages,indexPages.length+1);
        indexPages2[indexPages.length]=indexPage;
        indexPages = indexPages2;
    }

    protected long pageAllocate() {
        if(CC.PARANOID && !structuralLock.isHeldByCurrentThread())
            throw new AssertionError();

        long storeSize = parity16Get(vol.getLong(STORE_SIZE));
        vol.ensureAvailable(storeSize+PAGE_SIZE);
        vol.clear(storeSize,storeSize+PAGE_SIZE);
        vol.putLong(STORE_SIZE, parity16Set(storeSize + PAGE_SIZE));

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
