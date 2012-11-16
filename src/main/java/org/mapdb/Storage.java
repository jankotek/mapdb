package org.mapdb;


import java.io.IOError;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class Storage implements Engine {



    static final long PHYS_OFFSET_MASK = 0x0000FFFFFFFFFFFFL;


    /** File header. First 4 bytes are 'JDBM', last two bytes are store format version */
    static final long HEADER = 5646556656456456L;


    static final int RECID_CURRENT_PHYS_FILE_SIZE = 1;
    static final int RECID_CURRENT_INDEX_FILE_SIZE = 2;


    /** offset in index file which points to FREEINDEX list (free slots in index file) */
    static final int RECID_FREE_INDEX_SLOTS = 3;

    static final int RECID_SERIALIZER = 4;

    //TODO slots 5 to 18 are currently unused

    static final int RECID_NAMED_RECODS = 19;


    static final int RECID_FREE_PHYS_RECORDS_START = 20;

    static final int NUMBER_OF_PHYS_FREE_SLOT =1000 + 1535;
    static final int MAX_RECORD_SIZE = 65535;

    /** must be smaller then 127 */
    static final byte LONG_STACK_NUM_OF_RECORDS_PER_PAGE = 100;

    static final int LONG_STACK_PAGE_SIZE =   8 + LONG_STACK_NUM_OF_RECORDS_PER_PAGE * 8;

    /** offset in index file from which normal physid starts */
    static final int INDEX_OFFSET_START = RECID_FREE_PHYS_RECORDS_START +NUMBER_OF_PHYS_FREE_SLOT;
    public static final String DATA_FILE_EXT = ".p";


    protected final ReentrantReadWriteLock lock;
    private final AtomicInteger writeLocksCounter;
    protected final boolean disableLocks;

    protected final boolean appendOnly;
    protected final boolean deleteFilesOnExit;
    protected final boolean failOnWrongHeader;

    protected Volume phys;
    protected Volume index;

    public Storage(Volume.VolumeFactory volFac, boolean disableLocks, boolean appendOnly,
                   boolean deleteFilesOnExit, boolean failOnWrongHeader) {

        this.disableLocks = disableLocks;
        this.appendOnly = appendOnly;
        this.deleteFilesOnExit = deleteFilesOnExit;
        this.failOnWrongHeader = failOnWrongHeader;
        this.lock = disableLocks? null: new ReentrantReadWriteLock();


        writeLocksCounter = CC.ASSERT && disableLocks? new AtomicInteger(0) : null;

        try{
            writeLock_lock();


            phys = volFac.createPhysVolume();
            index = volFac.createIndexVolume();
            phys.ensureAvailable(8);
            index.ensureAvailable(INDEX_OFFSET_START*8);

            final long header = index.getLong(0);
            if(header!=HEADER){
                if(failOnWrongHeader) throw new IOError(new IOException("Wrong file header"));
                else writeInitValues();
            }

        }finally {
            writeLock_unlock();
        }

    }


    private void writeInitValues() {
        writeLock_checkLocked();

        //zero out all index values
        for(int i=1;i<INDEX_OFFSET_START;i++){
            index.putLong(i*8, 0L);
        }

        //write headers
        phys.putLong(0, HEADER);
        index.putLong(0L,HEADER);


        //and set current sizes
        index.putLong(RECID_CURRENT_PHYS_FILE_SIZE * 8, 8L);
        index.putLong(RECID_CURRENT_INDEX_FILE_SIZE * 8, INDEX_OFFSET_START * 8);
        index.putLong(RECID_NAMED_RECODS*8,0);
    }


    @Override
    @SuppressWarnings("unchecked")
    public Long getNamedRecid(String name) {
        Map<String, Long> recids = (Map<String, Long>) recordGet(RECID_NAMED_RECODS, Serializer.BASIC_SERIALIZER);
        if(recids == null) return null;
        return recids.get(name);
    }

    protected final Object nameRecidLock = new Object();

    @Override
    @SuppressWarnings("unchecked")
    public void setNamedRecid(String name, Long recid) {
        synchronized (nameRecidLock){
            Map<String, Long> recids = (Map<String, Long>) recordGet(RECID_NAMED_RECODS, Serializer.BASIC_SERIALIZER);
            if(recids == null) recids = new HashMap<String, Long>();
            if(recid!=null)
                recids.put(name, recid);
            else
                recids.remove(name);
            recordUpdate(RECID_NAMED_RECODS, recids, Serializer.BASIC_SERIALIZER);
        }
    }

    protected void writeLock_lock() {
        if(!disableLocks)
            lock.writeLock().lock();
        else if(CC.ASSERT && disableLocks){
            int c = writeLocksCounter.incrementAndGet();
            if(c!=1) throw new InternalError("more then one writer");
        }


    }

    protected void writeLock_unlock() {
        if(!disableLocks)
            lock.writeLock().unlock();
        else if(CC.ASSERT && disableLocks){
            int c = writeLocksCounter.decrementAndGet();
            if(c!=0) throw new InternalError("more then one writer");
        }

    }

    protected void writeLock_checkLocked() {
        if(CC.ASSERT && !disableLocks && !lock.writeLock().isHeldByCurrentThread())
            throw new IllegalAccessError("no write lock");
        if(CC.ASSERT && disableLocks && writeLocksCounter.get()>1)
            throw new InternalError("more then one writer");
    }



    protected void readLock_unlock() {
        if(CC.ASSERT && disableLocks && writeLocksCounter.get()!=0)
            throw new InternalError("writer operates");
        if(!disableLocks)
            lock.readLock().unlock();
    }

    protected void readLock_lock() {
        if(!disableLocks)
            lock.readLock().lock();
        if(CC.ASSERT && disableLocks && writeLocksCounter.get()!=0)
            throw new InternalError("writer operates");

    }

    final int freePhysRecSize2FreeSlot(final int size){
        if(CC.ASSERT && size>MAX_RECORD_SIZE) throw new IllegalArgumentException("too big record");
        if(CC.ASSERT && size<0) throw new IllegalArgumentException("negative size");

        if(size<1535)
            return size-1;
        else if(size == MAX_RECORD_SIZE)
            return NUMBER_OF_PHYS_FREE_SLOT-1;
        else
            return 1535 -1 + (size-1535)/64;
    }

    @Override
    public void close() {
        try{
            writeLock_lock();

            phys.close();
            index.close();
            if(deleteFilesOnExit){
                phys.deleteFile();
                index.deleteFile();
            }
            phys = null;
            index = null;

        }finally {
            writeLock_unlock();
        }
    }

    protected  <A> A recordGet2(long indexValue, Volume data, Serializer<A> serializer) throws IOException {
        final long dataPos = indexValue & PHYS_OFFSET_MASK;
        final int dataSize = (int) (indexValue>>>48);
        if(dataPos == 0) return null;

        DataInput2 in = data.getDataInput(dataPos, dataSize);
        final A value = serializer.deserialize(in,dataSize);

        if(CC.ASSERT &&  in.pos != dataSize +dataPos%Volume.BUF_SIZE)
            throw new InternalError("Data were not fully read.");

        return value;
    }


    abstract protected long longStackTake(final long listRecid) throws IOException;

    abstract protected void longStackPut(final long listRecid, final long offset) throws IOException;

    abstract protected long freePhysRecTake(final int requiredSize) throws IOException;

    protected void freePhysRecPut(final long indexValue) throws IOException {
        if(CC.ASSERT && (indexValue &PHYS_OFFSET_MASK)==0) throw new InternalError("zero indexValue: ");
        final int size =  (int) (indexValue>>>48);

        final long listRecid = RECID_FREE_PHYS_RECORDS_START + freePhysRecSize2FreeSlot(size);
        longStackPut(listRecid, indexValue);
    }

    protected long findFreePhysSlot(int requiredSize) throws IOException {
        int slot = freePhysRecSize2FreeSlot(requiredSize);
        //check if this slot can contain smaller records,
        if(requiredSize>1 && slot==freePhysRecSize2FreeSlot(requiredSize-1))
            slot ++; //yes, in this case we have to start at next slot with bigger record and divide it

        while(slot< NUMBER_OF_PHYS_FREE_SLOT){

            final long v = longStackTake(RECID_FREE_PHYS_RECORDS_START +slot);
            if(v!=0){
                //we found it, check if we need to split record
                final int foundRecSize = (int) (v>>>48);
                if(foundRecSize!=requiredSize){

                    //yes we need split
                    final long newIndexValue =
                            ((long)(foundRecSize - requiredSize)<<48) | //encode size into new free record
                                    (v & PHYS_OFFSET_MASK) +   requiredSize; //and encode new free record phys offset
                    freePhysRecPut(newIndexValue);
                }

                //return offset combined with required size
                return (v & PHYS_OFFSET_MASK) |
                        (((long)requiredSize)<<48);
            }else{
                slot++;
            }
        }
        return 0;

    }


    @Override
    public long serializerRecid() {
        return RECID_SERIALIZER;
    }



}
