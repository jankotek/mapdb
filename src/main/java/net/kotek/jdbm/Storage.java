package net.kotek.jdbm;


import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class Storage implements  RecordManager{



    static final long PHYS_OFFSET_MASK = 0x0000FFFFFFFFFFFFL;


    /** File header. First 4 bytes are 'JDBM', last two bytes are store format version */
    static final long HEADER = (long)'J' <<(8*7)  + (long)'D' <<(8*6) + (long)'B' <<(8*5) + (long)'M' <<(8*4) + CC.STORE_FORMAT_VERSION;


    static final int RECID_CURRENT_PHYS_FILE_SIZE = 1;
    static final int RECID_CURRENT_INDEX_FILE_SIZE = 2;

    /** offset in index file which points to FREEINDEX list (free slots in index file) */
    static final int RECID_FREE_INDEX_SLOTS = 3;

    //TODO slots 4 to 18 are currently unused


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


    private final AtomicInteger writeLocksCounter;
    protected final boolean enableLocks;
    protected final boolean inMemory;
    protected final boolean deleteFilesOnExit;
    protected final boolean readOnly;
    protected final boolean appendOnly;

    protected final ReentrantReadWriteLock lock;


    protected ByteBuffer2 phys;
    protected ByteBuffer2 index;
    protected final File indexFile;




    public Storage(File indexFile, boolean enableLocks, boolean deleteFilesAfterClose, boolean readOnly, boolean appendOnly) {
        this.indexFile = indexFile;
        this.enableLocks = enableLocks;
        this.deleteFilesOnExit = deleteFilesAfterClose;
        this.readOnly = readOnly;
        this.appendOnly = appendOnly;
        this.lock = enableLocks? new ReentrantReadWriteLock() : null;
        this.inMemory = indexFile == null;
        writeLocksCounter = CC.ASSERT && !enableLocks? new AtomicInteger(0) : null;

        try{
            writeLock_lock();

            File dataFile = inMemory? null : new File(indexFile.getPath()+ DATA_FILE_EXT);

            if(inMemory){
                phys = new ByteBuffer2(true, null,
                        readOnly? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE,
                        "phys");
                index = new ByteBuffer2(true, null,
                        readOnly? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE,
                        "index");
                writeInitValues();

            }else{

                RandomAccessFile indexRaf = new RandomAccessFile(indexFile, "rw");
                checkFileBeforeOpening(indexFile, indexRaf);

                RandomAccessFile dataRaf = new RandomAccessFile(dataFile, "rw");
                checkFileBeforeOpening(dataFile, dataRaf);
                boolean existed = indexFile.exists() && indexFile.length()>0;

                phys = new ByteBuffer2(false, dataRaf.getChannel(),
                        readOnly? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE,
                        "phys");
                index = new ByteBuffer2(false, indexRaf.getChannel(),
                        readOnly? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE,
                        "index");

                if(!existed){
                    //store does not exist, create files
                    writeInitValues();
                }
            }

        }catch (IOException e){
            throw new IOError(e);
        }finally {
            writeLock_unlock();
        }

    }

    private void checkFileBeforeOpening(File indexFile, RandomAccessFile indexRaf) throws IOException {
        if(indexFile.exists())
            if(!indexFile.isFile() || !indexFile.canRead())
                throw new IllegalAccessError("Could not read file: "+indexFile);
        long len = indexFile.length();

        if(len!=0&&(len<8 || indexRaf.readLong()!=HEADER)){
            throw new IllegalArgumentException("Invalid file header: "+indexFile);
        }
    }


    private void writeInitValues() {
        writeLock_checkLocked();
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
        if(enableLocks)
            lock.writeLock().lock();
        else if(CC.ASSERT && !enableLocks){
            int c = writeLocksCounter.incrementAndGet();
            if(c!=1) throw new InternalError("more then one writer");
        }


    }

    protected void writeLock_unlock() {
        if(enableLocks)
            lock.writeLock().unlock();
        else if(CC.ASSERT && !enableLocks){
            int c = writeLocksCounter.decrementAndGet();
            if(c!=0) throw new InternalError("more then one writer");
        }

    }

    protected void writeLock_checkLocked() {
        if(CC.ASSERT && enableLocks && !lock.writeLock().isHeldByCurrentThread())
            throw new IllegalAccessError("no write lock");
        if(CC.ASSERT && ! enableLocks && writeLocksCounter.get()>1)
            throw new InternalError("more then one writer");
    }



    protected void readLock_unlock() {
        if(CC.ASSERT && ! enableLocks && writeLocksCounter.get()!=0)
            throw new InternalError("writer operates");
        if(enableLocks)
            lock.readLock().unlock();
    }

    protected void readLock_lock() {
        if(enableLocks)
            lock.readLock().lock();
        if(CC.ASSERT && ! enableLocks && writeLocksCounter.get()!=0)
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

            if(deleteFilesOnExit && indexFile!=null){
                indexFile.delete();
                new File(indexFile.getPath()+DATA_FILE_EXT).delete();
            }

        }catch(IOException e){
            throw new IOError(e);
        }finally {
            writeLock_unlock();
        }
    }

    protected  <A> A recordGet2(long indexValue, ByteBuffer2 data, Serializer<A> serializer) throws IOException {
        final long dataPos = indexValue & PHYS_OFFSET_MASK;
        final int dataSize = (int) (indexValue>>>48);
        if(dataPos == 0) return null;

        DataInput2 in = data.getDataInput(dataPos, dataSize);
        final A value = serializer.deserialize(in,dataSize);

        if(CC.ASSERT &&  in.pos != dataSize +dataPos%ByteBuffer2.BUF_SIZE)
            throw new InternalError("Data were not fully read.");

        return value;
    }


    abstract protected long longStackTake(final long listRecid);

    abstract protected void longStackPut(final long listRecid, final long offset);

    abstract protected long freePhysRecTake(final int requiredSize);

    protected void freePhysRecPut(final long indexValue){
        if(CC.ASSERT && (indexValue &PHYS_OFFSET_MASK)==0) throw new InternalError("zero indexValue: ");
        final int size =  (int) (indexValue>>>48);

        final long listRecid = RECID_FREE_PHYS_RECORDS_START + freePhysRecSize2FreeSlot(size);
        longStackPut(listRecid, indexValue);
    }

    protected long findFreePhysSlot(int requiredSize) {
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



}
