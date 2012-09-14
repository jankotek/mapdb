package net.kotek.jdbm;


import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RecordStore implements RecordManager {

    protected final boolean inMemory;

    private FileChannel dataFileChannel;
    private FileChannel indexFileChannel;

    protected ByteBuffer[] dataBufs = new ByteBuffer[8];
    protected ByteBuffer[] indexBufs = new ByteBuffer[8];

    static final int  BUF_SIZE = 1<<30;
    static final int BUF_SIZE_RECID = BUF_SIZE/8;

    static final int BUF_GROWTH = 1<<23;

    static final long PHYS_OFFSET_MASK = 0x0000FFFFFFFFFFFFL;




    final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** File header. First 4 bytes are 'JDBM', last two bytes are store format version */
    static final long HEADER = (long)'J' <<(8*7)  + (long)'D' <<(8*6) + (long)'B' <<(8*5) + (long)'M' <<(8*4) + CC.STORE_FORMAT_VERSION;


    static final int RECID_CURRENT_PHYS_FILE_SIZE = 1;
    static final int RECID_CURRENT_INDEX_FILE_SIZE = 2;

    /** offset in index file which points to FREEINDEX list (free slots in index file) */
    static final int RECID_FREE_INDEX_SLOTS = 3;

    //TODO slots 4 to 18 are currently unused
    static final int RECID_NAMED_RECODS = 4;
    /**
     * This recid is reserved for user usage. You may put whatever you want here
     * It is only used by JDBM during unit tests, not at production
     * */
    static final int RECID_USER_WHOTEVER =19;

    static final int RECID_FREE_PHYS_RECORDS_START = 20;

    static final int NUMBER_OF_PHYS_FREE_SLOT =1000 + 1535;

    /** minimal number of longs  to grow index file by, prevents to often buffer remapping*/
    static final int MINIMAL_INDEX_FILE_GROW = 1024;
    /** when index file overflows it is grown by NEWSIZE = SIZE + SIZE/N */
    static final int INDEX_FILE_GROW_FACTOR= 10;

    static final int MAX_RECORD_SIZE = 65535;




    /** must be smaller then 127 */
    static final byte LONG_STACK_NUM_OF_RECORDS_PER_PAGE = 100;

    static final int LONG_STACK_PAGE_SIZE =  8 + LONG_STACK_NUM_OF_RECORDS_PER_PAGE * 8;

    /** offset in index file from which normal physid starts */
    static final int INDEX_OFFSET_START = RECID_FREE_PHYS_RECORDS_START +NUMBER_OF_PHYS_FREE_SLOT;






    public RecordStore(String fileName) {


        this.inMemory = fileName ==null;
        try{
            writeLock_lock();


            File dataFile = inMemory? null : new File(fileName+".d");
            File indexFile = inMemory? null : new File(fileName+".i");

            if(inMemory){
                dataBufs[0] = ByteBuffer.allocate(1<<16);
                indexBufs[0] = ByteBuffer.allocate(1<<16);
                writeInitValues();

            }else if(!dataFile.exists() || dataFile.length()==0){
                //store does not exist, create files
                dataFileChannel = new RandomAccessFile(dataFile, "rw").getChannel();
                indexFileChannel =new RandomAccessFile(indexFile, "rw").getChannel();

                dataBufs[0] =  dataFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, BUF_GROWTH);
                indexBufs[0] =  indexFileChannel.map(FileChannel.MapMode.READ_WRITE, 0,BUF_GROWTH);
                writeInitValues();
            }else{
                dataFileChannel = new RandomAccessFile(dataFile, "rw").getChannel();
                indexFileChannel =new RandomAccessFile(indexFile, "rw").getChannel();

                //store exists, open
                final long dataFileSize = dataFileChannel.size();
                final long indexFileSize = indexFileChannel.size();
                //map zero buffers to check header
                dataBufs[0] =  dataFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Math.min(BUF_SIZE, dataFileSize));
                indexBufs[0] =  indexFileChannel.map(FileChannel.MapMode.READ_WRITE, 0,Math.min(BUF_SIZE, indexFileSize));

                //check headers
                if(CC.ASSERT){
                    if(dataFileSize<8 || indexFileSize<8 ||
                            dataBufs[0].getLong(0)!=HEADER ||
                            indexBufs[0].getLong(0)!=HEADER ){
                         throw new IOException("Wrong file header, probably not JDBM store.");
                    }
                }

                //now map remaining buffers
                long pos = 1;
                while(pos*BUF_SIZE<dataFileSize){
                    if(pos == dataBufs.length){
                        dataBufs = Arrays.copyOf(dataBufs, dataBufs.length*2);
                    }

                    long remSize = Math.min(dataFileSize-pos*BUF_SIZE, BUF_SIZE) ;
                    dataBufs[((int) pos)] = dataFileChannel.map(FileChannel.MapMode.READ_WRITE, pos*BUF_SIZE, remSize);
                    pos++;
                }

                pos = 1;
                while(pos*BUF_SIZE<indexFileSize){
                    if(pos == indexBufs.length){
                        indexBufs = Arrays.copyOf(indexBufs, indexBufs.length*2);
                    }

                    long remSize = Math.min(indexFileSize-pos*BUF_SIZE, BUF_SIZE) ;
                    indexBufs[((int) pos)] = indexFileChannel.map(FileChannel.MapMode.READ_WRITE, pos*BUF_SIZE, remSize);
                    pos++;
                }

            }


        }catch (IOException e){
            throw new IOError(e);
        }finally {
            writeLock_unlock();
        }

    }

    private void writeInitValues() {
        //write headers
        dataBufs[0].putLong(0, HEADER);
        indexValPut(0L,HEADER);

        //and set current sizes
        indexValPut(RECID_CURRENT_PHYS_FILE_SIZE, 8L);
        indexValPut(RECID_CURRENT_INDEX_FILE_SIZE, INDEX_OFFSET_START * 8);

        forceRecordUpdateOnGivenRecid(RECID_NAMED_RECODS, new byte[]{});
    }


    @Override
    public <A> long recordPut(A value, Serializer<A> serializer) {
        try{
            DataOutput2 out = new DataOutput2();
            serializer.serialize(out,value);
            if(CC.ASSERT && out.pos>1<<16) throw new InternalError("Record bigger then 64KB");

            try{
                writeLock_lock();
                //update index file
                long recid = freeRecidTake();

                //get physical record
                // first 16 bites is record size, remaining 48 bytes is record offset in phys file
                final long indexValue = out.pos!=0?
                        freePhysRecTake(out.pos):
                        0L;

                indexValPut(recid, indexValue);

                final long dataPos = indexValue & PHYS_OFFSET_MASK;

                final ByteBuffer dataBuf = dataBufs[((int) (dataPos / BUF_SIZE))];

                //set data cursor to desired position
                dataBuf.position((int) (dataPos%BUF_SIZE));
                //write data
                dataBuf.put(out.buf,0,out.pos);

                return recid;
            }finally {
                writeLock_unlock();
            }
        }catch(IOException e){
            throw new IOError(e);
        }
    }

    protected long freeRecidTake() throws IOException {
        writeLock_checkLocked();
        long recid = longStackTake(RECID_FREE_INDEX_SLOTS);
        if(recid == 0){
            //could not reuse recid, so create new one
            final long indexSize = indexValGet(RECID_CURRENT_INDEX_FILE_SIZE);
            recid = indexSize/8;
            if(CC.ASSERT && indexSize%8!=0) throw new InternalError();

            indexValPut(RECID_CURRENT_INDEX_FILE_SIZE, indexSize+8);

            //grow buffer if necessary
            final int indexSlot = (int) (indexSize/BUF_SIZE);
            ByteBuffer indexBuf =
                    indexSlot==indexBufs.length?
                            null:
                            indexBufs[indexSlot];
            if(indexBuf == null){
                //nothing was yet allocated at this position, so create new ByteBuffer
                if(CC.ASSERT && indexSize%BUF_SIZE!=0) throw new InternalError();
                indexBuf = indexFileChannel.map(FileChannel.MapMode.READ_WRITE, indexSize, BUF_GROWTH);
                //make sure array is big enought for new item
                if(indexSlot == indexBufs.length){
                    indexBufs = Arrays.copyOf(indexBufs, indexBufs.length * 2);
                }

                indexBufs[indexSlot] =  indexBuf;
            }else if(indexSize%BUF_SIZE>=indexBuf.capacity()){
                //grow buffer
                if(inMemory){
                    int newSize = Math.min(BUF_SIZE, indexBuf.capacity()*2);
                    ByteBuffer newBuf = ByteBuffer.allocate(newSize);
                    indexBuf.rewind();
                    newBuf.put(indexBuf);
                    indexBuf = newBuf;
                }else{
                    indexBuf = indexFileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        (indexSize/BUF_SIZE)*BUF_SIZE,
                        indexBuf.capacity() + BUF_GROWTH);
                }
                if(CC.ASSERT && indexBuf.capacity()>BUF_SIZE) throw new InternalError();
//                        //force old buffer to be written
//                        if(indexBuf instanceof MappedByteBuffer){
//                            ((MappedByteBuffer)indexBuf).force();
//                        }
                indexBufs[indexSlot] =  indexBuf;
            }

        }
        return recid;
    }


    protected void freeRecidPut(long recid) {
        longStackPut(RECID_FREE_INDEX_SLOTS, recid);
    }


    @Override
    public <A> A  recordGet(long recid, Serializer<A> serializer) {
        try{
            try{
                readLock_lock();

                final long indexValue = indexValGet(recid) ;
                final long dataPos = indexValue & PHYS_OFFSET_MASK;
                final int dataSize = (int) (indexValue>>>48);
                if(dataPos == 0) return null;

                final ByteBuffer dataBuf = dataBufs[((int) (dataPos / BUF_SIZE))];

                DataInput2 in = new DataInput2(dataBuf, (int) (dataPos%BUF_SIZE));
                final A value = serializer.deserialize(in,dataSize);

                if(CC.ASSERT &&  in.pos != dataPos%BUF_SIZE + dataSize)
                        throw new InternalError("Data were not fully read, recid:"+recid+", serializer:"+serializer);

                return value;
            }finally{
                readLock_unlock();
            }


        }catch(IOException e){
            throw new IOError(e);
        }
    }


    @Override
    public <A> void recordUpdate(long recid, A value, Serializer<A> serializer){
       try{
           DataOutput2 out = new DataOutput2();
           serializer.serialize(out,value);

           //TODO special handling for zero size records
           if(CC.ASSERT && out.pos>1<<16) throw new InternalError("Record bigger then 64KB");
           try{
               writeLock_lock();

               //check if size has changed
               final long oldIndexVal = indexValGet(recid);
               if(oldIndexVal >>>48 == out.pos ){
                   //size is the same, so just write new data
                   final long dataPos = oldIndexVal&PHYS_OFFSET_MASK;
                   final ByteBuffer dataBuf = dataBufs[((int) (dataPos / BUF_SIZE))];
                   dataBuf.position((int) (dataPos%BUF_SIZE));
                   dataBuf.put(out.buf,0,out.pos);
               }else{
                   //size has changed, so write into new location
                   final long newIndexValue = freePhysRecTake(out.pos);
                   final long dataPos = newIndexValue&PHYS_OFFSET_MASK;
                   final ByteBuffer dataBuf = dataBufs[((int) (dataPos / BUF_SIZE))];
                   dataBuf.position((int) (dataPos%BUF_SIZE));
                   dataBuf.put(out.buf,0,out.pos);
                   //update index file with new location
                   indexValPut(recid,newIndexValue);

                   //and set old phys record as free
                   if(oldIndexVal!=0)
                        freePhysRecPut(oldIndexVal);
               }
           }finally {
               writeLock_unlock();
           }
       }catch(IOException e){
           throw new IOError(e);
       }

   }

   @Override
   public void recordDelete(long recid){
        try{
            writeLock_lock();
            final long oldIndexVal = indexValGet(recid);
            indexValPut(recid, 0L);
            freeRecidPut(recid);
            if(oldIndexVal!=0)
                freePhysRecPut(oldIndexVal);
        }finally {
            writeLock_unlock();
        }
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


    @Override
    public void close() {
        try{
            writeLock_lock();
//            for(ByteBuffer b : dataBufs){
//                if(b instanceof MappedByteBuffer){
//                    ((MappedByteBuffer)b).force();
//                }
//            }
//            for(ByteBuffer b : indexBufs){
//                if(b instanceof MappedByteBuffer){
//                    ((MappedByteBuffer)b).force();
//                }
//            }

            dataBufs = null;
            indexBufs = null;

//            dataFileChannel.force(true);
            if(dataFileChannel!=null)
                dataFileChannel.close();
            dataFileChannel = null;
//            indexFileChannel.force(true);
            if(indexFileChannel!=null)
                indexFileChannel.close();
            indexFileChannel = null;

        }catch(IOException e){
            throw new IOError(e);
        }finally {
            writeLock_unlock();
        }
    }


    long longStackTake(final long listRecid) {
        final long listPhysid = indexValGet(listRecid) &PHYS_OFFSET_MASK;
        if(listPhysid == 0)
            return 0; //there is no such list, so just return 0

        writeLock_checkLocked();

        final int bufOffset = (int) (listPhysid%BUF_SIZE);
       final ByteBuffer dataBuf = dataBufs[((int) (listPhysid / BUF_SIZE))];

        final byte numberOfRecordsInPage = dataBuf.get(bufOffset);
        final long ret = dataBuf.getLong (bufOffset+numberOfRecordsInPage*8);

        //was it only record at that page?
        if(numberOfRecordsInPage == 1){
            //yes, delete this page
            final long previousListPhysid =dataBuf.getLong(bufOffset) &PHYS_OFFSET_MASK;
            if(previousListPhysid !=0){
                //update index so it points to previous page
                indexValPut(listRecid, previousListPhysid | (((long) LONG_STACK_PAGE_SIZE) << 48));
            }else{
                //zero out index
                indexValPut(listRecid, 0L);
            }
            //put space used by this page into free list
            freePhysRecPut(listPhysid | (((long)LONG_STACK_PAGE_SIZE)<<48));
        }else{
            //no, it was not last record at this page, so just decrement the counter
            dataBuf.put(bufOffset, (byte)(numberOfRecordsInPage-1));
        }
        return ret;

    }

   void longStackPut(final long listRecid, final long offset) {
       writeLock_checkLocked();

       //index position was cleared, put into free index list
        final long listPhysid2 = indexValGet(listRecid) &PHYS_OFFSET_MASK;

        if(listPhysid2 == 0){ //empty list?
            //yes empty, create new page and fill it with values
            final long listPhysid = freePhysRecTake(LONG_STACK_PAGE_SIZE) &PHYS_OFFSET_MASK;
            if(CC.ASSERT && listPhysid == 0) throw new InternalError();
            ByteBuffer dataBuf = dataBufs[((int) (listPhysid / BUF_SIZE))];
            //set previous Free Index List page to zero as this is first page
            dataBuf.putLong((int) (listPhysid%BUF_SIZE ), 0L);
            //set number of free records in this page to 1
            dataBuf.put((int)(listPhysid%BUF_SIZE),(byte)1);

            //set  record
            dataBuf.putLong((int) (listPhysid%BUF_SIZE  + 8), offset);
            //and update index file with new page location
            indexValPut(listRecid, (((long) LONG_STACK_PAGE_SIZE) << 48) | listPhysid);
        }else{

            final ByteBuffer dataBuf2 = dataBufs[((int) (listPhysid2 / BUF_SIZE))];
            final byte numberOfRecordsInPage = dataBuf2.get((int) (listPhysid2%BUF_SIZE));
            if(numberOfRecordsInPage == LONG_STACK_NUM_OF_RECORDS_PER_PAGE){ //is current page full?
                //yes it is full, so we need to allocate new page and write our number there

                final long listPhysid = freePhysRecTake(LONG_STACK_PAGE_SIZE) &PHYS_OFFSET_MASK;
                if(CC.ASSERT && listPhysid == 0) throw new InternalError();
                final ByteBuffer dataBuf = dataBufs[((int) (listPhysid / BUF_SIZE))];
                final int buffOffset =(int) (listPhysid%BUF_SIZE);
                //set location to previous page
                dataBuf.putLong(buffOffset, listPhysid2);
                //set number of free records in this page to 1
                dataBuf.put(buffOffset,(byte)1);
                //set free record
                dataBuf.putLong(buffOffset +  8, offset);
                //and update index file with new page location
                indexValPut(listRecid, (((long) LONG_STACK_PAGE_SIZE) << 48) | listPhysid);
            }else{
                //there is space on page, so just write released recid and increase the counter
                dataBuf2.putLong((int) (listPhysid2%BUF_SIZE +  8 + 8 * numberOfRecordsInPage), offset);
                dataBuf2.put((int) (listPhysid2%BUF_SIZE), (byte) (numberOfRecordsInPage+1));
            }
        }
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


    final long freePhysRecTake(final int requiredSize){
        writeLock_checkLocked();


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

        try{

        //No free records found, so lets increase the file size.
        //We need to take case of growing ByteBuffers.
        // Also max size of ByteBuffer is 2GB, so we need to use multiple ones

        final long physFileSize = indexValGet(RECID_CURRENT_PHYS_FILE_SIZE);
        if(CC.ASSERT && physFileSize <=0) throw new InternalError();

        if(physFileSize%BUF_SIZE+requiredSize<BUF_SIZE){
            //there is no need to overflow into new 2GB ByteBuffer.
            //so just increase file size
            indexValPut(RECID_CURRENT_PHYS_FILE_SIZE, physFileSize + requiredSize);

            //check that current mapped ByteBuffer is large enought, if not we need to grow and remap it
            final ByteBuffer dataBuf = dataBufs[((int) (physFileSize / BUF_SIZE))];
            if(physFileSize%BUF_SIZE+requiredSize>dataBuf.capacity()){
                //TODO optimize remap to grow slower
                int newCapacity = dataBuf.capacity();
                while(physFileSize%BUF_SIZE+requiredSize>newCapacity){
                    if(inMemory)
                        newCapacity*=2;
                    else
                        newCapacity+=BUF_GROWTH;
                }

                newCapacity = Math.min(BUF_SIZE, newCapacity);


                ByteBuffer dataBuf2;
                if(inMemory){
                    dataBuf2 = ByteBuffer.allocate(newCapacity);
                    dataBuf.rewind();
                    dataBuf2.put(dataBuf);
                }else{
                    dataBuf2 = dataFileChannel.map(FileChannel.MapMode.READ_WRITE,
                            ((physFileSize/BUF_SIZE)*BUF_SIZE),
                            newCapacity);
                }
                dataBufs[((int) (physFileSize / BUF_SIZE))]  = dataBuf2;

//                //force old buffer to be written
//                if(dataBuf instanceof MappedByteBuffer){
//                    ((MappedByteBuffer)dataBuf).force();
//                }

            }

            //and return this
            return (((long)requiredSize)<<48) | physFileSize;
        }else{
            //new size is overlapping 2GB ByteBuffer, so map second ByteBuffer
            final ByteBuffer dataBuf1 = dataBufs[((int) (physFileSize / BUF_SIZE))];
            if(CC.ASSERT && dataBuf1.capacity()!=BUF_SIZE) throw new InternalError();

            //required size does not fit into remaining chunk at dataBuf1, so lets create an free records
            final long  freeSizeToCreate = BUF_SIZE -  physFileSize%BUF_SIZE;
            if(CC.ASSERT && freeSizeToCreate == 0) throw new InternalError();

            final long nextBufferStartOffset = physFileSize + freeSizeToCreate;
            if(CC.ASSERT && nextBufferStartOffset%BUF_SIZE!=0) throw new InternalError();
            if(CC.ASSERT && dataBufs[((int) (nextBufferStartOffset / BUF_SIZE))]!=null) throw new InternalError();

            //allocate next ByteBuffer in row
            final ByteBuffer dataBuf2 = dataFileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    nextBufferStartOffset,
                    BUF_GROWTH
            );

            //grow array if necessary
            final int bufSlot = (int) (nextBufferStartOffset / BUF_SIZE);
            if(dataBufs.length == bufSlot){
                dataBufs = Arrays.copyOf(dataBufs,dataBufs.length*2);
            }
            dataBufs[bufSlot] =  dataBuf2;


            //increase the disk size
            indexValPut(RECID_CURRENT_PHYS_FILE_SIZE, physFileSize + freeSizeToCreate + requiredSize);

            //previous buffer was not fully filled, so mark it as free record
            freePhysRecPut(freeSizeToCreate<<48|physFileSize);

            //and finally return position at beginning of new buffer
            return (((long)requiredSize)<<48) | nextBufferStartOffset;
        }
        }catch(IOException e){
            throw new IOError(e);
        }

    }


    final void freePhysRecPut(final long indexValue){
        if(CC.ASSERT && (indexValue &PHYS_OFFSET_MASK)==0) throw new InternalError("zero indexValue: ");
        final int size =  (int) (indexValue>>>48);

        final long listRecid = RECID_FREE_PHYS_RECORDS_START + freePhysRecSize2FreeSlot(size);
        longStackPut(listRecid, indexValue);
    }

    final long indexValGet(final long recid) {
        return indexBufs[((int) (recid / BUF_SIZE_RECID))].getLong( (int) (recid%BUF_SIZE_RECID) * 8);
    }

    final void indexValPut(final long recid, final  long val) {
        indexBufs[((int) (recid / BUF_SIZE_RECID))].putLong((int) ((recid % BUF_SIZE_RECID) * 8), val);
    }


    protected void writeLock_lock() {
        lock.writeLock().lock();
    }

    protected void writeLock_unlock() {
        lock.writeLock().unlock();
    }

    protected void writeLock_checkLocked() {
        if(CC.ASSERT && !lock.writeLock().isHeldByCurrentThread()) throw new IllegalAccessError("no write lock");
    }



    protected void readLock_unlock() {
        lock.readLock().unlock();
    }

    protected void readLock_lock() {
        lock.readLock().lock();
    }


    protected void forceRecordUpdateOnGivenRecid(final long recid, final byte[] value) {
        try{
            writeLock_lock();
            //check file size
            final long currentIndexFileSize = indexValGet(RECID_CURRENT_INDEX_FILE_SIZE);
            if(recid * 8 >currentIndexFileSize){
                //TODO grow index file with buffers overflow
                long newIndexFileSize = recid*8;
                indexValPut(RECID_CURRENT_INDEX_FILE_SIZE, newIndexFileSize);
            }
            //size has changed, so write into new location
            final long newIndexValue = freePhysRecTake(value.length);
            final long dataPos = newIndexValue&PHYS_OFFSET_MASK;
            final ByteBuffer dataBuf = dataBufs[((int) (dataPos / BUF_SIZE))];
            dataBuf.position((int) (dataPos%BUF_SIZE));
            dataBuf.put(value);

            long oldIndexValue = indexValGet(recid);
            //update index file with new location
            indexValPut(recid,newIndexValue);

            //and set old phys record as free
            if(oldIndexValue!=0)
                freePhysRecPut(oldIndexValue);
        }finally {
            writeLock_unlock();
        }
    }
}