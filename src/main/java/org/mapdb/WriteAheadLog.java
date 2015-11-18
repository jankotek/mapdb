package org.mapdb;

import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * WAL shared between {@link StoreWAL} and {@link StoreAppend}
 */
public class WriteAheadLog {

    private static final Logger LOG = Logger.getLogger(WriteAheadLog.class.getName());

    /** 2 byte store version*/
    protected static final int WAL_STORE_VERSION = 100;

    /** 4 byte file header */
    protected static final int WAL_HEADER = (0x8A77<<16) | WAL_STORE_VERSION;


    protected static final long WAL_SEAL = 8234892392398238983L;

    protected static final int I_EOF = 0;
    protected static final int I_LONG = 1;
    protected static final int I_BYTE_ARRAY = 2;
    protected static final int I_SKIP_MANY = 3;
    protected static final int I_SKIP_SINGLE = 4;
    protected static final int I_RECORD = 5;
    protected static final int I_TOMBSTONE = 6;
    protected static final int I_PREALLOCATE = 7;
    protected static final int I_COMMIT = 8;
    protected static final int I_ROLLBACK = 9;

    protected static final long MAX_FILE_SIZE = 16L * 1024L * 1024L;
    protected static final long MAX_FILE_RESERVE = 16;


    protected final long featureBitMap;

    protected final int pointerOffsetBites=32;
    protected final long pointerOffsetMask = DataIO.fillLowBits(pointerOffsetBites);
    protected final int pointerSizeBites=16;
    protected final long pointerSizeMask = DataIO.fillLowBits(pointerSizeBites);
    protected final int pointerFileBites=16;
    protected final long pointerFileMask = DataIO.fillLowBits(pointerFileBites);

    protected int lastChecksum=0;
    protected long lastChecksumOffset=16;

    public WriteAheadLog(String fileName, Volume.VolumeFactory volumeFactory, long featureBitMap) {
        this.fileName = fileName;
        this.volumeFactory = volumeFactory;
        this.featureBitMap = featureBitMap;
    }

    public WriteAheadLog(String fileName) {
        this(
                fileName,
                fileName==null? CC.DEFAULT_MEMORY_VOLUME_FACTORY:CC.DEFAULT_FILE_VOLUME_FACTORY,
                0L
        );
    }


    public void initFailedCloseFiles() {
        if(walRec!=null){
            for(Volume v:walRec){
                if(v!=null && !v.isClosed())
                    v.close();
            }
            walRec.clear();
        }
        if(volumes!=null){
            for(Volume v:volumes){
                if(v!=null && !v.isClosed())
                    v.close();
            }
            volumes.clear();
        }
    }

    public void close() {
        for(Volume v:walRec){
            v.close();
        }

        walRec.clear();

        for(Volume v:volumes){
            v.close();
        }
        volumes.clear();
        curVol = null;
    }

    public void seal() {
        ensureFileReady(false);
        long finalOffset = allocate(0,1);
        curVol.ensureAvailable(finalOffset+1); //TODO overlap here
        //put EOF instruction
        curVol.putUnsignedByte(finalOffset, (I_EOF<<4) | (Long.bitCount(finalOffset)&15));
        //TODO EOF should contain checksum
        curVol.sync();
        //put wal seal
        curVol.putLong(8, WAL_SEAL);
        curVol.sync();
    }

    public void startNextFile() {
        fileNum++;
        String filewal = getWalFileName(""+fileNum);
        Volume nextVol = volumeFactory.makeVolume(filewal, false, true);

        nextVol.ensureAvailable(16);

        nextVol.putInt(0, WAL_HEADER);
        nextVol.putLong(8, featureBitMap);

        fileOffsetSet(16);
        volumes.add(nextVol);
        lastChecksum=0;
        lastChecksumOffset=0;

        curVol = nextVol;
    }

    public void rollback() {
        ensureFileReady(false);
        final int plusSize = +1+4;
        long walOffset2 = allocate(plusSize,0);

        Volume curVol2 = curVol;

        curVol2.ensureAvailable(walOffset2+plusSize);

        if(lastChecksumOffset==0)
            lastChecksumOffset=16;
        int checksum =  lastChecksum+checksum(curVol2, lastChecksumOffset, walOffset2);
        lastChecksumOffset=walOffset2+plusSize;
        lastChecksum = checksum;

        int parity = 1+Long.bitCount(walOffset2)+Integer.bitCount(checksum);
        parity &=15;
        curVol2.putUnsignedByte(walOffset2, (I_ROLLBACK << 4)|parity);
        walOffset2++;
        curVol2.putInt(walOffset2,checksum);
        curVol2.sync();
    }

    public void commit() {
        ensureFileReady(false);
        final int plusSize = +1+4;
        long walOffset2 = allocate(plusSize, 0);

        Volume curVol2 = curVol;

        curVol2.ensureAvailable(walOffset2+plusSize);

        if(lastChecksumOffset==0)
            lastChecksumOffset=16;
        if(walOffset2==lastChecksumOffset)
            return;
        int checksum =  lastChecksum+checksum(curVol2, lastChecksumOffset, walOffset2);
        lastChecksumOffset=walOffset2+plusSize;
        lastChecksum = checksum;

        int parity = 1+Long.bitCount(walOffset2)+Integer.bitCount(checksum);
        parity &=15;
        curVol2.putUnsignedByte(walOffset2, (I_COMMIT << 4)|parity);
        walOffset2++;
        curVol2.putInt(walOffset2,checksum);
        curVol2.sync();
    }

    protected int checksum(Volume vol, long startOffset, long endOffset){
        int ret =  DataIO.longHash(vol.hash(startOffset, endOffset-startOffset, 111L));
        return ret==0?1:ret;
    }

    public boolean fileLoad() {
        boolean ret=false;
        for(Volume vol:volumes){
            ret = vol.fileLoad();
        }
        return ret;
    }

    public void sync() {
        curVol.sync();
    }


    public interface WALReplay{

        void beforeReplayStart();

        void writeLong(long offset, long value);

        void writeRecord(long recid, long walId, Volume vol, long volOffset, int length);

        void writeByteArray(long offset, long walId, Volume vol, long volOffset, int length);

        void beforeDestroyWAL();

        void commit();

        void rollback();


        void writeTombstone(long recid);

        void writePreallocate(long recid);
    }

    /** does nothing */
    public static final WALReplay NOREPLAY = new WALReplay() {
        @Override
        public void beforeReplayStart() {
        }

        @Override
        public void writeLong(long offset, long value) {
        }

        @Override
        public void writeRecord(long recid, long walId, Volume vol, long volOffset, int length) {
        }

        @Override
        public void writeByteArray(long offset, long walId, Volume vol, long volOffset, int length) {
        }

        @Override
        public void beforeDestroyWAL() {
        }

        @Override
        public void commit() {
        }

        @Override
        public void rollback() {
        }

        @Override
        public void writeTombstone(long recid) {
        }

        @Override
        public void writePreallocate(long recid) {
        }
    };


    final String fileName;
    final Volume.VolumeFactory volumeFactory;


    protected volatile long fileOffset = 16;
    protected ReentrantLock fileOffsetLock = new ReentrantLock(CC.FAIR_LOCKS);

    protected final List<Volume> volumes = Collections.synchronizedList(new ArrayList<Volume>());


    /** record WALs, store recid-record pairs. Created during compaction when memory allocator is not available */
    protected final List<Volume> walRec = Collections.synchronizedList(new ArrayList<Volume>());

    protected Volume curVol;

    protected long fileNum = -1;

    /**
     * Allocate space in WAL
     *
     * @param reqSize space which can not cross page boundaries
     * @param optSize space which can cross page boundaries
     * @return allocated fileOffset
     */
    protected long allocate(final int reqSize, final int optSize){
        if(CC.ASSERT && reqSize>=StoreDirect.PAGE_SIZE)
            throw new AssertionError();
        fileOffsetLock.lock();
        try{
            while (fileOffset >>> CC.VOLUME_PAGE_SHIFT != (fileOffset + reqSize) >>> CC.VOLUME_PAGE_SHIFT) {
                int singleByteSkip = (I_SKIP_SINGLE << 4) | (Long.bitCount(fileOffset) & 15);
                curVol.putUnsignedByte(fileOffset, singleByteSkip);
                fileOffset++;
            }
            //long ret =  walPointer(0, fileNum, fileOffset);
            long ret = fileOffset;
            fileOffset+=reqSize+optSize;
            return ret;
        }finally{
            fileOffsetLock.unlock();
        }
    }

    protected void fileOffsetSet(long fileOffset){
        fileOffsetLock.lock();
        try{
            this.fileOffset = fileOffset;
        }finally {
            fileOffsetLock.unlock();
        }
    }
/*
    //does it overlap page boundaries?
    if((walOffset2>>>CC.VOLUME_PAGE_SHIFT)==(walOffset2+plusSize)>>>CC.VOLUME_PAGE_SHIFT){
        return false; //no, does not, all fine
    }
    new Exception("SKIP").printStackTrace();
    //put skip instruction until plusSize
    while(plusSize>0){
        int singleByteSkip = (I_SKIP_SINGLE<<4)|(Long.bitCount(walOffset2)&15);
        curVol.putUnsignedByte(walOffset2, singleByteSkip);
        walOffset2++;
        plusSize--;
    }
*/

    void open(WALReplay replay){
        //replay WAL files
        String wal0Name = getWalFileName("0");
//        String walCompSeal = getWalFileName("c");
//        boolean walCompSealExists =
//                walCompSeal!=null &&
//                        new File(walCompSeal).exists();

        if(/*walCompSealExists ||*/
                (wal0Name!=null &&
                        new File(wal0Name).exists())){

            //fill wal files
            for(int i=0;;i++){
                String wname = getWalFileName(""+i);
                if(!new File(wname).exists())
                    break;
                volumes.add(volumeFactory.makeVolume(wname, false, true));
            }

            long walId = replayWALSkipRollbacks(replay);
            fileNum = walPointerToFileNum(walId);
            curVol = volumes.get((int) fileNum);
            fileOffsetSet(walPointerToOffset(walId));


//            for(Volume v:walRec){
//                v.close();
//            }
            walRec.clear();
//            volumes.clear();
//            fileNum = volumes.size()-1;
//            curVol = volumes.get(fileNum);
//            startNextFile();

        }

    }


    /** replays wall, but skips section between rollbacks. That means only committed transactions will be passed to
     * replay callback
     */
    long replayWALSkipRollbacks(WALReplay replay) {
        replay.beforeReplayStart();

        long start = skipRollbacks(16);
        long ret =  start;
        commitLoop: while(start!=0){
            long fileNum2 = walPointerToFileNum(start);
            Volume wal = volumes.get((int) fileNum2);
            long pos = walPointerToOffset(start);
            ret = start;

            instLoop: for(;;) {
                int checksum = wal.getUnsignedByte(pos++);
                int instruction = checksum>>>4;
                checksum = (checksum&15);
                switch(instruction) {
                    case I_EOF: {
                        //EOF
                        if ((Long.bitCount(pos - 1) & 15) != checksum)
                            throw new DBException.DataCorruption("WAL corrupted "+fileNum2+" - "+pos);

                        if(CC.LOG_WAL_CONTENT && LOG.isLoggable(Level.FINER)){
                            LOG.log(Level.FINER, "WAL EOF: file="+fileNum2+", pos="+(pos-1));
                        }
                        //start at new file
                        start = walPointer(0, fileNum2 + 1, 16);
                        continue commitLoop;
                        //break;
                    }
                    case I_LONG:
                        pos = instLong(wal, pos, checksum, replay);
                        break;
                    case I_BYTE_ARRAY:
                        pos = instByteArray(wal, pos, checksum, fileNum2, replay);
                        break;
                    case I_SKIP_MANY: {
                        //skip N bytes
                        int skipN = wal.getInt(pos - 1) & 0xFFFFFF; //read 3 bytes

                        if(CC.LOG_WAL_CONTENT && LOG.isLoggable(Level.FINER))
                            LOG.log(Level.FINER, "WAL SKIPN: file="+fileNum2+", pos="+(pos-1)+", skipN="+skipN);

                        if ((Integer.bitCount(skipN) & 15) != checksum)
                            throw new  DBException.DataCorruption("WAL corrupted");
                        pos += 3 + skipN;
                        break;
                    }
                    case I_SKIP_SINGLE: {
                        if(CC.LOG_WAL_CONTENT && LOG.isLoggable(Level.FINER))
                            LOG.log(Level.FINER, "WAL SKIP: file="+fileNum2+", pos="+(pos-1));

                        //skip single byte
                        if ((Long.bitCount(pos - 1) & 15) != checksum)
                            throw new  DBException.DataCorruption("WAL corrupted");
                        break;
                    }
                    case I_RECORD:
                        pos = instRecord(wal, pos, checksum, fileNum2, replay);
                        break;
                    case I_TOMBSTONE:
                        pos = instTombstone(wal, pos, checksum, replay);
                        break;
                    case I_PREALLOCATE:
                        pos = instPreallocate(wal, pos, checksum, replay);
                        break;
                    case I_COMMIT: {
                        int checksum2 = wal.getInt(pos);
                        pos += 4;
                        if(CC.LOG_WAL_CONTENT && LOG.isLoggable(Level.FINER))
                            LOG.log(Level.FINER, "WAL COMMIT: file="+fileNum2+", pos="+(pos-5));

                        if (((1 + Long.bitCount(pos - 5) + Integer.bitCount(checksum2)) & 15) != checksum)
                            throw new  DBException.DataCorruption("WAL corrupted");
                        if(replay!=null)
                            replay.commit();
                        long currentPos = walPointer(0, fileNum2, pos);
                        ret = currentPos;
                        //skip next rollbacks if there are any
                        start = skipRollbacks(currentPos);
                        continue commitLoop;
                        //break
                    }
                    case I_ROLLBACK:
                        throw new DBException.DataCorruption("Rollback should be skipped");
                    default:
                        throw new  DBException.DataCorruption("WAL corrupted, unknown instruction");
                }

            }
        }

        Volume vol = volumes.get((int) walPointerToFileNum(ret));
        long offset = walPointerToOffset(ret);
        if(offset!=0 && offset!=vol.length()) {
            vol.clearOverlap(offset, vol.length());
            vol.sync();
        }
        return ret;
    }

    /**
     * Iterates log until it finds commit or rollback instruction. If commit instruction is found,
     * it returns starting offset. If rollback instruction is find, it continues, and returns offset
     * after last rollback. If no commit is found before end of log, it returns zero.
     *
     * @param start offset
     * @return offset after last rollback
     */
    long skipRollbacks(long start){
        long fileNum2 = walPointerToFileNum(start);
        long pos = walPointerToOffset(start);

        commitLoop:for(;;){
            if(volumes.size()<=fileNum2)
                return 0; //there will be no commit in this file
            Volume wal = volumes.get((int) fileNum2);
            if(wal.length()<16 /*|| wal.getLong(8)!=WAL_SEAL*/) {
                break commitLoop;
                //TODO better handling for corrupted logs
            }


            try{ for(;;) {
                int checksum = wal.getUnsignedByte(pos++);
                int instruction = checksum >>> 4;
                checksum = (checksum & 15);
                switch (instruction) {
                    case I_EOF: {
                        //EOF
                        if ((Long.bitCount(pos - 1) & 15) != checksum)
                            throw new  DBException.DataCorruption("WAL corrupted "+fileNum2+" - "+pos);
                        fileNum2++;
                        pos = 16;
                        //TODO check next file seal?
                        continue commitLoop;
                        //break;
                    }
                    case I_LONG:
                        pos = instLong(wal, pos, checksum, null);
                        break;
                    case I_BYTE_ARRAY:
                        pos = instByteArray(wal, pos, checksum, fileNum2, null);
                        break;
                    case I_SKIP_MANY: {
                        //skip N bytes
                        int skipN = wal.getInt(pos - 1) & 0xFFFFFF; //read 3 bytes
                        if ((Integer.bitCount(skipN) & 15) != checksum)
                            throw new  DBException.DataCorruption("WAL corrupted");
                        pos += 3 + skipN;
                        break;
                    }
                    case I_SKIP_SINGLE: {
                        //skip single byte
                        if ((Long.bitCount(pos - 1) & 15) != checksum)
                            throw new  DBException.DataCorruption("WAL corrupted");
                        break;
                    }
                    case I_RECORD:
                        pos = instRecord(wal, pos, checksum, fileNum2, null);
                        break;
                    case I_TOMBSTONE:
                        pos = instTombstone(wal, pos, checksum, null);
                        break;
                    case I_PREALLOCATE:
                        pos = instPreallocate(wal, pos, checksum, null);
                        break;
                    case I_COMMIT: {
                        int checksum2 = wal.getInt(pos);
                        pos += 4;
                        if (((1 + Long.bitCount(pos - 5) + Integer.bitCount(checksum2)) & 15) != checksum)
                            throw new  DBException.DataCorruption("WAL corrupted");
                        //TODO checksums
                        if(CC.LOG_WAL_CONTENT && LOG.isLoggable(Level.FINER))
                            LOG.log(Level.FINER, "WAL SKIP: ret="+start);
                        return start;
                        //break;
                    }
                    case I_ROLLBACK: {
                        int checksum2 = wal.getInt(pos);
                        pos += 4;
                        if (((1 + Long.bitCount(pos - 5) + Integer.bitCount(checksum2)) & 15) != checksum)
                            throw new  DBException.DataCorruption("WAL corrupted");


                        //rollback instruction pushes last valid to current offset
                        start = walPointer(0, fileNum2, pos);
                        continue commitLoop;
                        //break;
                    }
                    default:
                        throw new  DBException.DataCorruption("WAL corrupted, unknown instruction: "+pos);
                }
            }
            }catch(DBException e){
                LOG.log(Level.INFO, "WAL corrupted, skipping",e);
                return 0;
            }

        }

        if(CC.LOG_WAL_CONTENT && LOG.isLoggable(Level.FINER))
            LOG.log(Level.FINER, "WAL SKIP: ret=0");

        return 0;
    }

    void replayWAL(WALReplay replay){
        replay.beforeReplayStart();

        long fileNum2=-1;

        file:for(Volume wal:volumes){
            fileNum2++;
            if(wal.length()<16 /*|| wal.getLong(8)!=WAL_SEAL*/) {
                break file;
                //TODO better handling for corrupted logs
            }

            long pos = 16;
            instLoop: for(;;) {
                int checksum = wal.getUnsignedByte(pos++);
                int instruction = checksum>>>4;
                checksum = (checksum&15);
                switch(instruction){
                    case I_EOF: {
                        //EOF
                        if ((Long.bitCount(pos - 1) & 15) != checksum)
                            throw new  DBException.DataCorruption("WAL corrupted");
                        continue file;
                    }
                    case I_LONG:
                        pos = instLong(wal, pos, checksum, replay);
                        break;
                    case I_BYTE_ARRAY:
                        pos = instByteArray(wal, pos, checksum, fileNum2, replay);
                        break;
                    case I_SKIP_MANY: {
                        //skip N bytes
                        int skipN = wal.getInt(pos - 1) & 0xFFFFFF; //read 3 bytes
                        if ((Integer.bitCount(skipN) & 15) != checksum)
                            throw new  DBException.DataCorruption("WAL corrupted");
                        pos += 3 + skipN;
                        break;
                    }
                    case I_SKIP_SINGLE: {
                        //skip single byte
                        if ((Long.bitCount(pos - 1) & 15) != checksum)
                            throw new  DBException.DataCorruption("WAL corrupted");
                        break;
                    }
                    case I_RECORD:
                        pos = instRecord(wal, pos, checksum, fileNum2, replay);
                        break;
                    case I_TOMBSTONE:
                        pos = instTombstone(wal, pos, checksum, replay);
                        break;
                    case I_PREALLOCATE:
                        pos = instPreallocate(wal, pos, checksum, replay);
                        break;
                    case I_COMMIT: {
                        int checksum2 = wal.getInt(pos);
                        pos += 4;
                        if (((1 + Long.bitCount(pos - 5) + Integer.bitCount(checksum2)) & 15) != checksum)
                            throw new  DBException.DataCorruption("WAL corrupted");
                        replay.commit();
                        break;
                    }
                    case I_ROLLBACK: {
                        int checksum2 = wal.getInt(pos);
                        pos += 4;
                        if (((1 + Long.bitCount(pos - 5) + Integer.bitCount(checksum2)) & 15) != checksum)
                            throw new  DBException.DataCorruption("WAL corrupted");
                        replay.rollback();
                        break;
                    }
                    default:
                        throw new  DBException.DataCorruption("WAL corrupted, unknown instruction");
                }

            }
        }
        replay.beforeDestroyWAL();
    }

    private long instTombstone(Volume wal, long pos, int checksum, WALReplay replay) {
        long recid = wal.getPackedLong(pos);
        pos += recid >>> 60;
        recid &= DataIO.PACK_LONG_RESULT_MASK;

        if(CC.LOG_WAL_CONTENT && LOG.isLoggable(Level.FINER))
            LOG.log(Level.FINER, "WAL TOMBSTONE: pos="+(pos-1-DataIO.packLongSize(recid))+", recid="+recid);

        if(((1+Long.bitCount(recid))&15)!=checksum)
            throw new  DBException.DataCorruption("WAL corrupted");

        if(replay!=null)
            replay.writeTombstone(recid);
        return pos;
    }

    private long instPreallocate(Volume wal, long pos, int checksum, WALReplay replay) {
        long recid = wal.getPackedLong(pos);
        pos += recid >>> 60;
        recid &= DataIO.PACK_LONG_RESULT_MASK;

        if(CC.LOG_WAL_CONTENT && LOG.isLoggable(Level.FINER))
            LOG.log(Level.FINER, "WAL PREALLOC: pos="+(pos-1-DataIO.packLongSize(recid))+", recid="+recid);


        if (((1 + Long.bitCount(recid)) & 15) != checksum)
            throw new  DBException.DataCorruption("WAL corrupted: "+pos);
        if(replay!=null)
            replay.writePreallocate(recid);
        return pos;
    }

    private long instRecord(Volume wal, long pos, int checksum, long fileNum2, WALReplay replay) {
        long pos2 = pos-1;
        long walId = walPointer(0, fileNum2, pos2);

        // read record
        long recid = wal.getPackedLong(pos);
        pos += recid >>> 60;
        recid &= DataIO.PACK_LONG_RESULT_MASK;

        long size = wal.getPackedLong(pos);
        pos += size >>> 60;
        size &= DataIO.PACK_LONG_RESULT_MASK;

        if(CC.LOG_WAL_CONTENT && LOG.isLoggable(Level.FINER))
            LOG.log(Level.FINER, "WAL RECORD: pos="+(pos2)+", recid="+recid+", size="+size);

        if(((1+Long.bitCount(recid)+Long.bitCount(size)+Long.bitCount(pos2))&15)!=checksum){
            throw new  DBException.DataCorruption("WAL corrupted");
        }

        if (size == 0) {
            if(replay!=null)
                replay.writeRecord(recid, 0, null, 0 ,0);
        } else {
            size--; //zero is used for null
//                        byte[] data = new byte[(int) size];
//                        wal.getData(pos, data, 0, data.length);
            if(replay!=null)
                replay.writeRecord(recid, walId, wal, pos, (int) size);
            pos += size;
        }
        return pos;
    }

    private long instByteArray(Volume wal, long pos, int checksum, long fileNum2, WALReplay replay) {
        //write byte[]
        long walId = walPointer(0, fileNum2, pos-1);

        int dataSize = wal.getUnsignedShort(pos);
        pos += 2;
        long offset = wal.getSixLong(pos);
        pos += 6;
//                    byte[] data = new byte[dataSize];
//                    wal.getData(pos, data, 0, data.length);
        if(CC.LOG_WAL_CONTENT && LOG.isLoggable(Level.FINER))
            LOG.log(Level.FINER, "WAL BYTE[]: pos="+(pos-1-8)+", size="+dataSize+", offset="+offset);


        if(((1+Integer.bitCount(dataSize)+Long.bitCount(offset))&15)!=checksum)
            throw new  DBException.DataCorruption("WAL corrupted");
        long val = ((long)fileNum)<<(pointerOffsetBites);
        val |=pos;

        if(replay!=null)
            replay.writeByteArray(offset, walId, wal, pos, dataSize);

        pos += dataSize;
        return pos;
    }

    private long instLong(Volume wal, long pos, int checksum, WALReplay replay) {
        //write long
        long val = wal.getLong(pos);
        pos += 8;
        long offset = wal.getSixLong(pos);
        pos += 6;

        if(CC.LOG_WAL_CONTENT && LOG.isLoggable(Level.FINER))
            LOG.log(Level.FINER, "WAL LONG: pos="+(pos-1-8-6)+", val="+val+", offset="+offset);

        if(((1+Long.bitCount(val)+Long.bitCount(offset))&15)!=checksum)
            throw new  DBException.DataCorruption("WAL corrupted");
        if(replay!=null)
            replay.writeLong(offset,val);
        return pos;
    }

    public void destroyWalFiles() {
        //destroy old wal files
        for(Volume wal:volumes){
            if(!wal.isClosed()) {
                wal.truncate(0);
                wal.close();
            }
            wal.deleteFile();
        }
        fileNum = -1;
        curVol = null;
        volumes.clear();
    }

    protected String getWalFileName(String ext) {
        return fileName==null? null :
                fileName+".wal"+"."+ext;
    }


    public long getNumberOfFiles(){
        return volumes.size();
    }

    /**
     * Retrieve {@code DataInput} from WAL. This data were written by {@link WriteAheadLog#walPutByteArray(long, byte[], int, int)}
     *
     * @param walPointer pointer returned by {@link WriteAheadLog#walPutByteArray(long, byte[], int, int)}
     * @return DataInput
     */
    public DataInput walGetByteArray(long walPointer) {
        int arraySize = walPointerToSize(walPointer);
        int fileNum = (int) (walPointerToFileNum(walPointer));
        long dataOffset = (walPointerToOffset(walPointer));

        Volume vol = volumes.get(fileNum);
        return vol.getDataInput(dataOffset, arraySize);
    }


    /**
     * Retrieve {@code byte[]} from WAL. This data were written by {@link WriteAheadLog#walPutByteArray(long, byte[], int, int)}
     *
     * @param walPointer pointer returned by {@link WriteAheadLog#walPutByteArray(long, byte[], int, int)}
     * @return DataInput
     */
    public byte[] walGetByteArray2(long walPointer) {
        int arraySize = walPointerToSize(walPointer);
        long fileNum = walPointerToFileNum(walPointer);
        long dataOffset = walPointerToOffset(walPointer);

        Volume vol = volumes.get((int) fileNum);
        byte[] ret = new byte[arraySize];
        vol.getData(dataOffset, ret, 0, arraySize);
        return ret;
    }

    protected long walPointerToOffset(long walPointer) {
        return walPointer & pointerOffsetMask;
    }

    protected long walPointerToFileNum(long walPointer) {
        return (walPointer >>> (pointerOffsetBites)) & pointerFileMask;
    }

    protected int walPointerToSize(long walPointer) {
        return (int) ((walPointer >>> (pointerOffsetBites+pointerFileBites))&pointerSizeMask);
    }

    //TODO return DataInput
    synchronized public byte[] walGetRecord(long walPointer, long expectedRecid) {
        long fileNum = walPointerToFileNum(walPointer);
        long dataOffset = (walPointerToOffset(walPointer));

        Volume vol = volumes.get((int) fileNum);
        //skip instruction
        //TODO verify it is 7
        //TODO verify checksum
        dataOffset++;

        long recid = vol.getPackedLong(dataOffset);
        dataOffset += recid >>> 60;
        recid &= DataIO.PACK_LONG_RESULT_MASK;

        if(CC.ASSERT && expectedRecid!=0 && recid!=expectedRecid){
            throw new AssertionError();
        }

        long size = vol.getPackedLong(dataOffset);
        dataOffset += size >>> 60;
        size &= DataIO.PACK_LONG_RESULT_MASK;

        if (size == 0) {
            return null;
        }else if(size==1){
            return new byte[0];
        }else {
            size--; //zero is used for null
            byte[] data = new byte[(int) size];
            DataInput in = vol.getDataInputOverlap(dataOffset, data.length);
            try {
                in.readFully(data);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
            return data;
        }
    }


    /**
     * Puts instruction into WAL. It should write part of {@code byte[]} at given offset.
     * This value returns pointer to WAL, which can be used to retrieve data back with {@link WriteAheadLog#walGetByteArray(long)}.
     * Pointer is composed of file number, and offset in WAL file.
     *
     * @param offset where data will be written in main store, after WAL replay (6 bytes)
     * @param buf byte array of data
     * @param bufPos starting position within byte array
     * @param size number of bytes to take from byte array
     * @return
     */
    public long walPutByteArray(long offset, byte[] buf, int bufPos, int size){
        ensureFileReady(true);
        final int plusSize = +1+2+6+size;
        long walOffset2 = allocate(plusSize,0);

        curVol.ensureAvailable(walOffset2+plusSize);
        int checksum = 1+Integer.bitCount(size)+Long.bitCount(offset);
        checksum &= 15;
        curVol.putUnsignedByte(walOffset2, (I_BYTE_ARRAY << 4)|checksum);
        walOffset2+=1;
        if(CC.ASSERT && (size&0xFFFF)!=size)
            throw new AssertionError();
        curVol.putLong(walOffset2, ((long) size) << 48 | offset);
        walOffset2+=8;
        curVol.putData(walOffset2, buf,bufPos,size);

        if(CC.ASSERT && (size&pointerSizeMask)!=size)
            throw new AssertionError();
        if(CC.ASSERT && (fileNum&pointerFileMask)!=fileNum)
            throw new AssertionError();
        if(CC.ASSERT && (walPointerToOffset(walOffset2))!=walOffset2)
            throw new AssertionError();

        return walPointer(size,fileNum,walOffset2);
    }

    protected long walPointer(long size, long fileNum, long offset){
        long val = (size)<<(pointerOffsetBites+pointerFileBites);
        val |= (fileNum)<<(pointerOffsetBites);
        val |= offset;

        if(CC.ASSERT && offset!=walPointerToOffset(val))
            throw new AssertionError();
        if(CC.ASSERT && fileNum!=walPointerToOffset(fileNum))
            throw new AssertionError();
        if(CC.ASSERT && size!=walPointerToOffset(size))
            throw new AssertionError();

        return val;
    }

    //TODO walPutRecord and walGetRecord are both synchronized, that is just broken
    synchronized public long walPutRecord(long recid, byte[] buf, int bufPos, int size){
        if(CC.ASSERT && buf==null && size!=0)
            throw new AssertionError();
        ensureFileReady(true);
        long sizeToWrite = buf==null?0:(size+1);
        final int plusSize = +1+ DataIO.packLongSize(recid)+DataIO.packLongSize(sizeToWrite)+size;
        long walOffset2 = allocate(plusSize-size, size);
        long startPos = walOffset2;
        if(CC.ASSERT && startPos>=MAX_FILE_SIZE)
            throw new AssertionError();


        curVol.ensureAvailable(walOffset2+plusSize);
        int checksum = 1+Long.bitCount(recid)+Long.bitCount(sizeToWrite)+Long.bitCount(walOffset2);
        checksum &= 15;
        curVol.putUnsignedByte(walOffset2, (I_RECORD << 4)|checksum);
        walOffset2++;

        walOffset2+=curVol.putPackedLong(walOffset2, recid);
        walOffset2+=curVol.putPackedLong(walOffset2, sizeToWrite);

        if(buf!=null) {
            curVol.putDataOverlap(walOffset2, buf, bufPos, size);
        }

        long ret =  walPointer(0, fileNum,startPos);
        return ret;
    }


    /**
     * Put 8 byte long into WAL.
     *
     * @param offset where data will be written in main store, after WAL replay (6 bytes)
     * @param value
     */
    protected void walPutLong(long offset, long value){
        ensureFileReady(false);
        final int plusSize = +1+8+6;
        long walOffset2 = allocate(plusSize,0);

        Volume curVol2 = curVol;

        if(CC.ASSERT && offset>>>48!=0)
            throw new DBException.DataCorruption();
        curVol2.ensureAvailable(walOffset2+plusSize);
        int parity = 1+Long.bitCount(value)+Long.bitCount(offset);
        parity &=15;
        curVol2.putUnsignedByte(walOffset2, (I_LONG << 4)|parity);
        walOffset2+=1;
        curVol2.putLong(walOffset2, value);
        walOffset2+=8;
        curVol2.putSixLong(walOffset2, offset);
    }

    protected void ensureFileReady(boolean addressable) {
        if(curVol==null){
            startNextFile();
            return;
        }

        if(addressable){
            //TODO fileOffset should be under lock, perhaps this entire section should be under lock
            if(fileOffset+MAX_FILE_RESERVE>MAX_FILE_SIZE){
                //EOF and move on
                seal();
                startNextFile();
            }
        }
    }


    public void walPutTombstone(long recid) {
        ensureFileReady(false);
        int plusSize = 1+DataIO.packLongSize(recid);
        long walOffset2 = allocate(plusSize, 0);

        Volume curVol2 = curVol;


        curVol2.ensureAvailable(walOffset2+plusSize);
        int checksum = 1+Long.bitCount(recid);
        checksum &= 15;
        curVol2.putUnsignedByte(walOffset2, (I_TOMBSTONE << 4)|checksum);
        walOffset2+=1;

        curVol2.putPackedLong(walOffset2, recid);
    }

    public void walPutPreallocate(long recid) {
        ensureFileReady(false);
        int plusSize = 1+DataIO.packLongSize(recid);
        long walOffset2 = allocate(plusSize,0);

        Volume curVol2 = curVol;

        curVol2.ensureAvailable(walOffset2+plusSize);
        int checksum = 1+Long.bitCount(recid);
        checksum &= 15;
        curVol2.putUnsignedByte(walOffset2, (I_PREALLOCATE << 4)|checksum);
        walOffset2+=1;

        curVol2.putPackedLong(walOffset2, recid);
    }




}
