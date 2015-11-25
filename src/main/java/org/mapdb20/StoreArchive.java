package org.mapdb20;

import java.io.DataInput;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.NavigableMap;

/**
 * Store without index table.
 * Recid is actual physical offset in file.
 * Very space efficient, but read-only, must be created with Data Pump
 */
//TODO modifications are thread unsafe
//TODO boundary overlaps
//TODO instance cache for reads
public final class StoreArchive extends Store{

    protected static final long FILE_SIZE_OFFSET = 16;
    protected static final long FIRST_RESERVED_RECID_OFFSET = FILE_SIZE_OFFSET+9*8;
    protected static final long DATA_START_OFFSET = FIRST_RESERVED_RECID_OFFSET+7*8;

    public StoreArchive(
            String fileName,
            Volume.VolumeFactory volumeFactory,
            boolean readonly){
        this(
            fileName,
            volumeFactory,
            null,
            1,
            0,
            false,
            false,
            null,
            readonly,
            false,
            false,
            null
        );
    }

    public StoreArchive(
            String fileName,
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

        super(
                fileName,
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

    protected Volume vol;
    protected long volSize;

    @Override
    public void init() {
        boolean empty = Volume.isEmptyFile(fileName);
        vol = volumeFactory.makeVolume(
                fileName,
                readonly);

        if(empty){
            volSize = DATA_START_OFFSET;
            vol.ensureAvailable(volSize);
            //fill recids
            for(long recid=1;recid<RECID_LAST_RESERVED;recid++){
                long offset = FIRST_RESERVED_RECID_OFFSET + recid*8 - 8;
                vol.putLong(offset, DataIO.parity4Set(0<<4));
            }
        }else{
            volSize = DataIO.parity4Get(vol.getLong(FILE_SIZE_OFFSET))>>>4;
        }
    }

    @Override
    protected <A> A get2(long recid, Serializer<A> serializer) {
        if(recid<=Engine.RECID_LAST_RESERVED) {
            //special case for reserved recid
            recid = DataIO.parity4Get(
                    vol.getLong(FIRST_RESERVED_RECID_OFFSET+recid*8-8))>>>4;
            if(recid==0)
                return null;
        }

        if(recid>volSize)
            throw new DBException.EngineGetVoid();

        //read size, extract number of bytes read
        long recSize = vol.getPackedLong(recid);
        long recSizeBytesRead = recSize>>>60;
        recSize &= DataIO.PACK_LONG_RESULT_MASK;

        if(recSize==0) {
            throw new DBException.EngineGetVoid();
        }

        //do parity check, normalize
        recSize = (DataIO.parity1Get(recSize)>>>1)-1;
        if(recSize==-1) {
            return null;
        }

        if(recid + recSizeBytesRead + recSize>volSize){
            throw new DBException.DataCorruption("Record goes beyond EOF");

        }

        DataInput in = vol.getDataInputOverlap(recid + recSizeBytesRead, (int) recSize);
        return deserialize(serializer, (int) recSize, in);
    }

    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        if(readonly) {
            throw new UnsupportedOperationException("StoreArchive is read-only");
        }

        if(value==null){
            //null record, write zero and we are done
            long ret = volSize;
            vol.ensureAvailable(volSize+1);
            volSize+=vol.putPackedLong(volSize, DataIO.parity1Set(0<<1));
            return ret;
        }

        DataIO.DataOutputByteArray out = serialize(value, serializer);
        return add2(out);
    }

    protected long add2(DataIO.DataOutputByteArray out) {
        long size = DataIO.parity1Set((1L + out.pos) << 1);

        //make sure that size will not overlap, there must be at least 10 bytes before overlap
        if(volSize>>>CC.VOLUME_PAGE_SHIFT!=(volSize+5)>>CC.VOLUME_PAGE_SHIFT){
            volSize = Fun.roundUp(volSize, 1L<<CC.VOLUME_PAGE_SHIFT);
        }

        long ret = volSize;
        vol.ensureAvailable(volSize + 5);
        int bytesWritten = vol.putPackedLong(volSize,size);

        vol.ensureAvailable(volSize+bytesWritten+out.pos);
        vol.putDataOverlap(volSize + bytesWritten, out.buf, 0, out.pos);
        volSize += bytesWritten + out.pos;

        return ret;
    }

    @Override
    public void close() {
        if(!readonly)
            commit();
        vol.close();
        vol=null;
    }

    @Override
    public void commit() {
        if(!readonly){
            //synchronize file size and sync it
            vol.putLong(FILE_SIZE_OFFSET,
                    DataIO.parity4Set(volSize<<4)
                    );
            vol.sync();
        }
    }

    protected void rewriteNamedCatalog(NavigableMap<String, Object> catalog) {
        if(readonly) {
            throw new UnsupportedOperationException("StoreArchive is read-only");
        }

        long offset = Pump.buildTreeMap(
                (Iterator) catalog.descendingMap().entrySet().iterator(),
                this,
                Fun.extractMapEntryKey(),
                Fun.extractMapEntryValue(),
                true,
                32,
                false,
                0L,
                BTreeKeySerializer.STRING,
                Serializer.BASIC, //TODO attach this to DB serialization, update POJO class catalog if needed
                null
                );

        offset = DataIO.parity4Set(offset<<4);
        vol.putLong(StoreArchive.FIRST_RESERVED_RECID_OFFSET + Engine.RECID_NAME_CATALOG*8-8,offset);
    }


    @Override
    public long getCurrSize() {
        return volSize;
    }

    @Override
    protected void update2(long recid, DataIO.DataOutputByteArray out) {
        if(readonly) {
            throw new UnsupportedOperationException("StoreArchive is read-only");
        }

        if(recid<=Engine.RECID_LAST_RESERVED) {
            //special case for reserved recid
            long recidVal = out==null ? 0 : add2(out); //insert new data
            vol.putLong(FIRST_RESERVED_RECID_OFFSET+recid*8-8,
                    DataIO.parity4Set(recidVal<<4)); //and update index micro-table
            return;
        }

        //update only if old record has the same size, and record layout does not have to be changed
        if(recid>volSize)
            throw new DBException.EngineGetVoid();

        //read size, extract number of bytes read
        long recSize = vol.getPackedLong(recid);
        long recSizeBytesRead = recSize>>>60;
        recSize &= DataIO.PACK_LONG_RESULT_MASK;

        if(recSize==0) {
            throw new DBException.EngineGetVoid();
        }

        //do parity check, normalize
        recSize = (DataIO.parity1Get(recSize)>>>1)-1;
        if(recSize==-1 && out!=null) {
            //TODO better exception
            throw new DBException.WrongConfig(
                    "StoreArchive supports updates only if old and new record has the same size." +
                    "But here old=null, new!=null");
        }

        if(recSize!=out.pos){
            //TODO better exception
            throw new DBException.WrongConfig(
                    "StoreArchive supports updates only if old and new record has the same size." +
                            "But here oldSize="+recSize+", newSize="+out.pos);
        }

        //overwrite data
        vol.putDataOverlap(recid + recSizeBytesRead, out.buf, 0, out.pos);
    }

    @Override
    protected <A> void delete2(long recid, Serializer<A> serializer) {
        throw new UnsupportedOperationException("StoreArchive is read-only");
    }

    @Override
    public long getFreeSize() {
        return 0;
    }

    @Override
    public boolean fileLoad() {
        return vol.fileLoad();
    }

    @Override
    public void backup(OutputStream out, boolean incremental) {
        throw new UnsupportedOperationException("StoreArchive has different RECID layout");
    }

    @Override
    public void backupRestore(InputStream[] in) {
        throw new UnsupportedOperationException("StoreArchive has different RECID layout");
    }

    @Override
    public long preallocate() {
        throw new UnsupportedOperationException("StoreArchive is read-only");
    }


    @Override
    public void rollback() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("StoreArchive is read-only");
    }

    @Override
    public boolean canRollback() {
        return false;
    }

    @Override
    public Engine snapshot() throws UnsupportedOperationException {
        return this;
    }

    @Override
    public void compact() {
    }

}

