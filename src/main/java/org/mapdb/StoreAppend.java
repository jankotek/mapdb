/*
 *  Copyright (c) 2012 Jan Kotek
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.mapdb;

import java.io.DataInput;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;

/**
 * Append only store. Uses different file format than Direct and WAL store
 *
 */
public class StoreAppend extends Store{

    /** header at beginning of each file */
    protected static final long HEADER = 1239900952130003033L;

    /** index value has two parts, first is file number, second is offset in file, this is how many bites file offset occupies */
    protected static final int FILE_SHIFT = 24;

    /** mask used to get file offset from index val*/
    protected static final long FILE_MASK = 0xFFFFFF;

    protected static final int MAX_FILE_SIZE_SHIFT = CC.VOLUME_SLICE_SHIFT + 6; //TODO shift + 6 !!

    /** add to size before writing it to file */
    protected static final long SIZEP = 2;
    /** add to recid before writing it to file */
    protected static final long RECIDP = 3;
    /** at place of recid indicates uncommited transaction, an end of append log */
    protected static final long END = 1-RECIDP;
    /** at place of recid indicates commited transaction, just ignore this value and continue */
    protected static final long SKIP = 2-RECIDP;

    protected final boolean useRandomAccessFile;
    protected final boolean readOnly;
    protected final boolean syncOnCommit;
    protected final boolean deleteFilesAfterClose;
    /** transactions enabled*/
    protected final boolean tx;

    /** true after file was closed */
    protected volatile boolean closed = false;
    /** true after file was modified */
    protected volatile boolean modified = false;


    /** contains opened files, key is file number*/
    protected final LongConcurrentHashMap<Volume> volumes = new LongConcurrentHashMap<Volume>();

    /** last uses file, currently writing into */
    protected Volume currVolume;
    /** last used position, currently writing into */
    protected long currPos;
    /** last file number, currently writing into */
    protected long currFileNum;
    /** maximal recid */
    protected long maxRecid;

    /** file position on last commit, used for rollback */
    protected long rollbackCurrPos;
    /** file number on last commit, used for rollback */
    protected long rollbackCurrFileNum;
    /** maximial recid on last commit, used for rollback */
    protected long rollbackMaxRecid;

    /** index table which maps recid into position in index log */
    protected Volume index = new Volume.MemoryVol(false,0, MAX_FILE_SIZE_SHIFT); //TODO option to keep index off-heap or in file
    /** same as `index`, but stores uncommited modifications made in this transaction*/
    protected final LongMap<Long> indexInTx;




    public StoreAppend(final String fileName, Fun.Function1<Volume,String> volumeFactory,
                       final boolean useRandomAccessFile, final boolean readOnly,
                       final boolean transactionDisabled, final boolean deleteFilesAfterClose,  final boolean syncOnCommitDisabled,
                       boolean checksum, boolean compress, byte[] password) {
        super(fileName, volumeFactory, checksum, compress, password);

        this.useRandomAccessFile = useRandomAccessFile;
        this.readOnly = readOnly;
        this.deleteFilesAfterClose = deleteFilesAfterClose;
        this.syncOnCommit = !syncOnCommitDisabled;
        this.tx = !transactionDisabled;
        indexInTx = tx?new LongConcurrentHashMap<Long>() : null;

        final File parent = new File(fileName).getAbsoluteFile().getParentFile();
        if(!parent.exists() || !parent.isDirectory())
            throw new IllegalArgumentException("Parent dir does not exist: "+fileName);

        //list all matching files and sort them by number
        final SortedSet<Fun.Pair<Long,File>> sortedFiles = new TreeSet<Fun.Pair<Long, File>>();
        final String prefix = new File(fileName).getName();
        for(File f:parent.listFiles()){
            String name= f.getName();
            if(!name.startsWith(prefix) || name.length()<=prefix.length()+1) continue;
            String number = name.substring(prefix.length()+1, name.length());
            if(!number.matches("^[0-9]+$")) continue;
            sortedFiles.add(new Fun.Pair(Long.valueOf(number),f));
        }


        if(sortedFiles.isEmpty()){
            //no files, create empty store
            Volume zero = Volume.volumeForFile(getFileFromNum(0),useRandomAccessFile, readOnly,0L,MAX_FILE_SIZE_SHIFT,0);
            zero.ensureAvailable(Engine.LAST_RESERVED_RECID*8+8);
            zero.putLong(0, HEADER);
            long pos = 8;
            //put reserved records as empty
            for(long recid=1;recid<=LAST_RESERVED_RECID;recid++){
                pos+=zero.putPackedLong(pos, recid+RECIDP);
                pos+=zero.putPackedLong(pos, 0+SIZEP); //and mark it with zero size (0==tombstone)
            }
            maxRecid = LAST_RESERVED_RECID;
            index.ensureAvailable(LAST_RESERVED_RECID * 8 + 8);

            volumes.put(0L, zero);

            if(tx){
                rollbackCurrPos = pos;
                rollbackMaxRecid = maxRecid;
                rollbackCurrFileNum = 0;
                zero.putUnsignedByte(pos, (int) (END+RECIDP));
                pos++;
            }

            currVolume = zero;
            currPos = pos;
        }else{
            //some files exists, open, check header and replay index
            for(Fun.Pair<Long,File> t:sortedFiles){
                Long num = t.a;
                File f = t.b;
                Volume vol = Volume.volumeForFile(f,useRandomAccessFile,readOnly, 0L, MAX_FILE_SIZE_SHIFT,0);
                if(vol.isEmpty()||vol.getLong(0)!=HEADER){
                    vol.sync();
                    vol.close();
                    Iterator<Volume> vols = volumes.valuesIterator();
                    while(vols.hasNext()){
                        Volume next = vols.next();
                        next.sync();
                        next.close();
                    }
                    throw new IOError(new IOException("File corrupted: "+f));
                }
                volumes.put(num, vol);

                long pos = 8;
                while(pos<=FILE_MASK){
                    long recid = vol.getPackedLong(pos);
                    pos+=packedLongSize(recid);
                    recid -= RECIDP;
                    maxRecid = Math.max(recid,maxRecid);
//                    System.out.println("replay "+recid+ " - "+pos);

                    if(recid==END){
                        //reached end of file
                        currVolume = vol;
                        currPos = pos;
                        currFileNum = num;
                        rollbackCurrFileNum = num;
                        rollbackMaxRecid = maxRecid;
                        rollbackCurrPos = pos-1;


                        return;
                    }else if(recid==SKIP){
                        //commit mark, so skip
                        continue;
                    }else if(recid<=0){
                        Iterator<Volume> vols = volumes.valuesIterator();
                        while(vols.hasNext()){
                            Volume next = vols.next();
                            next.sync();
                            next.close();
                        }
                        throw new IOError(new IOException("File corrupted: "+f));
                    }

                    index.ensureAvailable(recid*8+8);
                    long indexVal = (num<<FILE_SHIFT)|pos;
                    long size = vol.getPackedLong(pos);
                    pos+=packedLongSize(size);
                    size-=SIZEP;

                    if(size==0){
                        index.putLong(recid*8,0);
                    }else if(size>0){
                        pos+=size;
                        index.putLong(recid*8,indexVal);
                    }else{
                        index.putLong(recid*8, Long.MIN_VALUE); //TODO tombstone
                    }
                }
            }
            Iterator<Volume> vols = volumes.valuesIterator();
            while(vols.hasNext()){
                Volume next = vols.next();
                next.sync();
                next.close();
            }
            throw new IOError(new IOException("File not sealed, data possibly corrupted"));
        }
    }

    public StoreAppend(String fileName) {
        this(   fileName,
                fileName==null || fileName.isEmpty()?Volume.memoryFactory():Volume.fileFactory(),
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                null
        );
    }


    protected File getFileFromNum(long fileNumber){
        return new File(fileName+"."+fileNumber);
    }

    protected void rollover(){
        if(currVolume.getLong(0)!=HEADER) throw new AssertionError();
        if(currPos<=FILE_MASK || readOnly) return;
        //beyond usual file size, so create new file
        currVolume.sync();
        currFileNum++;
        currVolume = Volume.volumeForFile(getFileFromNum(currFileNum),useRandomAccessFile, readOnly,0L, MAX_FILE_SIZE_SHIFT,0);
        currVolume.ensureAvailable(8);
        currVolume.putLong(0,HEADER);
        currPos = 8;
        volumes.put(currFileNum, currVolume);
    }



    protected long indexVal(long recid) {
        if(tx){
            Long val = indexInTx.get(recid);
            if(val!=null) return val;
        }
        return index.getLong(recid*8);
    }

    protected void setIndexVal(long recid, long indexVal) {
        if(tx) indexInTx.put(recid,indexVal);
        else{
            index.ensureAvailable(recid*8+8);
            index.putLong(recid*8,indexVal);
        }
    }

    @Override
    public long preallocate() {
        final Lock lock = locks[new Random().nextInt(locks.length)].readLock();
        lock.lock();

        try{
            structuralLock.lock();

            final long recid;
            try{
                recid = ++maxRecid;

                modified = true;
            }finally{
                structuralLock.unlock();
            }

            if(CC.PARANOID && ! (recid>0))
                throw new AssertionError();
            return recid;
        }finally {
            lock.unlock();
        }
    }


    @Override
    public void preallocate(long[] recids) {
        final Lock lock  = locks[new Random().nextInt(locks.length)].readLock();
        lock.lock();

        try{
            structuralLock.lock();
            try{
                for(int i = 0;i<recids.length;i++){
                    recids[i] = ++maxRecid;
                    if(CC.PARANOID && ! (recids[i]>0))
                        throw new AssertionError();
                }

                modified = true;
            }finally{
                structuralLock.unlock();
            }
        }finally {
            lock.unlock();
        }
    }

    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        if(CC.PARANOID && ! (value!=null))
            throw new AssertionError();
        DataIO.DataOutputByteArray out = serialize(value,serializer);

        final Lock lock = locks[new Random().nextInt(locks.length)].readLock();
        lock.lock();

        try{
            structuralLock.lock();

            final long oldPos,recid,indexVal;
            try{
                rollover();
                currVolume.ensureAvailable(currPos+6+4+out.pos);
                recid = ++maxRecid;

                //write recid
                currPos+=currVolume.putPackedLong(currPos, recid+RECIDP);
                indexVal = (currFileNum<<FILE_SHIFT)|currPos; //TODO file number
                //write size
                currPos+=currVolume.putPackedLong(currPos, out.pos+SIZEP);

                oldPos = currPos;
                currPos+=out.pos;

                modified = true;
            }finally{
                structuralLock.unlock();
            }

            //write data
            currVolume.putData(oldPos,out.buf,0,out.pos);

            recycledDataOuts.offer(out);
            setIndexVal(recid, indexVal);

            if(CC.PARANOID && ! (recid>0))
                throw new AssertionError();
            return recid;
        }finally {
            lock.unlock();
        }
    }

    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        if(CC.PARANOID && ! (recid>0))
            throw new AssertionError();
        final Lock lock = locks[Store.lockPos(recid)].readLock();
        lock.lock();
        try{
            return getNoLock(recid, serializer);
        }catch(IOException e){
            throw new IOError(e);
        }finally {
            lock.unlock();
        }
    }

    protected <A> A getNoLock(long recid, Serializer<A> serializer) throws IOException {
        long indexVal = indexVal(recid);

        if(indexVal==0) return null;
        Volume vol = volumes.get(indexVal>>>FILE_SHIFT);
        long fileOffset = indexVal&FILE_MASK;
        long size = vol.getPackedLong(fileOffset);
        fileOffset+= packedLongSize(size);
        size-=SIZEP;
        if(size<0) return null;
        if(size==0) return serializer.deserialize(new DataIO.DataInputByteArray(new byte[0]),0);
        DataInput in =  vol.getDataInput(fileOffset, (int) size);

        return deserialize(serializer, (int) size,in);
    }


    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        if(CC.PARANOID && ! (value!=null))
            throw new AssertionError();
        if(CC.PARANOID && ! (recid>0))
            throw new AssertionError();
        DataIO.DataOutputByteArray out = serialize(value,serializer);

        final Lock lock = locks[Store.lockPos(recid)].writeLock();
        lock.lock();

        try{
            updateNoLock(recid, out);
        }finally {
            lock.unlock();
        }
        recycledDataOuts.offer(out);
    }

    protected void updateNoLock(long recid, DataIO.DataOutputByteArray out) {
        final long indexVal, oldPos;

        structuralLock.lock();
        try{
            rollover();
            currVolume.ensureAvailable(currPos+6+4+out.pos);
            //write recid
            currPos+=currVolume.putPackedLong(currPos, recid+RECIDP);
            indexVal = (currFileNum<<FILE_SHIFT)|currPos; //TODO file number
            //write size
            currPos+=currVolume.putPackedLong(currPos, out.pos+SIZEP);
            oldPos = currPos;
            currPos+=out.pos;
            modified = true;
        }finally {
            structuralLock.unlock();
        }
        //write data
        currVolume.ensureAvailable(oldPos+out.pos);
        currVolume.putData(oldPos,out.buf,0,out.pos);

        setIndexVal(recid, indexVal);
    }


    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        if(CC.PARANOID && ! (expectedOldValue!=null && newValue!=null))
            throw new AssertionError();
        if(CC.PARANOID && ! (recid>0))
            throw new AssertionError();
        DataIO.DataOutputByteArray out = serialize(newValue,serializer);
        final Lock lock = locks[Store.lockPos(recid)].writeLock();
        lock.lock();

        boolean ret;
        try{
            Object old = getNoLock(recid,serializer);
            if(expectedOldValue.equals(old)){
                updateNoLock(recid,out);
                ret = true;
            }else{
                ret = false;
            }
        }catch(IOException e){
            throw new IOError(e);
        }finally {
            lock.unlock();
        }
        recycledDataOuts.offer(out);
        return ret;
    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer) {
        if(CC.PARANOID && ! (recid>0))
            throw new AssertionError();
        final Lock lock = locks[Store.lockPos(recid)].writeLock();
        lock.lock();

        try{
            structuralLock.lock();
            try{
                rollover();
                currVolume.ensureAvailable(currPos+6+0);
                currPos+=currVolume.putPackedLong(currPos, recid+SIZEP);
                setIndexVal(recid, (currFileNum<<FILE_SHIFT) | currPos);
                //write tombstone
                currPos+=currVolume.putPackedLong(currPos, 1);
                modified = true;
            }finally{
                structuralLock.unlock();
            }
        }finally{
            lock.unlock();
        }
    }

    @Override
    public void close() {
        if(closed) return;

        if(serializerPojo!=null && serializerPojo.hasUnsavedChanges()){
            serializerPojo.save(this);
        }

        Iterator<Volume> iter=volumes.valuesIterator();
        if(!readOnly && modified){ //TODO and modified since last open
            rollover();
            currVolume.putUnsignedByte(currPos, (int) (END+RECIDP));
        }
        while(iter.hasNext()){
            Volume v = iter.next();
            v.sync();
            v.close();
            if(deleteFilesAfterClose) v.deleteFile();
        }
        volumes.clear();
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }


    @Override
    public void commit() {
        if(!tx){
            currVolume.sync();
            return;
        }

        lockAllWrite();
        try{

            LongMap.LongMapIterator<Long> iter = indexInTx.longMapIterator();
            while(iter.moveToNext()){
                index.ensureAvailable(iter.key()*8+8);
                index.putLong(iter.key()*8, iter.value());
            }
            Volume rollbackCurrVolume = volumes.get(rollbackCurrFileNum);
            rollbackCurrVolume.putUnsignedByte(rollbackCurrPos, (int) (SKIP+RECIDP));
            if(syncOnCommit) rollbackCurrVolume.sync();

            indexInTx.clear();

            rollover();
            rollbackCurrPos = currPos;
            rollbackMaxRecid = maxRecid;
            rollbackCurrFileNum = currFileNum;

            currVolume.putUnsignedByte(rollbackCurrPos, (int) (END+RECIDP));
            currPos++;

            if(serializerPojo!=null && serializerPojo.hasUnsavedChanges()){
                serializerPojo.save(this);
            }

        }finally{
            unlockAllWrite();
        }

    }


    @Override
    public void rollback() throws UnsupportedOperationException {
        if(!tx) throw new UnsupportedOperationException("Transactions are disabled");

        lockAllWrite();
        try{

            indexInTx.clear();
            currVolume = volumes.get(rollbackCurrFileNum);
            currPos = rollbackCurrPos;
            maxRecid = rollbackMaxRecid;
            currFileNum = rollbackCurrFileNum;

            //TODO rollback serializerPojo?
        }finally{
            unlockAllWrite();
        }

    }

    @Override
    public boolean canRollback(){
        return tx;
    }


    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public void clearCache() {
        //no cache to clear
    }

    @Override
    public void compact() {
        if(readOnly) throw new IllegalAccessError("readonly");
        lockAllWrite();
        try{

            if(!indexInTx.isEmpty()) throw new IllegalAccessError("uncommited changes");

            LongHashMap<Boolean> ff = new LongHashMap<Boolean>();
            for(long recid=0;recid<=maxRecid;recid++){
                long indexVal = index.getLong(recid*8);
                if(indexVal ==0)continue;
                long fileNum = indexVal>>>FILE_SHIFT;
                ff.put(fileNum,true);
            }

            //now traverse files and delete unused
            LongMap.LongMapIterator<Volume> iter = volumes.longMapIterator();
            while(iter.moveToNext()){
                long fileNum = iter.key();
                if(fileNum==currFileNum || ff.get(fileNum)!=null) continue;
                Volume v = iter.value();
                v.sync();
                v.close();
                v.deleteFile();
                iter.remove();
            }
        }finally{
            unlockAllWrite();
        }

    }

    @Override
    public long getMaxRecid() {
        return maxRecid;
    }

    @Override
    public ByteBuffer getRaw(long recid) {
        //TODO use direct BB
        byte[] bb = get(recid, Serializer.BYTE_ARRAY_NOSIZE);
        if(bb==null) return null;
        return ByteBuffer.wrap(bb);
    }

    @Override
    public Iterator<Long> getFreeRecids() {
        return Fun.EMPTY_ITERATOR; //TODO free recid management
    }

    @Override
    public void updateRaw(long recid, ByteBuffer data) {
        rollover();
        byte[] b = null;
        if(data!=null){
            data = data.duplicate();
            b = new byte[data.remaining()];
            data.get(b);
        }
        //TODO use BB without copying
        update(recid, b, Serializer.BYTE_ARRAY_NOSIZE);
        modified = true;
    }

    @Override
    public long getSizeLimit() {
        return 0;
    }

    @Override
    public long getCurrSize() {
        return currFileNum*FILE_MASK;
    }

    @Override
    public long getFreeSize() {
        return 0;
    }

    @Override
    public String calculateStatistics() {
        return null;
    }


    /** get number of bytes occupied by packed long */
    protected static int packedLongSize(long value) {
        int ret = 1;
        while ((value & ~0x7FL) != 0) {
            ret++;
            value >>>= 7;
        }
        return ret;
    }

}


