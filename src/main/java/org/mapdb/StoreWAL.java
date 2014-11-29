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
import java.io.IOError;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;
import java.util.zip.CRC32;

/**
 * Write-Ahead-Log
 */
public class StoreWAL extends StoreCached {


    public static final String TRANS_LOG_FILE_EXT = ".t";
    public static final long LOG_SEAL = 123321234423334324L;

    public StoreWAL(String fileName) {
        super(fileName);
    }

    public StoreWAL(String fileName, Fun.Function1<Volume, String> volumeFactory, boolean checksum, boolean compress, byte[] password, boolean readonly, boolean deleteFilesAfterClose, int freeSpaceReclaimQ, boolean commitFileSyncDisable, int sizeIncrement) {
        super(fileName, volumeFactory, checksum, compress, password, readonly, deleteFilesAfterClose, freeSpaceReclaimQ, commitFileSyncDisable, sizeIncrement);
    }

    @Override
    public void rollback() throws UnsupportedOperationException {
        //flush modified records
        for(int i=0;i<locks.length;i++){
            Lock lock = locks[i].writeLock();
            lock.lock();
            try {
                LongMap.LongMapIterator<Fun.Pair<Object, Serializer>> iter = writeCache[i].longMapIterator();
                while(iter.moveToNext()){
                    long recid = iter.key();
                    Fun.Pair<Object, Serializer> p = iter.value();
                    if(p==TOMBSTONE){
                        delete2(recid,Serializer.ILLEGAL_ACCESS);
                    }else{
                        DataIO.DataOutputByteArray buf = serialize(p.a, p.b); //TODO somehow serialize outside lock?
                        update2(recid,buf);
                        recycledDataOut.lazySet(buf);
                    }
                    iter.remove();
                }

                if(CC.PARANOID && !writeCache[i].isEmpty())
                    throw new AssertionError();

            }finally {
                lock.unlock();
            }
        }

        structuralLock.lock();
        try {
            dirtyStackPages.clear();
            initHeadVol();
        }finally {
            structuralLock.unlock();
        }
    }

    @Override
    public boolean canRollback() {
        return true;
    }
}
