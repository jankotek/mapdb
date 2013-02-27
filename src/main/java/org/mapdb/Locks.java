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

import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Contains various concurrent locking utilities
 */
public final class Locks {

    private Locks(){}


    /**
     * An array of ReentrantLocks with infinitive size.
     * Is used for per-record locking.
     */
    public interface RecidLocks{
        /**
         * Unlock given recid. Throws an unspecified exception of recid is not locked
         * @param recid number
         */
        public void unlock(final long recid);

        /**
         * Throws an exception if current thread holds any locks.
         * Used for assertion that all recids were properly released
         */
        public void assertNoLocks();
        /**
         * Locks record with given recid. Blocks if already locked, until lock becomes available.
         * @param recid number
         */
        public void lock(final long recid);
    }

    /**
     * Holds all existing locks in HashMap.
     * Lock/unlock operation looks up lock existence in map and act accordingly.
     * Usefull if there is only handful of locks
     */
    public static class LongHashMapRecidLocks implements RecidLocks{

        protected final LongConcurrentHashMap<Thread> locks = new LongConcurrentHashMap<Thread>();

        public void unlock(final long recid) {
            if(CC.LOG_LOCKS)
                Utils.LOG.finest("UNLOCK R:"+recid+" T:"+Thread.currentThread().getId());

            final Thread t = locks.remove(recid);
            if(t!=Thread.currentThread())
                throw new InternalError("unlocked wrong thread");

        }

        public void assertNoLocks(){
            if(CC.PARANOID){
                LongMap.LongMapIterator<Thread> i = locks.longMapIterator();
                while(i.moveToNext()){
                    if(i.value()==Thread.currentThread()){
                        throw new InternalError("Node "+i.key()+" is still locked");
                    }
                }
            }
        }

        public void lock(final long recid) {
            if(CC.LOG_LOCKS)
                Utils.LOG.finest("TRYLOCK R:"+recid+" T:"+Thread.currentThread().getId());

            //feel free to rewrite, if you know better (more efficient) way
            if(locks.get(recid)==Thread.currentThread()){
                //check node is not already locked by this thread
                throw new InternalError("node already locked by current thread: "+recid);
            }


            while(locks.putIfAbsent(recid, Thread.currentThread()) != null){
                LockSupport.parkNanos(10);
            }
            if(CC.LOG_LOCKS)
                Utils.LOG.finest("LOCK R:"+recid+" T:"+Thread.currentThread().getId());
        }
    }

    /**
     * Fixed size array of locks. <code>Recid % locks.length</code> (modulo)
     * is used to determine which lock should be used.
     */
    public static class SegmentedRecidLocks implements RecidLocks{

        protected final ReentrantLock[] locks;

        protected final int numSegments;

        /**
         * @param numSegments number of locks, larger number means better concurrency but larger memory overhead. Good value is 16
         */
        public SegmentedRecidLocks(int numSegments) {
            this.numSegments = numSegments;
            locks = new ReentrantLock[numSegments];
            for(int i=0;i<numSegments;i++)
                locks[i] = new ReentrantLock();
        }

        @Override
        public void unlock(long recid) {
            locks[((int) (recid % numSegments))].unlock();
        }

        @Override
        public void assertNoLocks() {
            for(ReentrantLock l:locks){
                if(l.isLocked())
                    throw new InternalError("Some node is still locked by current thread");
            }
        }

        @Override
        public void lock(long recid) {
            locks[((int) (recid % numSegments))].lock();
        }
    }

}
