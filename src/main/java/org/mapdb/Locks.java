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

import java.util.concurrent.locks.ReentrantLock;

/**
 * Contains various concurrent locking utilities
 */
public final class Locks {

    private Locks(){}



    public interface RecidLocks{
        public void unlock(final long recid);
        public void assertNoLocks();
        public void lock(final long recid);
    }

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
            if(CC.ASSERT && locks.get(recid)==Thread.currentThread()){
                //check node is not already locked by this thread
                throw new InternalError("node already locked by current thread: "+recid);
            }


            while(locks.putIfAbsent(recid, Thread.currentThread()) != null){
                Thread.yield();
            }
            if(CC.LOG_LOCKS)
                Utils.LOG.finest("LOCK R:"+recid+" T:"+Thread.currentThread().getId());
        }
    }

    public static class SegmentedRecidLocks implements RecidLocks{

        protected final ReentrantLock[] locks;

        protected final int numSegments;

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
