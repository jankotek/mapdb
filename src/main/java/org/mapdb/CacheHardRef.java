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


/**
 * Cache created objects using hard reference.
 * It checks free memory every N operations (1024*10). If free memory is bellow 75% it clears the cache
 *
 * @author Jan Kotek
 */
public class CacheHardRef extends CacheLRU {

    final static int CHECK_EVERY_N = 10000;

    int counter = 0;

    public CacheHardRef(Engine engine, int initialCapacity) {
        super(engine, new LongConcurrentHashMap<Object>(initialCapacity));
    }

    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        checkFreeMem();
        return super.get(recid, serializer);
    }

    private void checkFreeMem() {
        if((counter++)%CHECK_EVERY_N==0 ){

            Runtime r = Runtime.getRuntime();
            long max = r.maxMemory();
            if(max == Long.MAX_VALUE)
                return;

            double free = r.freeMemory();
            double total = r.totalMemory();
            //We believe that free refers to total not max.
            //Increasing heap size to max would increase to max
            free = free + (max-total);

            if(CC.LOG_TRACE)
                Utils.LOG.fine("DBCache: freemem = " +free + " = "+(free/max)+"%");

            if(free<1e7 || free*4 <max){
                checkClosed(cache).clear();
            }
        }
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        checkFreeMem();
        super.update(recid, value, serializer);
    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer){
        checkFreeMem();
        super.delete(recid,serializer);
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        checkFreeMem();
        return super.compareAndSwap(recid, expectedOldValue, newValue, serializer);
    }
}

