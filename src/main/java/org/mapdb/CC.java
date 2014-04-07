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
 * Compiler Configuration. There are some static final boolean fields, which describe features MapDB was compiled with.
 * <p/>
 * MapDB can be compiled with/without some features. For example fine logging is useful for debuging,
 * but should not be present in production version. Java does not have preprocessor so
 * we use <a href="http://en.wikipedia.org/wiki/Dead_code_elimination">Dead code elimination</a> to achieve it.
 * <p/>
 * Typical usage:
 * <pre>
 *     if(CC.PARANOID && arg.calculateSize()!=33){  //calculateSize may take long time
 *         throw new IllegalArgumentException("wrong size");
 *     }
 * </pre>
 *
 * @author  Jan Kotek
 */
public interface CC {

    /**
     * Compile with more assertions and verifications.
     * For example HashMap may check if keys implements hash function correctly.
     * This may slow down MapDB thousands times
     */
    boolean PARANOID = false;

    /**
     * Compile-in detailed log messages from store.
     */
    boolean LOG_STORE = false;

    /**
     * Log lock/unlock events. Useful to diagnose deadlocks
     */
    boolean LOG_LOCKS = false;

    /**
     * If true MapDB will display warnings if user is using MapDB API wrong way.
     */
    boolean LOG_HINTS = true;


    int ASYNC_WRITE_FLUSH_DELAY = 100;
    int ASYNC_WRITE_QUEUE_SIZE = 32000;

    int ASYNC_RECID_PREALLOC_QUEUE_SIZE = 128;

    /**
     * Concurrency level. Should be greater than number of threads accessing
     * MapDB concurrently. On other side larger number consumes more memory
     * <p/>
     * This number must be power of two: `CONCURRENCY = 2^N`
     */
    int CONCURRENCY = 128;


    int BTREE_DEFAULT_MAX_NODE_SIZE = 32;


    int DEFAULT_CACHE_SIZE = 1024 * 32;

    String DEFAULT_CACHE = DBMaker.Keys.cache_hashTable;

    int DEFAULT_FREE_SPACE_RECLAIM_Q = 5;

    /** controls if locks used in MapDB are fair */
    boolean FAIR_LOCKS = false;


    int VOLUME_CHUNK_SHIFT = 20; // 1 MB
}

