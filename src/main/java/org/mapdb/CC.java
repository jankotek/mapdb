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
 *     if(CC.ASSERT && arg==null){
 *         throw new IllegalArgumentException("Argument is null");
 *     }
 * </pre>
 *
 * @author  Jan Kotek
 */
public interface CC {

    /**
     * MapDB has acceptance tests which may take long time to finish (<b>week!!!</b>).
     * For daily development it makes sense to disable it..
     * This flag controls if full test suite is ran.
     */
    boolean FULL_TEST = false;


    /**
     * Compile with basic assertions (boundaries, non null...).
     */
    boolean ASSERT = true;

    /**
     * Compile even with more assertions and verifications.
     * For example HashMap may check if keys implements hash function correctly.
     * This may slow down MapDB thousands times
     */
    boolean PARANOID = false;

    /**
     * Compile with fine trace logging statements (Logger.debug and Logger.trace).
     */
    boolean LOG_TRACE = false;

    /**
     * Log lock/unlock events. Useful to diagnose deadlocks
     */
    boolean LOG_LOCKS = false;

}
