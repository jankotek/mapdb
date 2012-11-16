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
 * Compiler Configuration.
 * Static final booleans to enable/disable features you want.
 * Compiler and dead code elimination will take care of removing unwanted features from bytecode.
 *
 * @author  Jan Kotek
 */
public interface CC {

    /**
     * Compile with assertions.
     */
    boolean ASSERT = true;

    /**
     * Compile with more assertions, this may slow down JDBM significantly
     */
    boolean PARANOID = false;

    /**
     * Compile with trace logging statements (Logger.debug and Logger.trace)
     */
    boolean TRACE = false;

    /**
     * JDBM has some long running acceptance tests. For daily development it makes sense to skip those.
     * This flag controls whatever all tests are run.
     */
    boolean FULL_TEST = false;


    /**
     * Log all binary writes into log.
     */
    boolean BB_LOG_WRITES = false;

    boolean BB_CHECK_AVAILABLE_SIZE = false;

    boolean BTREEMAP_LOG_NODE_LOCKS = false;


    short STORE_FORMAT_VERSION = 10000 + 1;


    /**
     * Values in BTreeMap are stored as part of nodes.
     * However if serialized size is greater then this,
     * value is placed as separate record and loaded
     * on request.
     */
    int MAX_BTREE_INLINE_VALUE_SIZE = 32;
}
