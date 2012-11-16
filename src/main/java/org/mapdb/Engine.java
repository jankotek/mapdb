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
 * Engine is center-piece for managing records in JDBM.
 * It is responsible for retrieving and storing records
 * from disk store or cache.
 * <p/>
 * Engine may be used for extending JDBM4
 * For example, instance cache is implemented as Engine wrapper.
 * JDBM4 can use alternative record store, so that other databases, such as LevelDB,
 * Kyoto etc can now take advantage of JDBM4 maps, instance cache and advanced serialization.
 *
 * @author Jan Kotek
 */
public interface Engine {

    /**
     * Adds new records into store/cache,
     *
     * @param value records to be added
     * @param serializer used to serialize record into binary form
     * @param <A> type of record
     * @return recid (record identifier) under which record is stored.
     */
    <A> long recordPut(A value, Serializer<A> serializer);

    /**
     * Get records from store/cache.
     *
     * @param recid (record identifier) under which record was persisted
     * @param serializer used to deserialize record from binary form
     * @param <A> record type
     * @return record matching given recid, or null if record is not found under given recid.
     */
    <A> A recordGet(long recid, Serializer<A> serializer);

    /**
     * Update existing record with new value.
     *
     * @param recid (record identifier) under which record was persisted.
     * @param value new record value to be stored
     * @param serializer used to serialize record from binary form
     * @param <A> record type
     *
     * TODO exception thrown if record does not exist
     */
    <A> void recordUpdate(long recid, A value, Serializer<A> serializer);


    /**
     * Remove existing record from store/cache
     *
     * @param recid (record identifier) under which was record persisted
     *
     * TODO exception thrown if record does not exist
     */
    void recordDelete(long recid);



    /**
     * Close store/cache.
     * This method must be called before JVM exits.
     * Engine can no longer be used, after it was closed.
     */
    void close();

    void commit();
    void rollback();

    long serializerRecid();
    long nameDirRecid();

    boolean isReadOnly();
}
