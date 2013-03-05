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
 * Central interface for managing records.
 * It is primitive key value store, with <code>long</code> keys and object instances values.
 * It contains basic CRUD operations.
 * <p/>
 * MapDB (unlike other DBs) does not use binary <code>byte[]</code> for its values.
 * Instead it takes object instance with serializer, and manages serialization/deserialization itself.
 * Integrating serialization into database gives us lot of flexibility.
 * <p/>
 * There is {@link Storage} class which implements basic persistence. Most of MapDB features
 * comes from {@link EngineWrapper}, they are stacked on top of each other
 * to provide asynchronous writes, instance cache, encryption etc..
 * <code>Engine</code> stack is very elegant and uniform way to handle additional functionality.
 *  Other DBs need an ORM framework to achieve similar features.
 * <p/>
 * In default configuration MapDB runs with this <code>Engine</code> stack:
 * <pre>
 *                     [DISK IO]
 *       StorageJournaled - permament record storage with journaled transactions
 *       AsyncWriteEngine - asynchronous writes to storage
 *    ByteTransformEngine - compression or encryption (optional)
 *         CacheHashTable - instance cache
 *         SnapshotEngine - support for snapshots
 *                     [USER API]
 * </pre>
 *
 * <p/>
 * Engine uses 'recid' to identify records. There is zero error handling in case recid is invalid
 * (random number or already deleted record). Passing illegal recid may result into anything
 * (return null, throw EOF or even corrupt store). Engine is considered low-level component
 * and it is responsibility of upper layers (collections) to ensure recid is consistent.
 * Lack of error handling is trade of for speed (similar way as manual memory management in C++)
 * <p/>
 * Engine must support {@code null} record values. You may insert, update and fetch null records.
 * Nulls play important role in recid preallocation and asynchronous writes.
 * <p/>
 * Recid can be reused after it was deleted. If your application relies on unique being unique,
 * you should update record with null value, instead of delete.
 * Null record consumes only 8 bytes in store and is preserved during defragmentation.
 *
 * @author Jan Kotek
 */
public interface Engine {

    long NAME_DIR_RECID = 1;
    long CLASS_INFO_RECID = 2;
    long LAST_RESERVED_RECID = 7;


    /**
     * Insert new record.
     *
     * @param value records to be added
     * @param serializer used to convert record into/from binary form
     * @param <A> type of record
     * @return recid (record identifier) under which record is stored.
     */
    <A> long put(A value, Serializer<A> serializer);

    /**
     * Get existing record.
     * <p/>
     * Recid must be a number returned by 'put' method.
     * Behaviour for invalid recid (random number or already deleted record)
     * is not defined, typically it returns null or throws 'EndOfFileException'
     *
     * @param recid (record identifier) under which record was persisted
     * @param serializer used to deserialize record from binary form
     * @param <A> record type
     * @return record matching given recid, or null if record is not found under given recid.
     */
    <A> A get(long recid, Serializer<A> serializer);

    /**
     * Update existing record with new value.
     * <p/>
     * Recid must be a number returned by 'put' method.
     * Behaviour for invalid recid (random number or already deleted record)
     * is not defined, typically it throws 'EndOfFileException',
     * but it may also corrupt store.
     *
     * @param recid (record identifier) under which record was persisted.
     * @param value new record value to be stored
     * @param serializer used to serialize record into binary form
     * @param <A> record type
     */
    <A> void update(long recid, A value, Serializer<A> serializer);


    /**
     * Updates existing record in atomic <a href="http://en.wikipedia.org/wiki/Compare-and-swap">(Compare And Swap)</a> manner.
     * Value is modified only if old value matches expected value. There are three ways to match values, MapDB may use any of them:
     * <ol>
     *    <li>Equality check <code>oldValue==expectedOldValue</code> when old value is found in instance cache</li>
     *    <li>Deserializing <code>oldValue</code> using <code>serializer</code> and checking <code>oldValue.equals(expectedOldValue)</code></li>
     *    <li>Serializing <code>expectedOldValue</code> using <code>serializer </code> and comparing binary array with already serialized <code>oldValue</code>
     * </ol>
     * <p/>
     * Recid must be a number returned by 'put' method.
     * Behaviour for invalid recid (random number or already deleted record)
     * is not defined, typically it throws 'EndOfFileException',
     * but it may also corrupt store.
     *
     * @param recid (record identifier) under which record was persisted.
     * @param expectedOldValue old value to be compared with existing record
     * @param newValue to be written if values are matching
     * @param serializer used to serialize record into binary form
     * @param <A>
     * @return true if values matched and newValue was written
     */
    <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer);

    /**
     * Remove existing record from store/cache
     *
     * <p/>
     * Recid must be a number returned by 'put' method.
     * Behaviour for invalid recid (random number or already deleted record)
     * is not defined, typically it throws 'EndOfFileException',
     * but it may also corrupt store.
     *
     * @param recid (record identifier) under which was record persisted
     * @param serializer which may be used in some circumstances to deserialize and store old object
     */
    <A> void delete(long recid, Serializer<A>  serializer);



    /**
     * Close store/cache. This method must be called before JVM exits to flush all caches and prevent store corruption.
     * Also it releases resources used by MapDB (disk, memory..).
     * <p/>
     * Engine can no longer be used after this method was called. If Engine is used after closing, it may
     * throw any exception including <code>NullPointerException</code>
     * </p>
     * There is an configuration option {@link DBMaker#closeOnJvmShutdown()} which uses shutdown hook to automatically
     * close Engine when JVM shutdowns.
     */
    void close();


    /**
     * Checks whether Engine was closed.
     *
     * @return true if engine was closed
     */
    public boolean isClosed();

    /**
     * Makes all changes made since the previous commit/rollback permanent.
     * In transactional mode (on by default) it means creating journal file and replaying it to storage.
     * In other modes it may flush disk caches or do nothing at all (check your config options)
     */
    void commit();

    /**
     * Undoes all changes made in the current transaction.
     * If transactions are disabled it throws {@link UnsupportedOperationException}.
     *
     * @throws UnsupportedOperationException if transactions are disabled
     */
    void rollback() throws UnsupportedOperationException;

    /**
     * Check if you can write into this Engine. It may be readonly in some cases (snapshot, read-only files).
     *
     * @return true if engine is read-only
     */
    boolean isReadOnly();

    void compact();
}
