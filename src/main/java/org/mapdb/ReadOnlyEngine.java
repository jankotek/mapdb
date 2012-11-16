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
 * Wraps an <code>Engine</code> and throws
 * <code>UnsupportedOperationException("Read-only")</code>
 * on any modification attempt.
 *
 * @author Jan Kotek
 */
public class ReadOnlyEngine implements Engine {

    protected Engine engine;

    public ReadOnlyEngine(Engine engine){
        this.engine = engine;
    }

    @Override
    public <A> long recordPut(A value, Serializer<A> serializer) {
        throw new UnsupportedOperationException("Read-only");
    }

    @Override
    public <A> A recordGet(long recid, Serializer<A> serializer) {
        return engine.recordGet(recid, serializer);
    }

    @Override
    public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {
        throw new UnsupportedOperationException("Read-only");
    }

    @Override
    public void recordDelete(long recid) {
        throw new UnsupportedOperationException("Read-only");
    }


    @Override
    public void close() {
        engine.close();
        engine = null;
    }

    @Override
    public void commit() {
        throw new UnsupportedOperationException("Read-only");
    }

    @Override
    public void rollback() {
        throw new UnsupportedOperationException("Read-only");
    }

    @Override
    public long serializerRecid() {
        return engine.serializerRecid();
    }

    @Override
    public long nameDirRecid() {
        return engine.nameDirRecid();
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

}
