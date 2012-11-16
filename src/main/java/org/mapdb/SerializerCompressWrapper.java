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
import java.io.DataOutput;
import java.io.IOException;

/**
 * Wraps existing serializer and compresses its input/output
 * @author Jan Kotek
 */
public class SerializerCompressWrapper<E> implements Serializer<E>{


    protected final Serializer<E> serializer;

    protected final CompressLZFSerializer compress = new CompressLZFSerializer();

    public SerializerCompressWrapper(Serializer<E> serializer) {
        this.serializer = serializer;
    }

    @Override
    public void serialize(DataOutput out, E value) throws IOException {
        //serialize to byte[]
        DataOutput2 out2 = new DataOutput2();
        serializer.serialize(out2, value);
        byte[] b = out2.copyBytes();
        compress.serialize(out, b);
    }

    @Override
    public E deserialize(DataInput in, int available) throws IOException {
        byte[] b = compress.deserialize(in, available);
        DataInput2 in2 = new DataInput2(b);
        return serializer.deserialize(in2, b.length);
    }
}
