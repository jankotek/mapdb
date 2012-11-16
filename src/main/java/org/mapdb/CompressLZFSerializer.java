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
import java.util.Arrays;

/**
 * Compress everything using LZF
 *
 * @author Jan Kotek
 */
public class CompressLZFSerializer implements Serializer<byte[]> {


    final ThreadLocal<CompressLZF> LZF = new ThreadLocal<CompressLZF>() {
        @Override
        protected CompressLZF initialValue() {
            return new CompressLZF();
        }
    };

    @Override
    public void serialize(DataOutput out, byte[] value) throws IOException {
        if (value == null) return;

        CompressLZF lzf = LZF.get();
        byte[] outbuf = new byte[value.length + 40];
        int len = lzf.compress(value, value.length, outbuf, 0);
        //check if compressed data are larger then original
        if (value.length <= len) {
            //in this case do not compress data, write 0 as indicator
            Utils.packInt(out, 0);
            out.write(value);
        } else {
            Utils.packInt(out, value.length); //write original decompressed size
            out.write(outbuf, 0, len);
        }
    }

    @Override
    public byte[] deserialize(DataInput in, int available) throws IOException {
        if (available == 0) return null;
        //get original decompressed size
        DataInput2 in2 = (DataInput2) in;
        int origPos = in2.pos;
        int expendedLen = Utils.unpackInt(in);
        byte[] inbuf = new byte[available - (in2.pos - origPos)];
        in.readFully(inbuf);
        if (expendedLen == 0) {
            //special case, data are not compressed
            return inbuf;
        }
        byte[] outbuf = new byte[expendedLen + 40];

        CompressLZF lzf = LZF.get();
        lzf.expand(inbuf, 0, inbuf.length, outbuf, 0, expendedLen);
        outbuf = Arrays.copyOf(outbuf, expendedLen);

        return outbuf;
    }

}
