/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mapdb.serializer;

import java.io.IOException;
import java.util.Arrays;
import java.sql.Timestamp;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.serializer.SerializerEightByte;

/**
 *
 * @author Per Minborg
 */
public class SerializerSqlTimestamp extends SerializerEightByte<Timestamp> {

    @Override
    public void serialize(DataOutput2 out, Timestamp value) throws IOException {
        out.writeLong(value.getTime());
    }

    @Override
    public Timestamp deserialize(DataInput2 in, int available) throws IOException {
        return new Timestamp(in.readLong());
    }

    @Override
    protected Timestamp unpack(long l) {
        return new Timestamp(l);
    }

    @Override
    protected long pack(Timestamp l) {
        return l.getTime();
    }

    @Override
    final public int valueArraySearch(Object keys, Timestamp key) {
        //TODO valueArraySearch versus comparator test
        long time = key.getTime();
        return Arrays.binarySearch((long[])keys, time);
    }
    
}
