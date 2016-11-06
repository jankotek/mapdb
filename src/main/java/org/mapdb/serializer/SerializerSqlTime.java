/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mapdb.serializer;

import java.io.IOException;
import java.util.Arrays;
import java.sql.Time;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.serializer.SerializerEightByte;

/**
 *
 * @author Per Minborg
 */
public class SerializerSqlTime extends SerializerEightByte<Time> {

    @Override
    public void serialize(DataOutput2 out, Time value) throws IOException {
        out.writeLong(value.getTime());
    }

    @Override
    public Time deserialize(DataInput2 in, int available) throws IOException {
        return new Time(in.readLong());
    }

    @Override
    protected Time unpack(long l) {
        return new Time(l);
    }

    @Override
    protected long pack(Time l) {
        return l.getTime();
    }

    @Override
    final public int valueArraySearch(Object keys, Time key) {
        //TODO valueArraySearch versus comparator test
        long time = key.getTime();
        return Arrays.binarySearch((long[])keys, time);
    }
    
}
