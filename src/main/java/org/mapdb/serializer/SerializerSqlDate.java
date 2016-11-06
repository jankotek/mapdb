/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mapdb.serializer;

import java.io.IOException;
import java.util.Arrays;
import java.sql.Date;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.serializer.SerializerEightByte;

/**
 *
 * @author Per Minborg
 */
public class SerializerSqlDate extends SerializerEightByte<Date> {

    @Override
    public void serialize(DataOutput2 out, Date value) throws IOException {
        out.writeLong(value.getTime());
    }

    @Override
    public Date deserialize(DataInput2 in, int available) throws IOException {
        return new Date(in.readLong());
    }

    @Override
    protected Date unpack(long l) {
        return new Date(l);
    }

    @Override
    protected long pack(Date l) {
        return l.getTime();
    }

    @Override
    final public int valueArraySearch(Object keys, Date key) {
        //TODO valueArraySearch versus comparator test
        long time = key.getTime();
        return Arrays.binarySearch((long[])keys, time);
    }
    
}
