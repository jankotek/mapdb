package org.mapdb.flat;

import java.nio.ByteBuffer;

public class LongBufStack{

    private int pos = 0;


    //TODO LongBuffer?
    private final ByteBuffer buf;

    public LongBufStack(ByteBuffer buf) {
        this.buf = buf;
    }

    public LongBufStack(){
        this(ByteBuffer.allocate(8*1024*1024));
    }

    public long pop(){
        if(pos==0)
            return 0L;
        return buf.getLong(8*(--pos));
    }

    public void push(long value){
        if(value == 0L)
            throw new IllegalArgumentException();

        buf.putLong(8*(pos++), value);
    }

}
