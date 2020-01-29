package org.mapdb.ser;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;

/**
 * Created by jan on 2/29/16.
 */
public class ByteArrayDeltaSerializer extends ByteArraySerializer {

    //TODO PERF char[][] versus Object[]

    @Override
    public void valueArraySerialize(DataOutput2 out, byte[][] vals) {
        byte[][] chars = (byte[][]) vals;
        //write lengths
        for(byte[] b:chars){
            out.packInt(b.length);
        }
        //$DELAY$
        //find common prefix
        int prefixLen = commonPrefixLen(chars);
        out.packInt(prefixLen);
        out.write(chars[0], 0, prefixLen);
        //$DELAY$
        for(byte[] b:chars){
            out.write(b,prefixLen,b.length-prefixLen);
        }


    }

    @Override
    public byte[][] valueArrayDeserialize(DataInput2 in, int size) {
        byte[][] ret = new byte[size][];

        //read lengths and init arrays
        for(int i=0;i<ret.length;i++){
            ret[i] = new byte[in.unpackInt()];
        }
        //$DELAY$
        //read and distribute common prefix
        int prefixLen = in.unpackInt();
        in.readFully(ret[0],0,prefixLen);
        for(int i=1;i<ret.length;i++){
            System.arraycopy(ret[0],0,ret[i],0,prefixLen);
        }
        //$DELAY$
        //read suffixes
        for (byte[] aRet : ret) {
            in.readFully(aRet, prefixLen, aRet.length - prefixLen);
        }

        return ret;
    }

    protected static int commonPrefixLen(byte[][] bytes) {
        //TODO PERF refactor to calculate minimal length first, to save comparations.
        for(int ret=0;;ret++){
            if(bytes[0].length==ret) {
                return ret;
            }
            byte byt = bytes[0][ret];
            for(int i=1;i<bytes.length;i++){
                if(bytes[i].length==ret || byt!=bytes[i][ret])
                    return ret;
            }
        }
    }

}
