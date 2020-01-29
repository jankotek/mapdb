package org.mapdb.ser;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;
import java.util.Comparator;

/**
 * Created by jan on 2/29/16.
 */
public interface GroupSerializer<A,G> extends Serializer<A> {

    default A valueArrayBinaryGet(DataInput2 input, int keysLen, int pos) {
        G keys = valueArrayDeserialize(input, keysLen);
        return valueArrayGet(keys, pos);
//        A a=null;
//        while(pos-- >= 0){
//            a = deserialize(input, -1);
//        }
//        return a;
    }



    default int valueArrayBinarySearch(A key, DataInput2 input, int keysLen, Comparator comparator) {
        G keys = valueArrayDeserialize(input, keysLen);
        return valueArraySearch(keys, key, comparator);
//        for(int pos=0; pos<keysLen; pos++){
//            A from = deserialize(input, -1);
//            int comp = compare(key, from);
//            if(comp==0)
//                return pos;
//            if(comp<0)
//                return -(pos+1);
//        }
//        return -(keysLen+1);
    }


    int valueArraySearch(G keys, A key);

    int valueArraySearch(G keys, A key, Comparator comparator);

    void valueArraySerialize(DataOutput2 out, G vals);

    G valueArrayDeserialize(DataInput2 in, int size);

    A valueArrayGet(G vals, int pos);

    int valueArraySize(G vals);

    G valueArrayEmpty();

    G valueArrayPut(G vals, int pos, A newValue);


    G valueArrayUpdateVal(G vals, int pos, A newValue);

    G valueArrayFromArray(Object[] objects);

    G valueArrayCopyOfRange(G vals, int from, int to);

    G valueArrayDeleteValue(G vals, int pos);

    default Object[] valueArrayToArray(G vals){
        Object[] ret = new Object[valueArraySize(vals)];
        for(int i=0;i<ret.length;i++){
            ret[i] = valueArrayGet(vals,i);
        }
        return ret;
    }


    /** returns value+1, or null if there is no bigger value. */
    default A nextValue(A value){
        throw new UnsupportedOperationException("Next Value not supported");
    }

}
