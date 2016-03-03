package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.util.Comparator;

/**
 * Created by jan on 2/29/16.
 */
public interface GroupSerializer<A> extends Serializer<A> {

    default A valueArrayBinaryGet(DataInput2 input, int keysLen, int pos) throws IOException {
        Object keys = valueArrayDeserialize(input, keysLen);
        return valueArrayGet(keys, pos);
//        A a=null;
//        while(pos-- >= 0){
//            a = deserialize(input, -1);
//        }
//        return a;
    }



    default int valueArrayBinarySearch(A key, DataInput2 input, int keysLen, Comparator comparator) throws IOException {
        Object keys = valueArrayDeserialize(input, keysLen);
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


    int valueArraySearch(Object keys, A key);

    int valueArraySearch(Object keys, A key, Comparator comparator);

    void valueArraySerialize(DataOutput2 out, Object vals) throws IOException;

    Object valueArrayDeserialize(DataInput2 in, int size) throws IOException;

    A valueArrayGet(Object vals, int pos);

    int valueArraySize(Object vals);

    Object valueArrayEmpty();

    Object valueArrayPut(Object vals, int pos, A newValue);


    Object valueArrayUpdateVal(Object vals, int pos, A newValue);

    Object valueArrayFromArray(Object[] objects);

    Object valueArrayCopyOfRange(Object vals, int from, int to);

    Object valueArrayDeleteValue(Object vals, int pos);

    default Object[] valueArrayToArray(Object vals){
        Object[] ret = new Object[valueArraySize(vals)];
        for(int i=0;i<ret.length;i++){
            ret[i] = valueArrayGet(vals,i);
        }
        return ret;
    }

}
