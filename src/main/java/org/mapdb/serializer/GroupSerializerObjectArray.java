package org.mapdb.serializer;

import org.mapdb.DBUtil;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by jan on 2/29/16.
 */
public abstract class GroupSerializerObjectArray<A> implements GroupSerializer<A> {


    @Override public void valueArraySerialize(DataOutput2 out, Object vals) throws IOException {
        for(Object o:(Object[])vals){
            serialize(out, (A) o);
        }
    }

    @Override public Object[] valueArrayDeserialize(DataInput2 in, int size) throws IOException {
        Object[] ret = new Object[size];
        for(int i=0;i<size;i++){
            ret[i] = deserialize(in,-1);
        }
        return ret;
    }

    @Override public A valueArrayGet(Object vals, int pos){
        return (A) ((Object[])vals)[pos];
    }

    @Override public int valueArraySize(Object vals){
        return ((Object[])vals).length;
    }

    @Override public Object[] valueArrayEmpty(){
        return new Object[0];
    }

    @Override public Object[] valueArrayPut(Object vals, int pos, A newValue) {
        return DBUtil.arrayPut((Object[])vals, pos, newValue);
    }

    @Override public Object[] valueArrayUpdateVal(Object vals, int pos, A newValue) {
        Object[] vals2 = (Object[]) vals;
        vals2 = vals2.clone();
        vals2[pos] = newValue;
        return vals2;
    }

    @Override public Object[] valueArrayFromArray(Object[] objects) {
        return objects;
    }

    @Override public Object[] valueArrayCopyOfRange(Object vals, int from, int to) {
        return Arrays.copyOfRange((Object[])vals, from, to);
    }

    @Override public Object[] valueArrayDeleteValue(Object vals, int pos) {
        return DBUtil.arrayDelete((Object[])vals, pos, 1);
    }

    @Override public  int valueArraySearch(Object keys, A key){
        return Arrays.binarySearch((Object[])keys, key, (Comparator<Object>)this);
    }

    @Override public Object[] valueArrayToArray(Object vals){
        return (Object[]) vals;
    }
    @Override public  int valueArraySearch(Object keys, A key, Comparator comparator){
        if(comparator==this)
            return valueArraySearch(keys, key);
        return Arrays.binarySearch((Object[])keys, key, comparator);
    }


}
