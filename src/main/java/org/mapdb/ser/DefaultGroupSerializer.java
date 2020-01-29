package org.mapdb.ser;

import org.mapdb.io.DataIO;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by jan on 2/29/16.
 */
public abstract class DefaultGroupSerializer<A> implements GroupSerializer<A, Object[]> {


    @Override public void valueArraySerialize(DataOutput2 out, Object[] vals) {
        for(Object o:vals){
            serialize(out, (A)o);
        }
    }

    @Override public Object[] valueArrayDeserialize(DataInput2 in, int size) {
        Object[] ret = new Object[size];
        for(int i=0;i<size;i++){
            ret[i] = deserialize(in);
        }
        return ret;
    }

    @Override public A valueArrayGet(Object[] vals, int pos){
        return (A) vals[pos];
    }

    @Override public int valueArraySize(Object[] vals){
        return vals.length;
    }

    @Override public Object[] valueArrayEmpty(){
        return new Object[0];
    }

    @Override public Object[] valueArrayPut(Object[] vals, int pos, A newValue) {
        return DataIO.arrayPut((Object[])vals, pos, newValue);
    }

    @Override public Object[] valueArrayUpdateVal(Object[] vals, int pos, A newValue) {
        Object[] vals2 = vals;
        vals2 = vals2.clone();
        vals2[pos] = newValue;
        return vals2;
    }

    @Override public Object[] valueArrayFromArray(Object[] objects) {
        return objects;
    }

    @Override public Object[] valueArrayCopyOfRange(Object[] vals, int from, int to) {
        return Arrays.copyOfRange(vals, from, to);
    }

    @Override public Object[] valueArrayDeleteValue(Object[] vals, int pos) {
        return DataIO.arrayDelete(vals, pos, 1);
    }

    @Override public  int valueArraySearch(Object[] keys, A key){
        return Arrays.binarySearch(keys, key, (Comparator<Object>)this);
    }

    @Override public Object[] valueArrayToArray(Object[] vals){
        return (Object[]) vals;
    }
    @Override public  int valueArraySearch(Object[] keys, A key, Comparator comparator){
        if(comparator==this)
            return valueArraySearch(keys, key);
        return Arrays.binarySearch(keys, key, comparator);
    }


}
