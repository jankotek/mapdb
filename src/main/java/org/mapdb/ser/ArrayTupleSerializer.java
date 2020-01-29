package org.mapdb.ser;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.CC;
import org.mapdb.io.DataIO;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Serializer for tuples. It serializes fixed size array, where each array index can use different serializer.
 *
 * It takes array of serializes in constructor parameter. All tuples (arrays) must have the same size.
 */
public class ArrayTupleSerializer implements GroupSerializer<Object[],Object[]> {

    protected final Serializer[] ser;
    protected final Comparator[] comp;
    protected final int size;

    public ArrayTupleSerializer(Serializer[] serializers, Comparator[] comparators) {
        this.ser = serializers.clone();
        this.comp = comparators.clone();
        this.size = ser.length;
    }

    public ArrayTupleSerializer(Serializer... serializers) {
        this.ser = serializers.clone();
        this.comp = ser;
        this.size = ser.length;
    }


    protected Object[] cast(Object o){
        if(CC.ASSERT && ((Object[])o).length%size!=0) {
            throw new AssertionError();
        }
        return (Object[])o;
    }

    @Override
    public void serialize(@NotNull DataOutput2 out, @NotNull Object[] value) {
        for(int i=0;i<size;i++){
            ser[i].serialize(out, value[i]);
        }
    }

    @Override
    public Object[] deserialize(@NotNull DataInput2 input) {
        Object[] v = new Object[size];
        for(int i=0;i<size;i++){
            v[i] = ser[i].deserialize(input,-1);
        }
        return v;
    }

    @Nullable
    @Override
    public Class serializedType() {
        return Object[].class;
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, Object[] vals) {
        //TODO delta compression
        Object[] v = cast(vals);
        for(int i=0;i<v.length;i++){
            ser[i%size].serialize(out,v[i]);
        }
    }

    @Override
    public Object[] valueArrayDeserialize(DataInput2 in, int size) {
        Object[] v = new Object[size * this.size];
        for (int i = 0; i < v.length; i++) {
            v[i] = ser[i%this.size].deserialize(in,-1);
        }
        return v;
    }


    @Override
    public int valueArraySearch(Object[] keys, Object[] key) {
        return Arrays.binarySearch(valueArrayToArray(keys), key, (Serializer)this);
    }

    @Override
    public int valueArraySearch(Object[] keys, Object[] key, Comparator comparator) {
        return Arrays.binarySearch(valueArrayToArray(keys), key, comparator);
    }


    @Override
    public Object[] valueArrayGet(Object[] vals, int pos) {
        pos*=size;
        return Arrays.copyOfRange(cast(vals), pos, pos+size);
    }

    @Override
    public int valueArraySize(Object[] vals) {
        return cast(vals).length/size;
    }

    @Override
    public Object[] valueArrayEmpty() {
        return new Object[0];
    }

    @Override
    public Object[] valueArrayPut(Object[] vals, int pos, Object[] newValue) {
        if(CC.ASSERT && newValue.length%size!=0)
            throw new AssertionError();
        Object[] array = cast(vals);
        pos*=size;
        final Object[] ret = Arrays.copyOf(array, array.length+size);
        if(pos<array.length){
            System.arraycopy(array, pos, ret, pos+size, array.length-pos);
        }
        System.arraycopy(newValue,0,ret,pos,size);
        return ret;
    }

    @Override
    public Object[] valueArrayUpdateVal(Object[] vals, int pos, Object[] newValue) {
        if(CC.ASSERT && newValue.length!=size)
            throw new AssertionError();
        Object[] ret = cast(vals).clone();
        System.arraycopy(newValue, 0, ret, pos*size, size);
        return ret;
    }

    @Override
    public Object[] valueArrayFromArray(Object[] objects) {
        Object[] v = new Object[objects.length*size];
        int pos = 0;
        for(Object oo:objects){
            Object[] oo2= (Object[]) oo;
            if(CC.ASSERT && oo2.length!=size)
                throw new AssertionError();
            for(Object o:oo2)
                v[pos++]=o;
        }
        return v;
    }

    @Override
    public Object[] valueArrayCopyOfRange(Object[] vals, int from, int to) {
        return Arrays.copyOfRange(cast(vals), from*size, to*size);
    }

    @Override
    public Object[] valueArrayDeleteValue(Object[] vals0, int pos) {
        pos*=size;
        Object[] vals = cast(vals0);
        Object[] vals2 = new Object[vals.length-size];
        System.arraycopy(vals, 0, vals2, 0, pos-size);
        System.arraycopy(vals, pos, vals2, pos-size, vals2.length-(pos-size));
        return vals2;
    }

    @Override
    public Object[] nextValue(Object[] value) {
        return Arrays.copyOf(value, value.length+1); //it expands array by one, and insert null at last position.
    }


    @Override
    public boolean equals(Object[] a1, Object[] a2) {
        if(a1.length!=a2.length)
            return false;
        for(int i=0;i<a1.length;i++){
            if(!ser[i%size].equals(a1[i],a2[i]))
                return false;
        }
        return true;
    }

    @Override
    public int compare(Object[] o1, Object[] o2) {

        int len = Math.min(o1.length, o2.length);

        for(int i=0;i<len;i++){
            Object a1 = o1[i];
            Object a2 = o2[i];
            if(a1==a2)
                continue;
            if(a1==null)
                return 1;
            if(a2==null)
                return -1;
            int res = comp[i].compare(a1, a2);
            if(res!=0)
                return res;
        }
        return Integer.compare(o1.length,o2.length);
    }

    @Override
    public int hashCode(@NotNull Object[] objects, int seed) {
        if(CC.ASSERT && objects.length%size!=0)
            throw new AssertionError();
        for(int i=0;i<objects.length;i++){
            seed+= DataIO.intHash(ser[i].hashCode(objects[i],seed));
        }
        return seed;
    }

    @Override
    public boolean isTrusted() {
        for(Serializer s:ser)
            if(!s.isTrusted())
                return false;
        return true;
    }
}
