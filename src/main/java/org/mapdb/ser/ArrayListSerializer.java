package org.mapdb.ser;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.util.ArrayList;

public class ArrayListSerializer<E> implements Serializer<ArrayList<E>> {

    protected final Serializer<E> ser;

    public ArrayListSerializer(Serializer<E> ser) {
        this.ser = ser;
    }

    @Override
    public void serialize(@NotNull DataOutput2 out, @NotNull ArrayList<E> list) {
        out.writePackedInt(list.size());
        for(E e:list){
            ser.serialize(out,e);
        }
    }

    @Override
    public ArrayList<E> deserialize(@NotNull DataInput2 input) {
        final int size = input.readPackedInt();
        ArrayList<E> list = new ArrayList<>(size);
        for(int i=0;i<size;i++){
            E e = ser.deserialize(input);
            list.add(e);
        }
        return list;
    }

    @Override
    public @Nullable Class serializedType() {
        return ArrayListSerializer.class;
    }
}
