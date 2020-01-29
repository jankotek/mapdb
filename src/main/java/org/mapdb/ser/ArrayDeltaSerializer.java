package org.mapdb.ser;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;

/**
 * Created by jan on 2/28/16.
 */
public class ArrayDeltaSerializer<T> extends ArraySerializer<T> {

    private static final long serialVersionUID = -930920902390439234L;


    public ArrayDeltaSerializer(Serializer<T> serializer) {
        super(serializer);
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, Object[] vals2) {
        Object[] vals = (Object[]) vals2;
        if (vals.length == 0)
            return;
        //write first array
        Object[] prevKey = (Object[]) vals[0];
        out.packInt(prevKey.length);
        for (Object key : prevKey) {
            serializer.serialize(out, (T) key);
        }

        //write remaining arrays
        for (int i = 1; i < vals.length; i++) {
            Object[] key = (Object[]) vals[i];
            //calculate number of entries equal with prevKey
            int len = Math.min(key.length, prevKey.length);
            int pos = 0;
            while (pos < len && (key[pos] == prevKey[pos] || serializer.equals((T) key[pos], (T) prevKey[pos]))) {
                pos++;
            }
            out.packInt(pos);
            //write remaining bytes
            out.packInt(key.length - pos);
            for (; pos < key.length; pos++) {
                serializer.serialize(out, (T) key[pos]);
            }
            prevKey = key;
        }

    }

    @Override
    public Object[] valueArrayDeserialize(DataInput2 in, final int size) {
        Object[] ret = new Object[size];
        if (size == 0)
            return ret;
        int ss = in.unpackInt();
        Object[] prevKey = new Object[ss];
        for (int i = 0; i < ss; i++) {
            prevKey[i] = serializer.deserialize(in, -1);
        }
        ret[0] = prevKey;
        for (int i = 1; i < size; i++) {
            //number of items shared with prev
            int shared = in.unpackInt();
            //number of items unique to this array
            int unq = in.unpackInt();
            Object[] key = new Object[shared + unq];
            //copy items from prev array
            System.arraycopy(prevKey, 0, key, 0, shared);
            //and read rest
            for (; shared < key.length; shared++) {
                key[shared] = serializer.deserialize(in, -1);
            }
            ret[i] = key;
            prevKey = key;
        }
        return ret;
    }

}
