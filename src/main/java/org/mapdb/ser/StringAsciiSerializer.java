package org.mapdb.ser;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;

/**
 * Created by jan on 2/28/16.
 */
public class StringAsciiSerializer extends DefaultGroupSerializer<String> {
    @Override
    public void serialize(DataOutput2 out, String value) {
        int size = value.length();
        out.packInt(size);
        for (int i = 0; i < size; i++) {
            out.write(value.charAt(i));
        }
    }

    @Override
    public String deserialize(DataInput2 in) {
        int size = in.unpackInt();
        StringBuilder result = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            result.append((char) in.readUnsignedByte());
        }
        return result.toString();
    }

    @Nullable
    @Override
    public Class serializedType() {
        return String.class;
    }

    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public int hashCode(@NotNull String s, int seed) {
        return Serializers.STRING.hashCode(s, seed);
    }

    //        @Override
//        public BTreeKeySerializer getBTreeKeySerializer(Comparator comparator) {
//            if(comparator!=null && comparator!=Fun.COMPARATOR) {
//                return super.getBTreeKeySerializer(comparator);
//            }
//            return BTreeKeySerializer.STRING; //PERF ascii specific serializer?
//        }

}
