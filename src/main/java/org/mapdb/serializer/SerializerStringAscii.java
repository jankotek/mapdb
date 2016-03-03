package org.mapdb.serializer;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerStringAscii extends GroupSerializerObjectArray<String> {
    @Override
    public void serialize(DataOutput2 out, String value) throws IOException {
        int size = value.length();
        out.packInt(size);
        for (int i = 0; i < size; i++) {
            out.write(value.charAt(i));
        }
    }

    @Override
    public String deserialize(DataInput2 in, int available) throws IOException {
        int size = in.unpackInt();
        StringBuilder result = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            result.append((char) in.readUnsignedByte());
        }
        return result.toString();
    }

    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public int hashCode(@NotNull String s, int seed) {
        return STRING.hashCode(s, seed);
    }

    //        @Override
//        public BTreeKeySerializer getBTreeKeySerializer(Comparator comparator) {
//            if(comparator!=null && comparator!=Fun.COMPARATOR) {
//                return super.getBTreeKeySerializer(comparator);
//            }
//            return BTreeKeySerializer.STRING; //PERF ascii specific serializer?
//        }

}
