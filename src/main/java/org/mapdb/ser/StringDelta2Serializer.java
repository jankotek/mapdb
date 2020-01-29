package org.mapdb.ser;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.CC;
import org.mapdb.DBException;
import org.mapdb.io.DataIO;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.StringDelta2Serializer.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by jan on 2/29/16.
 */
public class StringDelta2Serializer implements  GroupSerializer<String, StringArrayKeys> {

    public interface StringArrayKeys {

        int commonPrefixLen();

        int length();

        int[] getOffset();

        StringArrayKeys deleteKey(int pos);

        StringArrayKeys copyOfRange(int from, int to);

        StringArrayKeys putKey(int pos, String newKey);

        int compare(int pos1, String string);

        int compare(int pos1, int pos2);

        String getKeyString(int pos);

        boolean hasUnicodeChars();

        void serialize(DataOutput2 out, int prefixLen);
    }

    //PERF right now byte[] contains 7 bit characters, but it should be expandable to 8bit.
    public static final class ByteArrayKeys implements StringArrayKeys {
        final int[] offset;
        final byte[] array;

        ByteArrayKeys(int[] offset, byte[] array) {
            this.offset = offset;
            this.array = array;

            if(CC.ASSERT && ! (array.length==0 || array.length == offset[offset.length-1]))
                throw new DBException.DataCorruption("inconsistent array size");
        }

        ByteArrayKeys(DataInput2 in, int[] offsets, int prefixLen) {
            this.offset = offsets;
            array = new byte[offsets[offsets.length-1]];

            in.readFully(array, 0, prefixLen);
            for(int i=0; i<offsets.length-1;i++){
                System.arraycopy(array,0,array,offsets[i],prefixLen);
            }
            //$DELAY$
            //read suffixes
            int offset = prefixLen;
            for(int o:offsets){
                in.readFully(array,offset,o-offset);
                offset = o+prefixLen;
            }

        }

        ByteArrayKeys(int[] offsets, Object[] keys) {
            this.offset = offsets;
            //fill large array
            array = new byte[offsets[offsets.length-1]];
            int bbOffset = 0;
            //$DELAY$
            for (Object key : keys) {
                String str = (String) key;
                for (int j = 0; j < str.length(); j++) {
                    array[bbOffset++] = (byte) str.charAt(j);
                }
            }
        }

        @Override
        public int commonPrefixLen() {
            int lenMinus1 = offset.length-1;
            //$DELAY$
            for(int ret = 0;; ret++){
                if(offset[0]==ret)
                    return ret;
                byte byt = array[ret];
                for(int i=0;i<lenMinus1;i++){
                    int o = offset[i]+ret;
                    if( o==offset[i+1]  || //too long
                            array[o]!=byt //other character
                            )
                        return ret;
                }
            }
        }

        @Override
        public int length() {
            return offset.length;
        }

        @Override
        public int[] getOffset() {
            return offset;
        }

        @Override
        public ByteArrayKeys deleteKey(int pos) {
            int split = pos==0? 0: offset[pos-1];
            int next = offset[pos];

            byte[] bb = new byte[array.length - (next-split)];
            int[] offsets = new  int[offset.length - 1];

            System.arraycopy(array,0,bb,0,split);
            //$DELAY$
            System.arraycopy(array,next,bb,split,array.length-next);

            int minus=0;
            int plusI=0;
            for(int i=0;i<offsets.length;i++){
                if(i==pos){
                    //skip current item and normalize offsets
                    plusI=1;
                    minus = next-split;
                }
                offsets[i] = offset[i+plusI] - minus;
            }
            return new ByteArrayKeys(offsets,bb);
        }

        @Override
        public ByteArrayKeys copyOfRange(int from, int to) {
            int start = from==0? 0: offset[from-1];
            int end = to==0? 0: offset[to-1];
            byte[] bb = Arrays.copyOfRange(array,start,end);
            //$DELAY$
            int[] offsets = new int[to-from];
            for(int i=0;i<offsets.length;i++){
                offsets[i] = offset[i+from] - start;
            }

            return new ByteArrayKeys(offsets,bb);
        }

        @Override
        public StringArrayKeys putKey(int pos, String newKey) {
            if(containsUnicode(newKey)){
                return CharArrayKeys.putKey(this,pos,newKey);
            }
            return putKey(pos,newKey.getBytes());
        }

        static final boolean containsUnicode(String str){
            int strLen = str.length();
            //$DELAY$
            for(int i=0;i<strLen;i++){
                if(str.charAt(i)>127)
                    return true;
            }
            return false;
        }

        public ByteArrayKeys putKey(int pos, byte[] newKey) {
            byte[] bb = new byte[array.length+ newKey.length];
            int split1 = pos==0? 0: offset[pos-1];
            System.arraycopy(array,0,bb,0,split1);
            //$DELAY$
            System.arraycopy(newKey,0,bb,split1,newKey.length);
            System.arraycopy(array,split1,bb,split1+newKey.length,array.length-split1);

            int[] offsets = new int[offset.length+1];

            int plus = 0;
            int plusI = 0;
            for(int i=0;i<offset.length;i++){
                if(i==pos){
                    //skip one item and increase space
                    plus = newKey.length;
                    plusI = 1;

                }
                offsets[i+plusI] = offset[i] + plus;
            }
            offsets[pos] = split1+newKey.length;

            return new ByteArrayKeys(offsets,bb);
        }

        public byte[] getKey(int pos) {
            int from =  pos==0 ? 0 : offset[pos-1];
            int to =  offset[pos];
            return Arrays.copyOfRange(array, from, to);
        }

        public int compare(int pos1, byte[] string) {
            int strLen = string.length;
            int start1 = pos1==0 ? 0 : offset[pos1-1];
            int start2 = 0;
            int len1 = offset[pos1] - start1;
            int len = Math.min(len1,strLen);
            //$DELAY$
            while(len-- != 0){
                int b1 = array[start1++] & 0xFF;
                int b2 = string[start2++] & 0xFF;
                if(b1!=b2){
                    return b1-b2;
                }
            }
            return len1 - strLen;
        }

        @Override
        public int compare(int pos1, String string) {
            int strLen = string.length();
            int start1 = pos1==0 ? 0 : offset[pos1-1];
            int start2 = 0;
            int len1 = offset[pos1] - start1;
            int len = Math.min(len1,strLen);
            //$DELAY$
            while(len-- != 0){
                int b1 =  (array[start1++] & 0xff);
                int b2 = string.charAt(start2++);
                if(b1!=b2){
                    return b1-b2;
                }
            }
            return len1 - strLen;
        }

        @Override
        public int compare(int pos1, int pos2) {
            int start1 = pos1==0 ? 0 : offset[pos1-1];
            int start2 = pos2==0 ? 0 : offset[pos2-1];
            int len1 = offset[pos1] - start1;
            int len2 = offset[pos2] - start2;
            int len = Math.min(len1,len2);
            //$DELAY$
            while(len-- != 0){
                int b1 = array[start1++] & 0xFF;
                int b2 = array[start2++] & 0xFF;
                if(b1!=b2){
                    return b1-b2;
                }
            }
            return len1 - len2;
        }

        @Override
        public String getKeyString(int pos) {
            byte[] ret = getKey(pos);
            StringBuilder sb = new StringBuilder(ret.length);
            for(byte b:ret){
                sb.append((char)b);
            }
            return sb.toString();
        }

        @Override
        public boolean hasUnicodeChars() {
            return false;
        }

        @Override
        public void serialize(DataOutput2 out, int prefixLen) {
            //write rest of the suffix
            out.write(array,0,prefixLen);
            //$DELAY$
            //write suffixes
            int aa = prefixLen;
            for(int o:offset){
                out.write(array, aa, o-aa);
                aa = o+prefixLen;
            }
        }
    }

    public static final class CharArrayKeys implements StringArrayKeys {
        final int[] offset;
        final char[] array;

        CharArrayKeys(int[] offset, char[] array) {
            this.offset = offset;
            this.array = array;

            if(CC.ASSERT && ! (array.length==0 || array.length == offset[offset.length-1]))
                throw new DBException.DataCorruption("inconsistent array size");
        }

        public CharArrayKeys(DataInput2 in, int[] offsets, int prefixLen) {
            this.offset = offsets;
            array = new char[offsets[offsets.length-1]];

            inReadFully(in, 0, prefixLen);
            for(int i=0; i<offsets.length-1;i++){
                System.arraycopy(array,0,array,offsets[i],prefixLen);
            }

            //read suffixes
            int offset = prefixLen;
            for(int o:offsets){
                inReadFully(in, offset, o);
                offset = o+prefixLen;
            }
        }

        CharArrayKeys(int[] offsets, Object[] keys) {
            this.offset = offsets;
            //fill large array
            array = new char[offsets[offsets.length-1]];
            int bbOffset = 0;
            for (Object key : keys) {
                String str = (String) key;
                str.getChars(0, str.length(), array, bbOffset);
                bbOffset += str.length();
            }
        }



        private void inReadFully(DataInput in, int from, int to) throws IOException {
            for(int i=from;i<to;i++){
                array[i] = (char) DataIO.unpackInt(in);
            }
        }

        private void inReadFully(DataInput2 in, int from, int to) {
            for(int i=from;i<to;i++){
                array[i] = (char) DataIO.unpackInt(in);
            }
        }


        @Override
        public int commonPrefixLen() {
            int lenMinus1 = offset.length-1;
            for(int ret = 0;; ret++){
                if(offset[0]==ret)
                    return ret;
                char byt = array[ret];
                for(int i=0;i<lenMinus1;i++){
                    int o = offset[i]+ret;
                    if( o==offset[i+1]  || //too long
                            array[o]!=byt //other character
                            )
                        return ret;
                }
            }
        }

        @Override
        public int length() {
            return offset.length;
        }

        @Override
        public int[] getOffset() {
            return offset;
        }

        @Override
        public CharArrayKeys deleteKey(int pos) {
            int split = pos==0? 0: offset[pos-1];
            int next = offset[pos];
            //$DELAY$
            char[] bb = new char[array.length - (next-split)];
            int[] offsets = new  int[offset.length - 1];

            System.arraycopy(array,0,bb,0,split);
            //$DELAY$
            System.arraycopy(array,next,bb,split,array.length-next);

            int minus=0;
            int plusI=0;
            for(int i=0;i<offsets.length;i++){
                if(i==pos){
                    //skip current item and normalize offsets
                    plusI=1;
                    minus = next-split;
                }
                offsets[i] = offset[i+plusI] - minus;
            }
            return new CharArrayKeys(offsets,bb);
        }

        @Override
        public CharArrayKeys copyOfRange(int from, int to) {
            int start = from==0? 0: offset[from-1];
            int end = to==0? 0: offset[to-1];
            char[] bb = Arrays.copyOfRange(array,start,end);
            //$DELAY$
            int[] offsets = new int[to-from];
            for(int i=0;i<offsets.length;i++){
                offsets[i] = offset[i+from] - start;
            }

            return new CharArrayKeys(offsets,bb);
        }

        @Override
        public CharArrayKeys putKey(int pos, String newKey) {
            int strLen = newKey.length();
            char[] bb = new char[array.length+ strLen];
            int split1 = pos==0? 0: offset[pos-1];
            //$DELAY$
            System.arraycopy(array,0,bb,0,split1);
            newKey.getChars(0,strLen,bb,split1);
            System.arraycopy(array,split1,bb,split1+strLen,array.length-split1);

            int[] offsets = new int[offset.length+1];

            int plus = 0;
            int plusI = 0;
            for(int i=0;i<offset.length;i++){
                if(i==pos){
                    //skip one item and increase space
                    plus = strLen;
                    plusI = 1;
                }
                offsets[i+plusI] = offset[i] + plus;
            }
            offsets[pos] = split1+strLen;

            return new CharArrayKeys(offsets,bb);
        }

        public static StringArrayKeys putKey(ByteArrayKeys kk, int pos, String newKey) {
            int strLen = newKey.length();
            char[] bb = new char[kk.array.length+ strLen];
            int split1 = pos==0? 0: kk.offset[pos-1];
            for(int i=0;i<split1;i++){
                bb[i] = (char) kk.array[i];
            }
            newKey.getChars(0,strLen,bb,split1);
            for(int i=split1;i<kk.array.length;i++){
                bb[i+strLen] = (char) kk.array[i];
            }
            int[] offsets = new int[kk.offset.length+1];
            int plus = 0;
            int plusI = 0;
            //$DELAY$
            for(int i=0;i<kk.offset.length;i++){
                if(i==pos){
                    //skip one item and increase space
                    plus = strLen;
                    plusI = 1;
                }
                offsets[i+plusI] = kk.offset[i] + plus;
            }
            offsets[pos] = split1+strLen;

            return new CharArrayKeys(offsets,bb);

        }


        @Override
        public int compare(int pos1, String string) {
            int strLen = string.length();
            int start1 = pos1==0 ? 0 : offset[pos1-1];
            int start2 = 0;
            int len1 = offset[pos1] - start1;
            int len = Math.min(len1,strLen);
            //$DELAY$
            while(len-- != 0){
                char b1 = array[start1++];
                char b2 = string.charAt(start2++);
                if(b1!=b2){
                    return b1-b2;
                }
            }
            return len1 - strLen;
        }

        @Override
        public int compare(int pos1, int pos2) {
            int start1 = pos1==0 ? 0 : offset[pos1-1];
            int start2 = pos2==0 ? 0 : offset[pos2-1];
            int len1 = offset[pos1] - start1;
            int len2 = offset[pos2] - start2;
            int len = Math.min(len1,len2);
            //$DELAY$
            while(len-- != 0){
                char b1 = array[start1++];
                char b2 = array[start2++];
                if(b1!=b2){
                    return b1-b2;
                }
            }
            return len1 - len2;
        }

        @Override
        public String getKeyString(int pos) {
            int from =  pos==0 ? 0 : offset[pos-1];
            int len =  offset[pos]-from;
            return new String(array,from,len);
        }

        @Override
        public boolean hasUnicodeChars() {
            for(char c:array){
                if(c>127)
                    return true;
            }
            return false;
        }

        @Override
        public void serialize(DataOutput2 out, int prefixLen) {
            //write rest of the suffix
            outWrite(out, 0, prefixLen);
            //$DELAY$
            //write suffixes
            int aa = prefixLen;
            for(int o:offset){
                outWrite(out,  aa, o);
                aa = o+prefixLen;
            }
        }

        private void outWrite(DataOutput2 out, int from, int to) {
            for(int i=from;i<to;i++){
                DataIO.packInt(out,array[i]);
            }
        }

    }


    @Override
    public StringArrayKeys valueArrayDeserialize(DataInput2 in2, int size) {
        //read data sizes
        int[] offsets = new int[size];
        int old=0;
        for(int i=0;i<size;i++){
            old+= in2.unpackInt();
            offsets[i]=old;
        }
        //$DELAY$
        //read and distribute common prefix
        int prefixLen = in2.unpackInt();
        boolean useUnicode = (0!=(prefixLen & 1));
        prefixLen >>>=1;
        //$DELAY$
        return useUnicode?
                new CharArrayKeys(in2,offsets,prefixLen):
                new ByteArrayKeys(in2,offsets,prefixLen);

    }

    @Override
    public void valueArraySerialize(DataOutput2 out, StringArrayKeys vals) {
        StringArrayKeys keys = (StringArrayKeys) vals;
        int offset = 0;
        //write sizes
        for(int o: keys.getOffset()){
            out.packInt(o-offset);
            offset = o;
        }
        //$DELAY$
        int unicode = keys.hasUnicodeChars()?1:0;

        //find and write common prefix
        int prefixLen = keys.commonPrefixLen();
        out.packInt((prefixLen<<1) | unicode);
        keys.serialize(out, prefixLen);
    }

    @Override
    public StringArrayKeys valueArrayCopyOfRange(StringArrayKeys vals, int from, int to) {
        return ((StringArrayKeys)vals).copyOfRange(from,to);
    }

    @Override
    public StringArrayKeys valueArrayDeleteValue(StringArrayKeys vals, int pos) {
        //return vals.deleteKey(pos);
        Object[] vv = valueArrayToArray(vals);
        vv = DataIO.arrayDelete(vv, pos, 1);
        return valueArrayFromArray(vv);
    }

    @Override
    public StringArrayKeys valueArrayEmpty() {
        return new ByteArrayKeys(new int[0], new byte[0]);
    }

    @Override
    public StringArrayKeys valueArrayFromArray(Object[] keys) {
        if(keys.length==0)
            return valueArrayEmpty();
        //$DELAY$
        boolean unicode = false;

        //fill offsets
        int[] offsets = new int[keys.length];

        int old=0;
        for(int i=0;i<keys.length;i++){
            String b = (String) keys[i];

            if(!unicode && ByteArrayKeys.containsUnicode(b)) {
                unicode = true;
            }

            old+=b.length();
            offsets[i]=old;
        }

        return unicode?
                new CharArrayKeys(offsets, keys):
                new ByteArrayKeys(offsets, keys);
    }

    @Override
    public String valueArrayGet(StringArrayKeys vals, int pos) {
        return ((StringArrayKeys)vals).getKeyString(pos);
    }

    @Override
    public StringArrayKeys valueArrayPut(StringArrayKeys vals, int pos, String newValue) {
        return ((StringArrayKeys)vals).putKey(pos, newValue);
    }

    @Override
    public int valueArraySearch(StringArrayKeys keys, String key) {
        //TODO PERF optimize search
        Object[] v = valueArrayToArray(keys);
        return Arrays.binarySearch(v, key, (Comparator)this);
    }

    @Override
    public int valueArraySearch(StringArrayKeys keys, String key, Comparator comparator) {
        //TODO PERF optimize search
        Object[] v = valueArrayToArray(keys);
        return Arrays.binarySearch(v, key, comparator);
    }


    @Override
    public int valueArraySize(StringArrayKeys vals) {
        return ((StringArrayKeys)vals).length();
    }

    @Override
    public StringArrayKeys valueArrayUpdateVal(StringArrayKeys vals, int pos, String newValue) {
        //TODO PERF optimize value update
        Object[] v = valueArrayToArray(vals);
        v[pos] = newValue;
        return valueArrayFromArray(v);
    }

    @Override
    public boolean isTrusted() {
        return true;
    }


    @Override
    public void serialize(@NotNull DataOutput2 out, @NotNull String value) {
        Serializers.STRING.serialize(out, value);
    }

    @Override
    public String deserialize(@NotNull DataInput2 input) {
        return Serializers.STRING.deserialize(input);
    }

    @Nullable
    @Override
    public Class serializedType() {
        return String.class;
    }

    @Override
    public int hashCode(@NotNull String s, int seed) {
        return Serializers.STRING.hashCode(s, seed);
    }
}
