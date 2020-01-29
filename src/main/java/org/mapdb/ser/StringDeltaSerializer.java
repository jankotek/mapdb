package org.mapdb.ser;

import org.mapdb.io.DataIO;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;

/**
 * Created by jan on 2/29/16.
 */
public class StringDeltaSerializer extends StringSerializer{


    protected static int commonPrefixLen(char[][] chars) {
        //$DELAY$
        for(int ret=0;;ret++){
            if(chars[0].length==ret) {
                return ret;
            }
            char byt = chars[0][ret];
            for(int i=1;i<chars.length;i++){
                if(chars[i].length==ret || byt!=chars[i][ret])
                    return ret;
            }
        }
    }

    @Override
    public char[][] valueArrayDeserialize(DataInput2 in, int size) {
        char[][] ret = new char[size][];
        //$DELAY$
        //read lengths and init arrays
        for(int i=0;i<ret.length;i++){
            int len = in.unpackInt();
            ret[i] = new char[len];
        }
        //$DELAY$
        //read and distribute common prefix
        int prefixLen = in.unpackInt();
        //$DELAY$
        for(int i=0;i<prefixLen;i++){
            ret[0][i] = (char) in.unpackInt();
        }

        for(int i=1;i<ret.length;i++){
            System.arraycopy(ret[0],0,ret[i],0,prefixLen);
        }
        //$DELAY$
        //read suffixes
        for(char[] b:ret){
            for(int j=prefixLen;j<b.length;j++){
                b[j] = (char) in.unpackInt();
            }
        }
        //$DELAY$
        return ret;
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, char[][] chars2) {
        char[][] chars = (char[][]) chars2;
        //write lengths
        for(char[] b:chars){
            out.packInt(b.length);
            //$DELAY$
        }

        //find common prefix
        int prefixLen = commonPrefixLen(chars);
        DataIO.packInt(out,prefixLen);
        char[] first = chars[0];
        for (int i = 0; i < prefixLen; i++) {
            out.packInt(first[i]);
        }
        //$DELAY$
        for(char[] b:chars){
            for (int i = prefixLen; i < b.length; i++) {
                out.packInt(b[i]);
            }
        }
    }


// this might be useful in future
//    /** compares two char arrays, has same contract as {@link String#compareTo(String)} */
//    int compare(char[] c1, char[] c2){
//        int end = (c1.length <= c2.length) ? c1.length : c2.length;
//        int ret;
//        //$DELAY$
//        for(int i=0;i<end;i++){
//            if ((ret = c1[i] - c2[i]) != 0) {
//                return ret;
//            }
//        }
//        //$DELAY$
//        return c1.length - c2.length;
//    }
//
//
//    /** compares char array and string, has same contract as {@link String#compareTo(String)} */
//    int compare(char[] c1, String c2){
//        int end = Math.min(c1.length,c2.length());
//        int ret;
//        //$DELAY$
//        for(int i=0;i<end;i++){
//            if ((ret = c1[i] - c2.charAt(i)) != 0) {
//                return ret;
//            }
//        }
//        //$DELAY$
//        return c1.length - c2.length();
//    }


}
