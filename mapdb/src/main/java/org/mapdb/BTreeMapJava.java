package org.mapdb;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Comparator;

/**
 * Java code for BTreeMap. Mostly performance sensitive code.
 */
public class BTreeMapJava {

    static final int DIR = 1<<3;
    static final int LEFT = 1<<2;
    static final int RIGHT = 1<<1;
    static final int LAST_KEY_DOUBLE = 1;

    static class Node{

        /** bit flags (dir, left most, right most, next key equal to last...) */
        final byte flags;
        /** link to next node */
        final long link;
        /** represents keys */
        final Object[] keys;
        /** represents values for leaf node, or ArrayLong of children for dir node  */
        final Object values;

        Node(int flags, long link, Object[] keys, Object values) {
            this.flags = (byte)flags;
            this.link = link;
            this.keys = keys;
            this.values = values;

            if(CC.ASSERT && isLastKeyDouble() && isDir())
                throw new AssertionError();

            if(CC.ASSERT && isDir() &&
                    keys.length - 1 + intLeftEdge() + intRightEdge()!=
                    ((long[])values).length)
                throw new AssertionError();

            if(CC.ASSERT && isRightEdge() && (link!=0L))
                throw new AssertionError();

            if(CC.ASSERT && !isRightEdge() && (link==0L))
                throw new AssertionError();

            if(CC.ASSERT && !isDir() &&
                    keys.length !=
                    ((Object[])values).length + 2 - intLeftEdge() - intRightEdge())
                throw new AssertionError();
        }

        int intDir(){
            return (flags>>>3)&1;
        }

        int intLeftEdge(){
            return (flags>>>2)&1;
        }

        int intRightEdge(){
            return (flags>>>1)&1;
        }

        int intLastKeyDouble(){
            return flags&1;
        }


        boolean isDir(){
            return ((flags>>>3)&1)==1;
        }

        boolean isLeftEdge(){
            return ((flags>>>2)&1)==1;
        }

        boolean isRightEdge(){
            return ((flags>>>1)&1)==1;
        }

        boolean isLastKeyDouble(){
            return ((flags)&1)==1;
        }
    }

    static class NodeSerializer extends Serializer<Node>{

        final Serializer keySerializer;
        final Serializer valueSerializer;

        NodeSerializer(Serializer keySerializer, Serializer valueSerializer) {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }

        @Override
        public void serialize(@NotNull DataOutput2 out, @NotNull Node value) throws IOException {

            if(CC.ASSERT && value.flags>>>4!=0)
                throw new AssertionError();
            int keysLen = value.keys.length;
            keysLen = (keysLen<<5) + value.flags<<1;
            keysLen = DataIO.parity1Set(keysLen);

            //keysLen and flags are combined into single packed long, that saves a byte for small nodes
            out.packInt(keysLen);
            if(!value.isRightEdge())
                out.packLong(value.link);
            keySerializer.serialize(out, value.keys);
            valueSerializer.serialize(out, value.values);
        }

        @Override
        public Node deserialize(@NotNull DataInput2 input, int available) throws IOException {
            int keysLen = DataIO.parity1Get(input.unpackInt())>>>1;
            byte flags = (byte) (keysLen & 0xF);
            keysLen = keysLen>>>4;
            long link =  ((flags>>>1)&1)==1
                    ? 0L :
                    input.unpackLong();

            Object[] keys = (Object[]) keySerializer.deserialize(input, 0);
            if(CC.ASSERT && keysLen!=keysLen)
                throw new AssertionError();

            Object values = valueSerializer.deserialize(input, 0);

            return new Node(flags, link, keys, values);
        }

        @Override
        public boolean isTrusted() {
            return keySerializer.isTrusted() && valueSerializer.isTrusted();
        }
    }

    public static final Comparator COMPARATOR = new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
            return o1.compareTo(o2);
        }
    };



    static long findChild(Node node, Comparator comparator, Object key){
        if(CC.ASSERT && !node.isDir())
            throw new AssertionError();
        //find an index
        Object[] keys = node.keys;
        int index = 1-node.intLeftEdge();
        int keysLen = keys.length;
        long[] children = (long[]) node.values;
        while(index!=keysLen && comparator.compare(key, keys[index])>0){
            index++;
        }

        index += -1+node.intLeftEdge();

        index = Math.max(0, index);

        if(index>=children.length) {
            if(CC.ASSERT && node.isRightEdge())
                throw new AssertionError();
            return node.link;
        }
        return children[index];
    }


    static int findValue(Node node, Comparator comparator, Object key){
        if(CC.ASSERT && node.isDir())
            throw new AssertionError();

        Object[] keys = node.keys;
        int index = 0;
        int keysLen = keys.length;
        while(index!=keysLen){
            int compare = comparator.compare(key, keys[index]);
            if(compare<0)
                return -index-1;
            else if(compare==0)
                return index;
            index++;
        }
        return -index-1;
    }

    static final Object LINK = new Object(){
        @Override
        public String toString() {
            return "BTreeMap.LINK";
        }
    };

    static Object leafGet(Node node, Comparator comparator, Object key){
        int pos = findValue(node, comparator, key);
        Object[] vals = (Object[]) node.values;
        if(pos<0+1-node.intLeftEdge()) {
            if(!node.isRightEdge() && pos<-node.keys.length )
                return LINK;
            else
                return null;
        }

        if(!node.isRightEdge() && pos==vals.length+1)
            return null;
        else if(pos>=vals.length+1){
            return LINK;
        }
        pos = pos-1+node.intLeftEdge();
        if(pos>=vals.length)
            return null;
        return vals[pos];
    }
}

