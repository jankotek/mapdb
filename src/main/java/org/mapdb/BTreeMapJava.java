package org.mapdb;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.serializer.GroupSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Java code for BTreeMap. Mostly performance sensitive code.
 */
public class BTreeMapJava {

    static final int DIR = 1<<3;
    static final int LEFT = 1<<2;
    static final int RIGHT = 1<<1;
    static final int LAST_KEY_DOUBLE = 1;

    public static class Node{

        /** bit flags (dir, left most, right most, next key equal to last...) */
        final byte flags;
        /** link to next node */
        final long link;
        /** represents keys */
        final Object keys;
        /** represents values for leaf node, or ArrayLong of children for dir node  */
        final Object values;

        Node(int flags, long link, Object keys, Object values, GroupSerializer keySerializer, GroupSerializer valueSerializer) {
            this(flags, link, keys, values);

            if(CC.ASSERT) {
                int keysLen = keySerializer.valueArraySize(keys);
                if (isDir()){
                    // compare directory size
                    if( keysLen - 1 + intLeftEdge() + intRightEdge() !=
                                ((long[]) values).length) {
                        throw new AssertionError();
                    }
                } else{
                    // compare leaf size
                    if (keysLen != valueSerializer.valueArraySize(values) + 2 - intLeftEdge() - intRightEdge() - intLastKeyTwice()) {
                        throw new AssertionError();
                    }
                }
            }
        }
        Node(int flags, long link, Object keys, Object values){
            this.flags = (byte)flags;
            this.link = link;
            this.keys = keys;
            this.values = values;

            if(CC.ASSERT && isLastKeyDouble() && isDir())
                throw new AssertionError();

            if(CC.ASSERT && isRightEdge() && (link!=0L))
                throw new AssertionError();

            if(CC.ASSERT && !isRightEdge() && (link==0L))
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

        int intLastKeyTwice(){
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

        boolean isEmpty(GroupSerializer keySerializer){
            int keySize = keySerializer.valueArraySize(keys);
            return !isLastKeyDouble() && keySize == 2-intLeftEdge()-intRightEdge();
        }

        @Nullable
        public <K> K highKey(GroupSerializer<K> keySerializer) {
            int keysLen = keySerializer.valueArraySize(keys);
            return keySerializer.valueArrayGet(keys, keysLen-1);
        }

        public long[] getChildren(){
            return (long[]) values;
        }
    }

    static class NodeSerializer implements Serializer<Node>{

        final GroupSerializer keySerializer;
        final GroupSerializer valueSerializer;

        NodeSerializer(GroupSerializer keySerializer, GroupSerializer valueSerializer) {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }

        @Override
        public void serialize(@NotNull DataOutput2 out, @NotNull Node value) throws IOException {

            if(CC.ASSERT && value.flags>>>4!=0)
                throw new AssertionError();
            int keysLen = keySerializer.valueArraySize(value.keys)<<4;
            keysLen += value.flags;
            keysLen = DBUtil.parity1Set(keysLen<<1);

            //keysLen and flags are combined into single packed long, that saves a byte for small nodes
            out.packInt(keysLen);
            if(!value.isRightEdge())
                out.packLong(value.link);
            keySerializer.valueArraySerialize(out, value.keys);
            if(value.isDir()) {
                long[] child = (long[]) value.values;
                out.packLongArray(child, 0, child.length );
            }else
                valueSerializer.valueArraySerialize(out, value.values);
        }

        @Override
        public Node deserialize(@NotNull DataInput2 input, int available) throws IOException {
            int keysLen = DBUtil.parity1Get(input.unpackInt())>>>1;
            int flags = keysLen & 0xF;
            keysLen = keysLen>>>4;
            long link =  (flags&RIGHT)!=0
                    ? 0L :
                    input.unpackLong();

            Object keys = keySerializer.valueArrayDeserialize(input, keysLen);
            if(CC.ASSERT && keysLen!=keySerializer.valueArraySize(keys))
                throw new AssertionError();

            Object values;
            if((flags&DIR)!=0){
                keysLen = keysLen - 1 + (flags>>2&1) +(flags>>1&1);
                long[] c = new long[keysLen];
                values = c;
                input.unpackLongArray(c, 0, keysLen);
            }else{
                values = valueSerializer.valueArrayDeserialize(input,
                        keysLen - 2 + ((flags >>> 2) & 1) + ((flags >>> 1) & 1) + (flags & 1));
            }


            return new Node(flags, link, keys, values, keySerializer, valueSerializer);
        }

        @Override
        public boolean isTrusted() {
            return keySerializer.isTrusted() && valueSerializer.isTrusted();
        }
    }

    public static final Comparator COMPARABLE_COMPARATOR = new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
            return o1.compareTo(o2);
        }
    };



    static long findChild(GroupSerializer keySerializer, Node node, Comparator comparator, Object key){
        if(CC.ASSERT && !node.isDir())
            throw new AssertionError();
        //find an index
        int pos = keySerializer.valueArraySearch(node.keys, key, comparator);

        if(pos<0)
            pos = -pos-1;

        pos += -1+node.intLeftEdge();

        pos = Math.max(0, pos);
        long[] children = (long[]) node.values;
        if(pos>=children.length) {
            if(CC.ASSERT && node.isRightEdge())
                throw new AssertionError();
            return node.link;
        }
        return children[pos];
    }



    static final Object LINK = new Object(){
        @Override
        public String toString() {
            return "BTreeMap.LINK";
        }
    };

    static Object leafGet(Node node, Comparator comparator, Object key, GroupSerializer keySerializer, GroupSerializer valueSerializer){
        int pos = keySerializer.valueArraySearch(node.keys, key, comparator);
        return leafGet(node, pos, keySerializer, valueSerializer);
    }

    static Object leafGet(Node node, int pos, GroupSerializer keySerializer, GroupSerializer valueSerializer){

        if(pos<0+1-node.intLeftEdge()) {
            if(!node.isRightEdge() && pos<-keySerializer.valueArraySize(node.keys))
                return LINK;
            else
                return null;
        }
        int valsLen = valueSerializer.valueArraySize(node.values);
        if(!node.isRightEdge() && pos==valsLen+1)
            return null;
        else if(pos>=valsLen+1){
            return LINK;
        }
        pos = pos-1+node.intLeftEdge();
        if(pos>=valsLen)
            return null;
        return valueSerializer.valueArrayGet(node.values, pos);
    }



    /* expand array size by 1, and put value at given position. No items from original array are lost*/
    protected static Object[] arrayPut(final Object[] array, final int pos, final Object value){
        final Object[] ret = Arrays.copyOf(array, array.length+1);
        if(pos<array.length){
            System.arraycopy(array, pos, ret, pos+1, array.length-pos);
        }
        ret[pos] = value;
        return ret;
    }

    /* expand array size by 1, and put value at given position. No items from original array are lost*/
    protected static long[] arrayPut(final long[] array, final int pos, final long value){
        final long[] ret = Arrays.copyOf(array, array.length+1);
        if(pos<array.length){
            System.arraycopy(array, pos, ret, pos+1, array.length-pos);
        }
        ret[pos] = value;
        return ret;
    }

    public static class BinaryGet<K, V> implements StoreBinaryGetLong {
        final GroupSerializer<K> keySerializer;
        final GroupSerializer<V> valueSerializer;
        final Comparator<K> comparator;
        final K key;

        V value = null;

        public BinaryGet(
                @NotNull GroupSerializer<K> keySerializer,
                @NotNull GroupSerializer<V> valueSerializer,
                @NotNull Comparator<K> comparator,
                @NotNull K key
                ) {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            this.comparator = comparator;
            this.key = key;
        }

        @Override
        public long get(DataInput2 input, int size) throws IOException {
            //read size and flags
            int keysLen = DBUtil.parity1Get(input.unpackInt())>>>1;
            int flags = keysLen&0xF;
            keysLen = keysLen>>>4;

            long link =  (flags&RIGHT)!=0
                    ? 0L :
                    input.unpackLong();

            int intLeft = ((flags >>> 2) & 1);
            int intRight = ((flags >>> 1) & 1);

            int pos = keySerializer.valueArrayBinarySearch(key, input, keysLen, comparator);
            if((flags&DIR)!=0){
                //is directory, return related children

                if(pos<0)
                    pos = -pos-1;

                pos += -1 + intLeft;   // plus left edge
                pos = Math.max(0, pos);
                keysLen = keysLen - 1 + intLeft + intRight;

                if(pos>=keysLen) {
                    if(CC.ASSERT && intRight==1)
                        throw new AssertionError();
                    return link;
                }
                if(pos>0)
                    input.unpackLongSkip(pos-1);
                return input.unpackLong();
            }

            //is leaf, get value from leaf

            if(pos<0+1-intLeft) {
                if(intRight==0 && pos<-keysLen)
                    return link;
                else
                    return -1;
            }

            int valsLen = keysLen - 2 + intLeft + intRight + (flags & 1);

            if(intRight==0 /*is not right edge*/ && pos==valsLen+1) {
                //return null
                return -1;
            }else if(pos>=valsLen+1){
                return link;
            }

            pos = pos-1+((flags >>> 2) & 1);
            if(pos>=valsLen) {
                //return null
                return -1;
            }

            //found value, return it
            value = valueSerializer.valueArrayBinaryGet(input, valsLen, pos);
            return -1L;
        }
    }



    static <E> List<E> toList(Collection<E> c) {
        // Using size() here would be a pessimization.
        List<E> list = new ArrayList<E>();
        for (E e : c){
            list.add(e);
        }
        return list;
    }

    public static final class KeySet<E>
            extends AbstractSet<E>
            implements NavigableSet<E>,
            Closeable, Serializable {

        protected final ConcurrentNavigableMap2<E,Object> m;
        private final boolean hasValues;
        KeySet(ConcurrentNavigableMap2<E,Object> map, boolean hasValues) {
            m = map;
            this.hasValues = hasValues;
        }
        @Override
        public int size() { return m.size(); }

        public long sizeLong(){
            if (m instanceof BTreeMap)
                return ((BTreeMap<Object,E>)m).sizeLong();
            else
                return ((SubMap<Object,E>)m).sizeLong();
        }

        @Override
        public boolean isEmpty() { return m.isEmpty(); }
        @Override
        public boolean contains(Object o) { return m.containsKey(o); }
        @Override
        public boolean remove(Object o) { return m.remove(o) != null; }
        @Override
        public void clear() { m.clear(); }
        @Override
        public E lower(E e) { return m.lowerKey(e); }
        @Override
        public E floor(E e) { return m.floorKey(e); }
        @Override
        public E ceiling(E e) { return m.ceilingKey(e); }
        @Override
        public E higher(E e) { return m.higherKey(e); }
        @Override
        public Comparator<? super E> comparator() { return m.comparator(); }
        @Override
        public E first() { return m.firstKey(); }
        @Override
        public E last() { return m.lastKey(); }

        @Override
        public E pollFirst() {
            while(true){
                E e = m.firstKey2();
                if(e==null || m.remove(e)!=null){
                    return e;
                }
            }
        }

        @Override
        public E pollLast() {
            while(true){
                E e = m.lastKey2();
                if(e==null || m.remove(e)!=null){
                    return e;
                }
            }
        }

        @Override
        public Iterator<E> iterator() {
            if (m instanceof ConcurrentNavigableMapExtra)
                return ((ConcurrentNavigableMapExtra<E,Object>)m).keyIterator();
            else if(m instanceof SubMap)
                return ((BTreeMapJava.SubMap<E,Object>)m).keyIterator();
            else
                return ((BTreeMapJava.DescendingMap<E,Object>)m).keyIterator();
        }
        @Override
        public boolean equals(Object o) {
            if (o == this)
                return true;
            if (!(o instanceof Set))
                return false;
            Collection<?> c = (Collection<?>) o;
            try {
                return containsAll(c) && c.containsAll(this);
            } catch (ClassCastException unused)   {
                return false;
            } catch (NullPointerException unused) {
                return false;
            }
        }
        @Override
        public Object[] toArray()     { return toList(this).toArray();  }
        @Override
        public <T> T[] toArray(T[] a) { return toList(this).toArray(a); }
        @Override
        public Iterator<E> descendingIterator() {
            return descendingSet().iterator();
        }
        @Override
        public NavigableSet<E> subSet(E fromElement,
                                      boolean fromInclusive,
                                      E toElement,
                                      boolean toInclusive) {
            return new KeySet<E>((ConcurrentNavigableMap2)m.subMap(fromElement, fromInclusive,
                    toElement,   toInclusive),hasValues);
        }
        @Override
        public NavigableSet<E> headSet(E toElement, boolean inclusive) {
            return new KeySet<E>((ConcurrentNavigableMap2)m.headMap(toElement, inclusive),hasValues);
        }
        @Override
        public NavigableSet<E> tailSet(E fromElement, boolean inclusive) {
            return new KeySet<E>((ConcurrentNavigableMap2)m.tailMap(fromElement, inclusive),hasValues);
        }
        @Override
        public NavigableSet<E> subSet(E fromElement, E toElement) {
            return subSet(fromElement, true, toElement, false);
        }
        @Override
        public NavigableSet<E> headSet(E toElement) {
            return headSet(toElement, false);
        }
        @Override
        public NavigableSet<E> tailSet(E fromElement) {
            return tailSet(fromElement, true);
        }
        @Override
        public NavigableSet<E> descendingSet() {
            return new KeySet((ConcurrentNavigableMap2)m.descendingMap(),hasValues);
        }

        @Override
        public boolean add(E k) {
            if(hasValues)
                throw new UnsupportedOperationException();
            else
                return m.put(k, Boolean.TRUE ) == null;
        }

        @Override
        public void close() {
            if(m instanceof BTreeMap)
                ((BTreeMap)m).close();
        }

        Object writeReplace() throws ObjectStreamException {
            Set ret = new ConcurrentSkipListSet();
            for(Object e:this){
                ret.add(e);
            }
            return ret;
        }
    }

    static final class EntrySet<K1,V1> extends AbstractSet<Map.Entry<K1,V1>> {
        private final ConcurrentNavigableMap<K1, V1> m;
        private final Serializer valueSerializer;
        EntrySet(ConcurrentNavigableMap<K1, V1> map, Serializer valueSerializer) {
            m = map;
            this.valueSerializer = valueSerializer;
        }

        @Override
        public Iterator<Map.Entry<K1,V1>> iterator() {
            if (m instanceof BTreeMap)
                return ((BTreeMap<K1,V1>)m).entryIterator();
            else if(m instanceof  SubMap)
                return ((SubMap<K1,V1>)m).entryIterator();
            else
                return ((DescendingMap<K1,V1>)m).entryIterator();
        }

        @Override
        public boolean contains(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<K1,V1> e = (Map.Entry<K1,V1>)o;
            K1 key = e.getKey();
            if(key == null) return false;
            V1 v = m.get(key);
            //$DELAY$
            return v != null && valueSerializer.equals(v,e.getValue());
        }
        @Override
        public boolean remove(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<K1,V1> e = (Map.Entry<K1,V1>)o;
            K1 key = e.getKey();
            if(key == null) return false;
            return m.remove(key,
                    e.getValue());
        }
        @Override
        public boolean isEmpty() {
            return m.isEmpty();
        }
        @Override
        public int size() {
            return m.size();
        }
        @Override
        public void clear() {
            m.clear();
        }
        @Override
        public boolean equals(Object o) {
            if (o == this)
                return true;
            if (!(o instanceof Set))
                return false;
            Collection<?> c = (Collection<?>) o;
            try {
                return containsAll(c) && c.containsAll(this);
            } catch (ClassCastException unused)   {
                return false;
            } catch (NullPointerException unused) {
                return false;
            }
        }
        @Override
        public Object[] toArray()     { return toList(this).toArray();  }
        @Override
        public <T> T[] toArray(T[] a) { return toList(this).toArray(a); }
    }



    static protected  class SubMap<K,V> extends AbstractMap<K,V> implements  ConcurrentNavigableMap2<K,V> {

        protected final ConcurrentNavigableMapExtra<K,V> m;

        protected final K lo;
        protected final boolean loInclusive;

        protected final K hi;
        protected final boolean hiInclusive;

        public SubMap(ConcurrentNavigableMapExtra<K,V> m, K lo, boolean loInclusive, K hi, boolean hiInclusive) {
            this.m = m;
            this.lo = lo;
            this.loInclusive = loInclusive;
            this.hi = hi;
            this.hiInclusive = hiInclusive;
            if(lo!=null && hi!=null && m.comparator().compare(lo, hi)>0){
                throw new IllegalArgumentException();
            }


        }


/* ----------------  Map API methods -------------- */

        @Override
        public boolean containsKey(Object key) {
            if (key == null) throw new NullPointerException();
            K k = (K)key;
            return inBounds(k) && m.containsKey(k);
        }

        @Override
        public V get(Object key) {
            if (key == null) throw new NullPointerException();
            K k = (K)key;
            return ((!inBounds(k)) ? null : m.get(k));
        }

        @Override
        public V put(K key, V value) {
            checkKeyBounds(key);
            return m.put(key, value);
        }

        @Override
        public V remove(Object key) {
            if(key==null)
                throw new NullPointerException("key null");
            K k = (K)key;
            return (!inBounds(k))? null : m.remove(k);
        }

        @Override
        public int size() {
            return (int) Math.min(sizeLong(), Integer.MAX_VALUE);
        }

        public long sizeLong() {
            //PERF use counted btrees once they become available
            if(hi==null && lo==null)
                return m.sizeLong();

            Iterator<K> i = keyIterator();
            long counter = 0;
            while(i.hasNext()){
                counter++;
                i.next();
            }
            return counter;
        }


        @Override
        public boolean isEmpty() {
            return !keyIterator().hasNext();
        }

        @Override
        public boolean containsValue(Object value) {
            if(value==null) throw new NullPointerException();
            Iterator<V> i = valueIterator();
            while(i.hasNext()){
                if(m.getValueSerializer().equals((V)value,i.next()))
                    return true;
            }
            return false;
        }

        @Override
        public void clear() {
            Iterator<K> i = keyIterator();
            while(i.hasNext()){
                i.next();
                i.remove();
            }
        }


        /* ----------------  ConcurrentMap API methods -------------- */

        @Override
        public V putIfAbsent(K key, V value) {
            checkKeyBounds(key);
            return m.putIfAbsent(key, value);
        }

        @Override
        public boolean remove(Object key, Object value) {
            K k = (K)key;
            return inBounds(k) && m.remove(k, value);
        }

        @Override
        public boolean replace(K key, V oldValue, V newValue) {
            checkKeyBounds(key);
            return m.replace(key, oldValue, newValue);
        }

        @Override
        public V replace(K key, V value) {
            checkKeyBounds(key);
            return m.replace(key, value);
        }

        /* ----------------  SortedMap API methods -------------- */

        @Override
        public Comparator<? super K> comparator() {
            return m.comparator();
        }

        /* ----------------  Relational methods -------------- */

        @Override
        public Map.Entry<K,V> lowerEntry(K key) {
            if(key==null)throw new NullPointerException();
            if(tooLow(key))return null;

            if(tooHigh(key))
                return lastEntry();

            Entry<K,V> r = m.lowerEntry(key);
            return r!=null && !tooLow(r.getKey()) ? r :null;
        }

        @Override
        public K lowerKey(K key) {
            if(key==null)throw new NullPointerException();
            if(tooLow(key))return null;

            if(tooHigh(key))
                return lastKey2();

            K r = m.lowerKey(key);
            return r!=null && !tooLow(r) ? r :null;
        }

        @Override
        public Map.Entry<K,V> floorEntry(K key) {
            if(key==null) throw new NullPointerException();
            if(tooLow(key)) return null;

            if(tooHigh(key)){
                return lastEntry();
            }

            Entry<K,V> ret = m.floorEntry(key);
            if(ret!=null && tooLow(ret.getKey())) return null;
            return ret;
        }

        @Override
        public K floorKey(K key) {
            if(key==null) throw new NullPointerException();
            if(tooLow(key)) return null;

            if(tooHigh(key)){
                return lastKey2();
            }

            K ret = m.floorKey(key);
            if(ret!=null && tooLow(ret)) return null;
            return ret;        }

        @Override
        public Map.Entry<K,V> ceilingEntry(K key) {
            if(key==null) throw new NullPointerException();
            if(tooHigh(key)) return null;

            if(tooLow(key)){
                return firstEntry();
            }

            Entry<K,V> ret = m.ceilingEntry(key);
            if(ret!=null && tooHigh(ret.getKey())) return null;
            return ret;
        }

        @Override
        public K ceilingKey(K key) {
            if(key==null) throw new NullPointerException();
            if(tooHigh(key)) return null;

            if(tooLow(key)){
                return firstKey2();
            }

            K ret = m.ceilingKey(key);
            if(ret!=null && tooHigh(ret)) return null;
            return ret;
        }

        @Override
        public Entry<K, V> higherEntry(K key) {
            Entry<K,V> r = m.higherEntry(key);
            return r!=null && inBounds(r.getKey()) ? r : null;
        }

        @Override
        public K higherKey(K key) {
            K r = m.higherKey(key);
            return r!=null && inBounds(r) ? r : null;
        }



        public K firstKey2() {
            K k =
                    lo==null ?
                            m.firstKey2():
                            m.findHigherKey(lo, loInclusive);
            return k!=null && inBounds(k)? k : null;
        }

        public K lastKey2() {
            K k =
                    hi==null ?
                            m.lastKey2():
                            m.findLowerKey(hi, hiInclusive);

            return k!=null && inBounds(k)? k : null;
        }

        @Override
        public K firstKey() {
            K ret = firstKey2();
            if(ret==null)
                throw new NoSuchElementException();
            return ret;
        }

        @Override
        public K lastKey() {
            K ret = lastKey2();
            if(ret==null)
                throw new NoSuchElementException();
            return ret;        }

        @Override
        public Map.Entry<K,V> firstEntry() {
            Entry<K,V> k =
                    lo==null ?
                            m.firstEntry():
                            m.findHigher(lo, loInclusive);
            return k!=null && inBounds(k.getKey())? k : null;
        }

        @Override
        public Map.Entry<K,V> lastEntry() {
            Entry<K,V> k =
                    hi==null ?
                            m.lastEntry():
                            m.findLower(hi, hiInclusive);

            return k!=null && inBounds(k.getKey())? k : null;
        }

        @Override
        public Entry<K, V> pollFirstEntry() {
            while(true){
                Entry<K, V> e = firstEntry();
                if(e==null || remove(e.getKey(),e.getValue())){
                    return e;
                }
            }
        }

        @Override
        public Entry<K, V> pollLastEntry() {
            while(true){
                Entry<K, V> e = lastEntry();
                if(e==null || remove(e.getKey(),e.getValue())){
                    return e;
                }
            }
        }




        /**
         * Utility to create submaps, where given bounds override
         * unbounded(null) ones and/or are checked against bounded ones.
         */
        private SubMap<K,V> newSubMap(K fromKey,
                                      boolean fromInclusive,
                                      K toKey,
                                      boolean toInclusive) {

//            if(fromKey!=null && toKey!=null){
//                int comp = m.comparator.compare(fromKey, toKey);
//                if((fromInclusive||!toInclusive) && comp==0)
//                    throw new IllegalArgumentException();
//            }

            if (lo != null) {
                if (fromKey == null) {
                    fromKey = lo;
                    fromInclusive = loInclusive;
                }
                else {
                    int c = m.comparator().compare(fromKey, lo);
                    if (c < 0 || (c == 0 && !loInclusive && fromInclusive))
                        throw new IllegalArgumentException("key out of range");
                }
            }
            if (hi != null) {
                if (toKey == null) {
                    toKey = hi;
                    toInclusive = hiInclusive;
                }
                else {
                    int c = m.comparator().compare(toKey, hi);
                    if (c > 0 || (c == 0 && !hiInclusive && toInclusive))
                        throw new IllegalArgumentException("key out of range");
                }
            }
            return new SubMap<K,V>(m, fromKey, fromInclusive,
                    toKey, toInclusive);
        }

        @Override
        public SubMap<K,V> subMap(K fromKey,
                                  boolean fromInclusive,
                                  K toKey,
                                  boolean toInclusive) {
            if (fromKey == null || toKey == null)
                throw new NullPointerException();
            return newSubMap(fromKey, fromInclusive, toKey, toInclusive);
        }

        @Override
        public SubMap<K,V> headMap(K toKey,
                                   boolean inclusive) {
            if (toKey == null)
                throw new NullPointerException();
            return newSubMap(null, false, toKey, inclusive);
        }

        @Override
        public SubMap<K,V> tailMap(K fromKey,
                                   boolean inclusive) {
            if (fromKey == null)
                throw new NullPointerException();
            return newSubMap(fromKey, inclusive, null, false);
        }

        @Override
        public SubMap<K,V> subMap(K fromKey, K toKey) {
            return subMap(fromKey, true, toKey, false);
        }

        @Override
        public SubMap<K,V> headMap(K toKey) {
            return headMap(toKey, false);
        }

        @Override
        public SubMap<K,V> tailMap(K fromKey) {
            return tailMap(fromKey, true);
        }

        @Override
        public ConcurrentNavigableMap<K,V> descendingMap() {
            return new DescendingMap(m, lo,loInclusive, hi,hiInclusive);
        }

        @Override
        public NavigableSet<K> navigableKeySet() {
            return new KeySet<K>((ConcurrentNavigableMap2<K,Object>) this,m.getHasValues());
        }


        /* ----------------  Utilities -------------- */



        private boolean tooLow(K key) {
            if (lo != null) {
                int c = m.comparator().compare(key, lo);
                if (c < 0 || (c == 0 && !loInclusive))
                    return true;
            }
            return false;
        }

        private boolean tooHigh(K key) {
            if (hi != null) {
                int c = m.comparator().compare(key, hi);
                if (c > 0 || (c == 0 && !hiInclusive))
                    return true;
            }
            return false;
        }

        private boolean inBounds(K key) {
            return !tooLow(key) && !tooHigh(key);
        }

        private void checkKeyBounds(K key) throws IllegalArgumentException {
            if (key == null)
                throw new NullPointerException();
            if (!inBounds(key))
                throw new IllegalArgumentException("key out of range");
        }





        @Override
        public NavigableSet<K> keySet() {
            return new KeySet<K>((ConcurrentNavigableMap2<K,Object>) this, m.getHasValues());
        }

        @Override
        public NavigableSet<K> descendingKeySet() {
            return new DescendingMap<K,V>(m,lo,loInclusive, hi, hiInclusive).keySet();
        }



        @Override
        public Set<Entry<K, V>> entrySet() {
            return new EntrySet<K, V>(this,m.getValueSerializer());
        }



        Iterator<K> keyIterator() {
            return m.keyIterator(lo,loInclusive,hi,hiInclusive);
        }

        Iterator<V> valueIterator() {
            return m.valueIterator(lo,loInclusive,hi,hiInclusive);
        }

        Iterator<Map.Entry<K,V>> entryIterator() {
            return m.entryIterator(lo,loInclusive,hi,hiInclusive);
        }

    }

    interface ConcurrentNavigableMap2<K,V> extends ConcurrentNavigableMap<K,V>{
        K firstKey2();
        K lastKey2();
    }

    static protected  class DescendingMap<K,V> extends AbstractMap<K,V> implements  ConcurrentNavigableMap2<K,V> {

        protected final ConcurrentNavigableMapExtra<K,V> m;

        protected final K lo;
        protected final boolean loInclusive;

        protected final K hi;
        protected final boolean hiInclusive;

        public DescendingMap(ConcurrentNavigableMapExtra<K,V> m, K lo, boolean loInclusive, K hi, boolean hiInclusive) {
            this.m = m;
            this.lo = lo;
            this.loInclusive = loInclusive;
            this.hi = hi;
            this.hiInclusive = hiInclusive;
            if(lo!=null && hi!=null && m.comparator().compare(lo, hi)>0){
                throw new IllegalArgumentException();
            }


        }


/* ----------------  Map API methods -------------- */

        @Override
        public boolean containsKey(Object key) {
            if (key == null) throw new NullPointerException();
            K k = (K)key;
            return inBounds(k) && m.containsKey(k);
        }

        @Override
        public V get(Object key) {
            if (key == null) throw new NullPointerException();
            K k = (K)key;
            return ((!inBounds(k)) ? null : m.get(k));
        }

        @Override
        public V put(K key, V value) {
            checkKeyBounds(key);
            return m.put(key, value);
        }

        @Override
        public V remove(Object key) {
            K k = (K)key;
            return (!inBounds(k))? null : m.remove(k);
        }

        @Override
        public int size() {
            if(hi==null && lo==null)
                return m.size();

            //TODO PERF use ascending iterator for faster counting
            Iterator<K> i = keyIterator();
            long counter = 0;
            while(i.hasNext()){
                counter++;
                i.next();
            }
            return (int) Math.min(counter, Integer.MAX_VALUE);
        }

        @Override
        public boolean isEmpty() {
            return !keyIterator().hasNext();
        }

        @Override
        public boolean containsValue(Object value) {
            if(value==null) throw new NullPointerException();
            Iterator<V> i = valueIterator();
            while(i.hasNext()){
                if(m.getValueSerializer().equals((V) value,i.next()))
                    return true;
            }
            return false;
        }

        @Override
        public void clear() {
            Iterator<K> i = keyIterator();
            while(i.hasNext()){
                i.next();
                i.remove();
            }
        }


        /* ----------------  ConcurrentMap API methods -------------- */

        @Override
        public V putIfAbsent(K key, V value) {
            checkKeyBounds(key);
            return m.putIfAbsent(key, value);
        }

        @Override
        public boolean remove(Object key, Object value) {
            K k = (K)key;
            return inBounds(k) && m.remove(k, value);
        }

        @Override
        public boolean replace(K key, V oldValue, V newValue) {
            checkKeyBounds(key);
            return m.replace(key, oldValue, newValue);
        }

        @Override
        public V replace(K key, V value) {
            checkKeyBounds(key);
            return m.replace(key, value);
        }

        /* ----------------  SortedMap API methods -------------- */

        @Override
        public Comparator<? super K> comparator() {
            return m.comparator();
        }

        /* ----------------  Relational methods -------------- */

        @Override
        public Map.Entry<K,V> higherEntry(K key) {
            if(key==null)throw new NullPointerException();
            if(tooLow(key))return null;

            if(tooHigh(key))
                return firstEntry();

            Entry<K,V> r = m.lowerEntry(key);
            return r!=null && !tooLow(r.getKey()) ? r :null;
        }

        @Override
        public K lowerKey(K key) {
            Entry<K,V> n = lowerEntry(key);
            return (n == null)? null : n.getKey();
        }

        @Override
        public Map.Entry<K,V> ceilingEntry(K key) {
            if(key==null) throw new NullPointerException();
            if(tooLow(key)) return null;

            if(tooHigh(key)){
                return firstEntry();
            }

            Entry<K,V> ret = m.floorEntry(key);
            if(ret!=null && tooLow(ret.getKey())) return null;
            return ret;

        }

        @Override
        public K floorKey(K key) {
            Entry<K,V> n = floorEntry(key);
            return (n == null)? null : n.getKey();
        }

        @Override
        public Map.Entry<K,V> floorEntry(K key) {
            if(key==null) throw new NullPointerException();
            if(tooHigh(key)) return null;

            if(tooLow(key)){
                return lastEntry();
            }

            Entry<K,V> ret = m.ceilingEntry(key);
            if(ret!=null && tooHigh(ret.getKey())) return null;
            return ret;
        }

        @Override
        public K ceilingKey(K key) {
            Entry<K,V> k = ceilingEntry(key);
            return k!=null? k.getKey():null;
        }

        @Override
        public Entry<K, V> lowerEntry(K key) {
            Entry<K,V> r = m.higherEntry(key);
            return r!=null && inBounds(r.getKey()) ? r : null;
        }

        @Override
        public K higherKey(K key) {
            Entry<K,V> k = higherEntry(key);
            return k!=null? k.getKey():null;
        }

        @Override
        public K firstKey2() {
            Entry<K,V> e = firstEntry();
            if(e==null)
                return null;
            return e.getKey();
        }

        @Override
        public K lastKey2() {
            Entry<K,V> e = lastEntry();
            if(e==null)
                return null;
            return e.getKey();
        }


        @Override
        public K firstKey() {
            K key = firstKey2();
            if(key==null) throw new NoSuchElementException();
            return key;
        }

        @Override
        public K lastKey() {
            K key = lastKey2();
            if(key==null) throw new NoSuchElementException();
            return key;
        }


        @Override
        public Map.Entry<K,V> lastEntry() {
            Entry<K,V> k =
                    lo==null ?
                            m.firstEntry():
                            m.findHigher(lo, loInclusive);
            return k!=null && inBounds(k.getKey())? k : null;

        }

        @Override
        public Map.Entry<K,V> firstEntry() {
            Entry<K,V> k =
                    hi==null ?
                            m.lastEntry():
                            m.findLower(hi, hiInclusive);

            return k!=null && inBounds(k.getKey())? k : null;
        }

        @Override
        public Entry<K, V> pollFirstEntry() {
            while(true){
                Entry<K, V> e = firstEntry();
                if(e==null || remove(e.getKey(),e.getValue())){
                    return e;
                }
            }
        }

        @Override
        public Entry<K, V> pollLastEntry() {
            while(true){
                Entry<K, V> e = lastEntry();
                if(e==null || remove(e.getKey(),e.getValue())){
                    return e;
                }
            }
        }




        /**
         * Utility to create submaps, where given bounds override
         * unbounded(null) ones and/or are checked against bounded ones.
         */
        private DescendingMap<K,V> newSubMap(
                K toKey,
                boolean toInclusive,
                K fromKey,
                boolean fromInclusive) {

//            if(fromKey!=null && toKey!=null){
//                int comp = m.comparator.compare(fromKey, toKey);
//                if((fromInclusive||!toInclusive) && comp==0)
//                    throw new IllegalArgumentException();
//            }

            if (lo != null) {
                if (fromKey == null) {
                    fromKey = lo;
                    fromInclusive = loInclusive;
                }
                else {
                    int c = m.comparator().compare(fromKey, lo);
                    if (c < 0 || (c == 0 && !loInclusive && fromInclusive))
                        throw new IllegalArgumentException("key out of range");
                }
            }
            if (hi != null) {
                if (toKey == null) {
                    toKey = hi;
                    toInclusive = hiInclusive;
                }
                else {
                    int c = m.comparator().compare(toKey, hi);
                    if (c > 0 || (c == 0 && !hiInclusive && toInclusive))
                        throw new IllegalArgumentException("key out of range");
                }
            }
            return new DescendingMap<K,V>(m, fromKey, fromInclusive,
                    toKey, toInclusive);
        }

        @Override
        public DescendingMap<K,V> subMap(K fromKey,
                                         boolean fromInclusive,
                                         K toKey,
                                         boolean toInclusive) {
            if (fromKey == null || toKey == null)
                throw new NullPointerException();
            return newSubMap(fromKey, fromInclusive, toKey, toInclusive);
        }

        @Override
        public DescendingMap<K,V> headMap(K toKey,
                                          boolean inclusive) {
            if (toKey == null)
                throw new NullPointerException();
            return newSubMap(null, false, toKey, inclusive);
        }

        @Override
        public DescendingMap<K,V> tailMap(K fromKey,
                                          boolean inclusive) {
            if (fromKey == null)
                throw new NullPointerException();
            return newSubMap(fromKey, inclusive, null, false);
        }

        @Override
        public DescendingMap<K,V> subMap(K fromKey, K toKey) {
            return subMap(fromKey, true, toKey, false);
        }

        @Override
        public DescendingMap<K,V> headMap(K toKey) {
            return headMap(toKey, false);
        }

        @Override
        public DescendingMap<K,V> tailMap(K fromKey) {
            return tailMap(fromKey, true);
        }

        @Override
        public ConcurrentNavigableMap<K,V> descendingMap() {
            if(lo==null && hi==null) return m;
            return m.subMap(lo,loInclusive,hi,hiInclusive);
        }

        @Override
        public NavigableSet<K> navigableKeySet() {
            return new KeySet<K>((ConcurrentNavigableMap2<K,Object>) this,m.getHasValues());
        }


        /* ----------------  Utilities -------------- */



        private boolean tooLow(K key) {
            if (lo != null) {
                int c = m.comparator().compare(key, lo);
                if (c < 0 || (c == 0 && !loInclusive))
                    return true;
            }
            return false;
        }

        private boolean tooHigh(K key) {
            if (hi != null) {
                int c = m.comparator().compare(key, hi);
                if (c > 0 || (c == 0 && !hiInclusive))
                    return true;
            }
            return false;
        }

        private boolean inBounds(K key) {
            return !tooLow(key) && !tooHigh(key);
        }

        private void checkKeyBounds(K key) throws IllegalArgumentException {
            if (key == null)
                throw new NullPointerException();
            if (!inBounds(key))
                throw new IllegalArgumentException("key out of range");
        }





        @Override
        public NavigableSet<K> keySet() {
            return new KeySet<K>((ConcurrentNavigableMap2<K,Object>) this, m.getHasValues());
        }

        @Override
        public NavigableSet<K> descendingKeySet() {
            return new KeySet<K>((ConcurrentNavigableMap2<K,Object>) descendingMap(), m.getHasValues());
        }



        @Override
        public Set<Entry<K, V>> entrySet() {
            return new EntrySet<K, V>(this,m.getValueSerializer());
        }


        /*
         * ITERATORS
         */

        Iterator<K> keyIterator() {
            if(lo==null && hi==null )
                return m.descendingKeyIterator();
            else
                return m.descendingKeyIterator(lo, loInclusive, hi, hiInclusive);
        }

        Iterator<V> valueIterator() {
            if(lo==null && hi==null )
                return m.descendingValueIterator();
            else
                return m.descendingValueIterator(lo, loInclusive, hi, hiInclusive);
        }

        Iterator<Map.Entry<K,V>> entryIterator() {
            if(lo==null && hi==null )
                return m.descendingEntryIterator();
            else
                return m.descendingEntryIterator(lo, loInclusive, hi, hiInclusive);
        }

    }

}

