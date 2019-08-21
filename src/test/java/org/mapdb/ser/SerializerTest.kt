package org.mapdb.ser

import org.junit.Test
import java.io.Serializable
import java.math.BigDecimal
import java.math.BigInteger
import java.util.*
import org.junit.Assert.*
import org.mapdb.*
import org.mapdb.io.DataIO
import org.mapdb.io.DataInput2ByteArray
import org.mapdb.io.DataOutput2
import org.mapdb.io.DataOutput2ByteArray

abstract class SerializerTest<E> {

    val random = Random();

    abstract fun randomValue(): E

    abstract val serializer: Serializer<E>

    val max = 100L + TT.testScale()

    open val repeat = 100

    val arraySize = 10 + TT.testScale() * 10

    fun assertSerEquals(v1: Any?, v2: Any?) {
        assertTrue(serializer.equals(v1 as E, v2 as E))
        assertEquals(serializer.hashCode(v1, 0), serializer.hashCode(v2, 0))
    }


    @Test fun cloneEquals(){
        for(i in 0..max){
            val e = randomValue()
            val e2 = TT.clone(e,serializer)
            assertSerEquals(e, e2)
        }
    }

    @Test(timeout = 1000L)
    fun randomNotEquals(){
        // two random values should not be equal,
        // test will eventually timeout if they are always equal
        while(serializer.equals(randomValue(), randomValue())){

        }
    }

    @Test(timeout = 1000L)
    fun randomNotEqualHashCode(){
        //two random values should not have equal hash code,
        // test will eventually timeout if they are always equal
        while(serializer.hashCode(randomValue(),0) == serializer.hashCode(randomValue(),0)){

        }
    }

    open @Test fun trusted(){
        assertTrue(serializer.isTrusted || serializer== Serializers.JAVA
                /*|| serializer== Serializers.ELSA*/)
    }

    @Test fun fixedSize(){
        val size = serializer.fixedSize();
        if(size<0)
            return;
        for(i in 0..max) {
            val e = randomValue()
            val out = DataOutput2ByteArray()
            serializer.serialize(out, e);
            assertEquals(size,out.pos)
        }
    }

    @Test fun compare() {
        for (i in 0..max) {
            val v1 = randomValue()
            val v2 = randomValue()
            serializer.compare(v1, v2)
        }
    }

}


abstract class GroupSerializerTest<E,G>:SerializerTest<E>(){
    val serializer2:GroupSerializer<E,G>
        get() = serializer as  GroupSerializer<E,G>



    @Test open fun valueArrayBinarySearch(){
        var v = ArrayList<E>()
        for (i in 0..max) {
            v.add(randomValue())
        }
        Collections.sort(v, serializer)
        val keys = serializer2.valueArrayFromArray(v.toArray())

        fun check(keys:G, binary:ByteArray, e:E, diPos:Int){
            val v1 = serializer2.valueArraySearch(keys, e)
            val v2 = serializer2.valueArraySearch(keys, e, serializer)
            val v3 = Arrays.binarySearch(serializer2.valueArrayToArray(keys), e as Any, serializer as Comparator<Any>)

            assertEquals(v1, v3);
            assertEquals(v1, v2);
            val di = DataInput2ByteArray(binary);
            val v4 = serializer2.valueArrayBinarySearch(e, di, v.size, serializer)
            assertEquals(diPos, di.pos)
            assertEquals(v1, v4)
        }

        val out = DataOutput2ByteArray();
        serializer2.valueArraySerialize(out, keys)
        val deserialized = serializer2.valueArrayDeserialize(DataInput2ByteArray(out.buf), v.size);
        val diPos = out.pos
        assertTrue(Arrays.deepEquals(serializer2.valueArrayToArray(keys), serializer2.valueArrayToArray(deserialized)))

        for (i in 0..max*10) {
            val e = randomValue()
            check(keys, out.buf, e, diPos)
        }

        for(e in v){
            check(keys, out.buf, e, diPos)
        }
    }

    @Test open fun valueArrayGet(){
        var v = randomArray()
        val keys = serializer2.valueArrayFromArray(v)
        val out = DataOutput2ByteArray()
        serializer2.valueArraySerialize(out, keys)

        for(i in 0 until max.toInt()){
            val v1 = v[i] as E
            val v2 = serializer2.valueArrayGet(keys, i)
            val v3 = serializer2.valueArrayBinaryGet(DataInput2ByteArray(out.buf), max.toInt(), i)

            assertTrue(serializer.equals(v1, v2))
            assertTrue(serializer.equals(v1, v3))
        }

    }

    open protected fun randomArray() = Array<Any>(max.toInt(), { i -> randomValue() as Any })

    open protected fun randomValueArray() = serializer2.valueArrayFromArray(Array<Any>(arraySize.toInt(), { i -> randomValue() as Any }))

    fun cloneValueArray(vals:G):G{
        val out = DataOutput2ByteArray();
        out.pos = 0
        val size = serializer2.valueArraySize(vals)
        serializer2.valueArraySerialize(out, vals);
        val input = DataInput2ByteArray(out.buf)
        val ret = serializer2.valueArrayDeserialize(input,size)

        assertEquals(out.pos, input.pos)

        return ret;
    }

    fun assertValueArrayEquals(vals1:G, vals2:G){
        val size = serializer2.valueArraySize(vals1)
        assertEquals(size, serializer2.valueArraySize(vals2))

        for(i in 0 until size){
            val v1 = serializer2.valueArrayGet(vals1, i)
            val v2 = serializer2.valueArrayGet(vals2, i)

            assertSerEquals(v1, v2)
        }
    }


    @Test open fun valueArraySerDeser(){
// TODO size hint
//        if(serializer.needsAvailableSizeHint())
//            return
        for(i in 0..max){
            val e = randomValueArray()
            val e2 = cloneValueArray(e)
            assertValueArrayEquals(e,e2)
        }
    }

    @Test open fun valueArrayDeleteValue(){
        for(i in 0..max){
            val vals = randomValueArray()
            val valsSize = serializer2.valueArraySize(vals);
            if(valsSize==0)
                continue;
            val pos = 1+random.nextInt(valsSize-1);

            val vals2 = serializer2.valueArrayDeleteValue(vals, pos);
            assertEquals(valsSize-1, serializer2.valueArraySize(vals2))

            val arr1 = DataIO.arrayDelete(serializer2.valueArrayToArray(vals), pos, 1);
            val arr2 = serializer2.valueArrayToArray(vals2);

            arr1.forEachIndexed { i, any ->
                assertSerEquals(any, arr2[i])
            }
        }

    }

    @Test open fun valueArrayCopyOfRange(){
        for(i in 0..max){
            val vals = randomValueArray()
            val valsSize = serializer2.valueArraySize(vals);
            if(valsSize<5)
                continue;
            val pos = 1+random.nextInt(valsSize-4);
            val vals2 = serializer2.valueArrayCopyOfRange(vals,pos,pos+3);

            val arr1a = serializer2.valueArrayToArray(vals);
            val arr1 = Arrays.copyOfRange(arr1a, pos, pos+3)

            val arr2 = serializer2.valueArrayToArray(vals2);

            arr1.forEachIndexed { i, any ->
                assertSerEquals(any, arr2[i])
            }
        }

    }
//
//    @Test fun btreemap(){
//        val ser = serializer as GroupSerializer<Any>
//        val map = BTreeMap.make(keySerializer = ser, valueSerializer = Serializers.INTEGER)
//        val set = TreeSet(ser);
//        for(i in 1..100)
//            set.add(randomValue() as Any)
//        set.forEach { map.put(it,1) }
//        val iter1 = set.iterator()
//        val iter2 = map.keys.iterator()
//
//        while(iter1.hasNext()){
//            assertTrue(iter2.hasNext())
//            assertTrue(ser.equals(iter1.next(),iter2.next()))
//        }
//        assertFalse(iter2.hasNext())
//    }

    @Test fun valueArrayUpdate(){
        for(i in 0..max) {
            var vals = randomValueArray()
            val vals2 = serializer2.valueArrayToArray(vals)
            val valsSize = serializer2.valueArraySize(vals);
            for (j in 0 until valsSize) {
                val newVal = randomValue()
                vals2[j] = newVal
                vals = serializer2.valueArrayUpdateVal(vals, j, newVal)
                vals2.forEachIndexed { i, any ->
                    assertSerEquals(any, serializer2.valueArrayGet(vals,i))
                }
            }
        }
    }
}

class Serializer_CHAR: GroupSerializerTest<Char, Any>(){
    override fun randomValue() = random.nextInt().toChar()
    override val serializer = Serializers.CHAR
}
//
//class Serializer_STRINGXXHASH: GroupSerializerTest<String>(){
//    override fun randomValue() = TT.randomString(random.nextInt(10))
//    override val serializer = Serializers.STRING_ORIGHASH
//}

class Serializer_STRING: GroupSerializerTest<String, Any>(){
    override fun randomValue() = TT.randomString(random.nextInt(10))
    override val serializer = Serializers.STRING
}

//class Serializer_STRING_DELTA: GroupSerializerTest<String>(){
//    override fun randomValue() = TT.randomString(random.nextInt(10))
//    override val serializer = Serializers.STRING_DELTA
//}
//class Serializer_STRING_DELTA2: GroupSerializerTest<String>(){
//    override fun randomValue() = TT.randomString(random.nextInt(10))
//    override val serializer = Serializers.STRING_DELTA2
//}
//
//
//class Serializer_STRING_INTERN: GroupSerializerTest<String>(){
//    override fun randomValue() = TT.randomString(random.nextInt(10))
//    override val serializer = Serializers.STRING_INTERN
//}
//
//class Serializer_STRING_ASCII: GroupSerializerTest<String>(){
//    override fun randomValue() = TT.randomString(random.nextInt(10))
//    override val serializer = Serializers.STRING_ASCII
//}
//
//class Serializer_STRING_NOSIZE: SerializerTest<String>(){
//    override fun randomValue() = TT.randomString(random.nextInt(10))
//    override val serializer = Serializers.STRING_NOSIZE
//
//}

class Serializer_LONG: GroupSerializerTest<Long,Any>(){
    override fun randomValue() = random.nextLong()
    override val serializer = Serializers.LONG
}

//class Serializer_LONG_PACKED: GroupSerializerTest<Long>(){
//    override fun randomValue() = random.nextLong()
//    override val serializer = Serializers.LONG_PACKED
//}
//
//class Serializer_LONG_DELTA: GroupSerializerTest<Long>(){
//    override fun randomValue() = random.nextLong()
//    override val serializer = Serializers.LONG_DELTA
//    override fun randomArray(): Array<Any> {
//        val v = super.randomArray()
//        Arrays.sort(v)
//        return v
//    }
//
//    override fun randomValueArray(): Any {
//        val v = super.randomValueArray()
//        Arrays.sort(v as LongArray)
//        return v
//    }
//}



class Serializer_INTEGER: GroupSerializerTest<Int, Any>(){
    override fun randomValue() = random.nextInt()
    override val serializer = Serializers.INTEGER
}

//class Serializer_INTEGER_PACKED: GroupSerializerTest<Int>(){
//    override fun randomValue() = random.nextInt()
//    override val serializer = Serializers.INTEGER_PACKED
//}
//
//class Serializer_INTEGER_DELTA: GroupSerializerTest<Int>(){
//    override fun randomValue() = random.nextInt()
//    override val serializer = Serializers.INTEGER_DELTA
//
//    override fun randomArray(): Array<Any> {
//        val v = super.randomArray()
//        Arrays.sort(v)
//        return v
//    }
//
//    override fun randomValueArray(): Any {
//        val v = super.randomValueArray()
//        Arrays.sort(v as IntArray)
//        return v
//    }
//
//}

//
//class Serializer_LONG_PACKED_ZIGZAG:SerializerTest<Long>(){
//    override fun randomValue() = random.nextLong()
//    override val serializer = Serializers.LONG_PACKED_ZIGZAG
//}
//
//class Serializer_INTEGER_PACKED_ZIGZAG:SerializerTest<Int>(){
//    override fun randomValue() = random.nextInt()
//    override val serializer = Serializers.INTEGER_PACKED_ZIGZAG
//}

class Serializer_BOOLEAN: GroupSerializerTest<Boolean,Any>(){
    override fun randomValue() = random.nextBoolean()
    override val serializer = Serializers.BOOLEAN
}

class Serializer_RECID: GroupSerializerTest<Long,Any>(){
    override fun randomValue() = random.nextLong().and(0xFFFFFFFFFFFFL) //6 bytes
    override val serializer = Serializers.RECID
}

//class Serializer_RECID_ARRAY: GroupSerializerTest<LongArray>(){
//    override fun randomValue():LongArray {
//        val ret = LongArray(random.nextInt(50));
//        for(i in 0 until ret.size){
//            ret[i] = random.nextLong().and(0xFFFFFFFFFFFFL) //6 bytes
//        }
//        return ret
//    }
//
//    override val serializer = Serializers.RECID_ARRAY
//}

class Serializer_BYTE_ARRAY: GroupSerializerTest<ByteArray, Any>(){
    override fun randomValue() = TT.randomByteArray(random.nextInt(50))
    override val serializer = Serializers.BYTE_ARRAY

    @Test fun next_val(){
        fun check(b1:ByteArray?, b2:ByteArray?){
            assertArrayEquals(b1, Serializers.BYTE_ARRAY.nextValue(b2))
        }

        check(byteArrayOf(1,1), byteArrayOf(1,0))
        check(byteArrayOf(2), byteArrayOf(1))
        check(byteArrayOf(2,0), byteArrayOf(1,-1))
        check(null, byteArrayOf(-1,-1))
    }
}

//
//class Serializer_BYTE_ARRAY_DELTA: GroupSerializerTest<ByteArray>(){
//    override fun randomValue() = TT.randomByteArray(random.nextInt(50))
//    override val serializer = Serializers.BYTE_ARRAY_DELTA
//}
//
//class Serializer_BYTE_ARRAY_DELTA2: GroupSerializerTest<ByteArray>(){
//    override fun randomValue() = TT.randomByteArray(random.nextInt(50))
//    override val serializer = Serializers.BYTE_ARRAY_DELTA2
//}
//
//class Serializer_BYTE_ARRAY_NOSIZE: SerializerTest<ByteArray>(){
//    override fun randomValue() = TT.randomByteArray(random.nextInt(50))
//    override val serializer = Serializers.BYTE_ARRAY_NOSIZE
//
//}


class Serializer_BYTE: GroupSerializerTest<Byte, Any>(){
    override fun randomValue() = random.nextInt().toByte()
    override val serializer = Serializers.BYTE
}

class Serializer_CHAR_ARRAY: GroupSerializerTest<CharArray, Any>(){
    override fun randomValue():CharArray {
        val ret = CharArray(random.nextInt(50));
        for(i in 0 until ret.size){
            ret[i] = random.nextInt().toChar()
        }
        return ret
    }
    override val serializer = Serializers.CHAR_ARRAY
//
//    @Test fun prefix_submap(){
//        val map = BTreeMap.make(keySerializer = serializer, valueSerializer = Serializers.STRING)
//        for(i in 'a'..'f') for(j in 'a'..'f') {
//            map.put(charArrayOf(i, j), "$i-$j")
//        }
//
//        //zero subMap
//        assertEquals(0, map.prefixSubMap(charArrayOf('z')).size)
//
//        var i = 'b';
//        val sub = map.prefixSubMap(charArrayOf(i))
//        assertEquals(6, sub.size)
//        for(j in 'a'..'f')
//            assertEquals("$i-$j", sub[charArrayOf(i,j)])
//
//        //out of subMap range
//        assertNull(sub[charArrayOf('b','z')])
//
//        //max case
//        i = java.lang.Character.MAX_VALUE;
//        for(j in 'a'..'f')
//            map.put(charArrayOf(i, j), "$i-$j")
//
//        val subMax = map.prefixSubMap(charArrayOf(i))
//        assertEquals(6, subMax.size)
//        for(j in 'a'..'f')
//            assertEquals("$i-$j", subMax[charArrayOf(i,j.toChar())])
//
//        //out of subMap range
//        assertNull(sub[charArrayOf(i,'z')])
//
//        //max-max case
//        map.put(charArrayOf(i, i), "$i-$i")
//
//        val subMaxMax = map.prefixSubMap(charArrayOf(i, i))
//        assertEquals("$i-$i", subMaxMax[charArrayOf(i,i)])
//
//        //out of subMaxMax range
//        assertNull(subMaxMax[charArrayOf(i,'a')])
//
//        //min case
//        i = java.lang.Character.MIN_VALUE;
//        for(j in 'a'..'f')
//            map.put(charArrayOf(i, j.toChar()), "$i-$j")
//
//        val subMin = map.prefixSubMap(charArrayOf(i))
//        assertEquals(6, subMin.size)
//        for(j in 'a'..'f')
//            assertEquals("$i-$j", subMin[charArrayOf(i,j)])
//
//        //out of subMap range
//        assertNull(sub[charArrayOf('a','z')])
//
//        //min-min case
//        map.put(charArrayOf(i, i), "$i-$i")
//
//        val subMinMin = map.prefixSubMap(charArrayOf(i, i))
//        assertEquals("$i-$i", subMinMin[charArrayOf(i,i)])
//
//        //out of subMinMin range
//        assertNull(subMinMin[charArrayOf(i,'a')])
//    }
}

class Serializer_INT_ARRAY: GroupSerializerTest<IntArray, Any>(){
    override fun randomValue():IntArray {
        val ret = IntArray(random.nextInt(50));
        for(i in 0 until ret.size){
            ret[i] = random.nextInt()
        }
        return ret
    }
    override val serializer = Serializers.INT_ARRAY
//
//    @Test fun prefix_submap(){
//        val map = BTreeMap.make(keySerializer = serializer, valueSerializer = Serializers.STRING)
//        for(i in 1..10) for(j in 1..10) {
//            map.put(intArrayOf(i, j), "$i-$j")
//        }
//
//        //zero subMap
//        assertEquals(0, map.prefixSubMap(intArrayOf(15)).size)
//
//        var i = 5;
//        val sub = map.prefixSubMap(intArrayOf(i))
//        assertEquals(10, sub.size)
//        for(j in 1..10)
//            assertEquals("$i-$j", sub[intArrayOf(i,j)])
//
//        //out of subMap range
//        assertNull(sub[intArrayOf(3,5)])
//
//        //max case
//        i = Int.MAX_VALUE;
//        for(j in 1..10)
//            map.put(intArrayOf(i, j), "$i-$j")
//
//        val subMax = map.prefixSubMap(intArrayOf(i))
//        assertEquals(10, subMax.size)
//        for(j in 1..10)
//            assertEquals("$i-$j", subMax[intArrayOf(i,j)])
//
//        //out of subMap range
//        assertNull(sub[intArrayOf(3,5)])
//
//        //max-max case
//        map.put(intArrayOf(i, i), "$i-$i")
//
//        val subMaxMax = map.prefixSubMap(intArrayOf(i, i))
//        assertEquals("$i-$i", subMaxMax[intArrayOf(i,i)])
//
//        //out of subMaxMax range
//        assertNull(subMaxMax[intArrayOf(i,5)])
//
//        //min case
//        i = Int.MIN_VALUE;
//        for(j in 1..10)
//            map.put(intArrayOf(i, j), "$i-$j")
//
//        val subMin = map.prefixSubMap(intArrayOf(i))
//        assertEquals(10, subMin.size)
//        for(j in 1..10)
//            assertEquals("$i-$j", subMin[intArrayOf(i,j)])
//
//        //out of subMap range
//        assertNull(sub[intArrayOf(3,5)])
//
//        //min-min case
//        map.put(intArrayOf(i, i), "$i-$i")
//
//        val subMinMin = map.prefixSubMap(intArrayOf(i, i))
//        assertEquals("$i-$i", subMinMin[intArrayOf(i,i)])
//
//        //out of subMinMin range
//        assertNull(subMinMin[intArrayOf(i,5)])
//    }
}


class Serializer_LONG_ARRAY: GroupSerializerTest<LongArray, Any>(){
    override fun randomValue():LongArray {
        val ret = LongArray(random.nextInt(30));
        for(i in 0 until ret.size){
            ret[i] = random.nextLong()
        }
        return ret
    }
    override val serializer = Serializers.LONG_ARRAY
//
//    @Test fun prefix_submap(){
//        val map = BTreeMap.make(keySerializer = serializer, valueSerializer = Serializers.STRING)
//        for(i in 1L..10L) for(j in 1L..10L) {
//            map.put(longArrayOf(i, j), "$i-$j")
//        }
//
//        //zero subMap
//        assertEquals(0, map.prefixSubMap(longArrayOf(15)).size)
//
//        var i = 5L;
//        val sub = map.prefixSubMap(longArrayOf(i))
//        assertEquals(10, sub.size)
//        for(j in 1L..10L)
//            assertEquals("$i-$j", sub[longArrayOf(i,j)])
//
//        //out of subMap range
//        assertNull(sub[longArrayOf(3,5)])
//
//        //max case
//        i = Long.MAX_VALUE;
//        for(j in 1L..10L)
//            map.put(longArrayOf(i, j), "$i-$j")
//
//        val subMax = map.prefixSubMap(longArrayOf(i))
//        assertEquals(10, subMax.size)
//        for(j in 1L..10L)
//            assertEquals("$i-$j", subMax[longArrayOf(i,j)])
//
//        //out of subMap range
//        assertNull(sub[longArrayOf(3,5)])
//
//        //max-max case
//        map.put(longArrayOf(i, i), "$i-$i")
//
//        val subMaxMax = map.prefixSubMap(longArrayOf(i, i))
//        assertEquals("$i-$i", subMaxMax[longArrayOf(i,i)])
//
//        //out of subMaxMax range
//        assertNull(subMaxMax[longArrayOf(i,5)])
//
//        //min case
//        i = Long.MIN_VALUE;
//        for(j in 1L..10L)
//            map.put(longArrayOf(i, j), "$i-$j")
//
//        val subMin = map.prefixSubMap(longArrayOf(i))
//        assertEquals(10, subMin.size)
//        for(j in 1L..10L)
//            assertEquals("$i-$j", subMin[longArrayOf(i,j)])
//
//        //out of subMap range
//        assertNull(sub[longArrayOf(3,5)])
//
//        //min-min case
//        map.put(longArrayOf(i, i), "$i-$i")
//
//        val subMinMin = map.prefixSubMap(longArrayOf(i, i))
//        assertEquals("$i-$i", subMinMin[longArrayOf(i,i)])
//
//        //out of subMinMin range
//        assertNull(subMinMin[longArrayOf(i,5)])
//    }
}

class Serializer_DOUBLE_ARRAY: GroupSerializerTest<DoubleArray, Any>(){
    override fun randomValue():DoubleArray {
        val ret = DoubleArray(random.nextInt(30));
        for(i in 0 until ret.size){
            ret[i] = random.nextDouble()
        }
        return ret
    }
    override val serializer = Serializers.DOUBLE_ARRAY
}


class Serializer_JAVA: GroupSerializerTest<Any, Array<Any>>(){
    override fun randomValue() = TT.randomString(10)
    override val serializer = Serializers.JAVA

    override val repeat: Int = 3

    internal class Object2 : Serializable

    open internal class CollidingObject(val value: String) : Serializable {
        override fun hashCode(): Int {
            return this.value.hashCode() and 1
        }

        override fun equals(obj: Any?): Boolean {
            return obj is CollidingObject && obj.value == value
        }
    }

    internal class ComparableCollidingObject(value: String) : CollidingObject(value), Comparable<ComparableCollidingObject>, Serializable {
        override fun compareTo(o: ComparableCollidingObject): Int {
            return value.compareTo(o.value)
        }
    }

    @Test fun clone1(){
        val v = TT.clone(Object2(), Serializers.JAVA)
        assertTrue(v is Object2)
    }

    @Test fun clone2(){
        val v = TT.clone(CollidingObject("111"), Serializers.JAVA)
        assertTrue(v is CollidingObject)
        assertSerEquals("111", (v as CollidingObject).value)
    }

    @Test fun clone3(){
        val v = TT.clone(ComparableCollidingObject("111"), Serializers.JAVA)
        assertTrue(v is ComparableCollidingObject)
        assertSerEquals("111", (v as ComparableCollidingObject).value)

    }

}


//
//class Serializer_ELSA: GroupSerializerTest<Any>(){
//    override fun randomValue() = TT.randomString(10)
//    override val serializer = Serializers.ELSA
//
//    internal class Object2 : Serializable
//
//    open internal class CollidingObject(val value: String) : Serializable {
//        override fun hashCode(): Int {
//            return this.value.hashCode() and 1
//        }
//
//        override fun equals(obj: Any?): Boolean {
//            return obj is CollidingObject && obj.value == value
//        }
//    }
//
//    internal class ComparableCollidingObject(value: String) : CollidingObject(value), Comparable<ComparableCollidingObject>, Serializable {
//        override fun compareTo(o: ComparableCollidingObject): Int {
//            return value.compareTo(o.value)
//        }
//    }
//
//    @Test fun clone1(){
//        val v = TT.clone(Object2(), Serializers.ELSA)
//        assertTrue(v is Object2)
//    }
//
//    @Test fun clone2(){
//        val v = TT.clone(CollidingObject("111"), Serializers.ELSA)
//        assertTrue(v is CollidingObject)
//        assertSerEquals("111", (v as CollidingObject).value)
//    }
//
//    @Test fun clone3(){
//        val v = TT.clone(ComparableCollidingObject("111"), Serializers.ELSA)
//        assertTrue(v is ComparableCollidingObject)
//        assertSerEquals("111", (v as ComparableCollidingObject).value)
//
//    }
//
//}
//


//class Serializer_DB_default: GroupSerializerTest<Any?>(){
//    override fun randomValue() = TT.randomString(11)
//    override val serializer = DBMaker.memoryDB().make().defaultSerializer
//
//    @Test override fun trusted(){
//    }
//}
//

class Serializer_UUID: GroupSerializerTest<UUID, LongArray>(){
    override fun randomValue() = UUID(random.nextLong(), random.nextLong())
    override val serializer = Serializers.UUID
}

class Serializer_FLOAT: GroupSerializerTest<Float, Any>(){
    override fun randomValue() = random.nextFloat()
    override val serializer = Serializers.FLOAT
}

class Serializer_FLOAT_ARRAY: GroupSerializerTest<FloatArray, Any>(){
    override fun randomValue():FloatArray {
        val ret = FloatArray(random.nextInt(50));
        for(i in 0 until ret.size){
            ret[i] = random.nextFloat()
        }
        return ret
    }
    override val serializer = Serializers.FLOAT_ARRAY
}



class Serializer_DOUBLE: GroupSerializerTest<Double, LongArray>(){
    override fun randomValue() = random.nextDouble()
    override val serializer = Serializers.DOUBLE
}

class Serializer_SHORT: GroupSerializerTest<Short, Any>(){
    override fun randomValue() = random.nextInt().toShort()
    override val serializer = Serializers.SHORT
}

class Serializer_SHORT_ARRAY: GroupSerializerTest<ShortArray, Any>(){
    override fun randomValue():ShortArray {
        val ret = ShortArray(random.nextInt(50));
        for(i in 0 until ret.size){
            ret[i] = random.nextInt().toShort()
        }
        return ret
    }
    override val serializer = Serializers.SHORT_ARRAY

//    @Test fun prefix_submap(){
//        val map = BTreeMap.make(keySerializer = serializer, valueSerializer = Serializers.STRING)
//        for(i in 1..10) for(j in 1..10) {
//            map.put(shortArrayOf(i.toShort(), j.toShort()), "$i-$j")
//        }
//
//        //zero subMap
//        assertEquals(0, map.prefixSubMap(shortArrayOf(15)).size)
//
//        var i = 5.toShort();
//        val sub = map.prefixSubMap(shortArrayOf(i))
//        assertEquals(10, sub.size)
//        for(j in 1..10)
//            assertEquals("$i-$j", sub[shortArrayOf(i,j.toShort())])
//
//        //out of subMap range
//        assertNull(sub[shortArrayOf(3,5)])
//
//        //max case
//        i = Short.MAX_VALUE;
//        for(j in 1..10)
//            map.put(shortArrayOf(i, j.toShort()), "$i-$j")
//
//        val subMax = map.prefixSubMap(shortArrayOf(i))
//        assertEquals(10, subMax.size)
//        for(j in 1..10)
//            assertEquals("$i-$j", subMax[shortArrayOf(i,j.toShort())])
//
//        //out of subMap range
//        assertNull(sub[shortArrayOf(3,5)])
//
//        //max-max case
//        map.put(shortArrayOf(i, i), "$i-$i")
//
//        val subMaxMax = map.prefixSubMap(shortArrayOf(i, i))
//        assertEquals("$i-$i", subMaxMax[shortArrayOf(i,i)])
//
//        //out of subMaxMax range
//        assertNull(subMaxMax[shortArrayOf(i,5)])
//
//        //min case
//        i = Short.MIN_VALUE;
//        for(j in 1..10)
//            map.put(shortArrayOf(i, j.toShort()), "$i-$j")
//
//        val subMin = map.prefixSubMap(shortArrayOf(i))
//        assertEquals(10, subMin.size)
//        for(j in 1..10)
//            assertEquals("$i-$j", subMin[shortArrayOf(i,j.toShort())])
//
//        //out of subMap range
//        assertNull(sub[shortArrayOf(3,5)])
//
//        //min-min case
//        map.put(shortArrayOf(i, i), "$i-$i")
//
//        val subMinMin = map.prefixSubMap(shortArrayOf(i, i))
//        assertEquals("$i-$i", subMinMin[shortArrayOf(i,i)])
//
//        //out of subMinMin range
//        assertNull(subMinMin[shortArrayOf(i,5)])
//    }
}

class Serializer_BIG_INTEGER: GroupSerializerTest<BigInteger, Any>(){
    override fun randomValue() = BigInteger(random.nextInt(50), random)
    override val serializer = Serializers.BIG_INTEGER
}

class Serializer_BIG_DECIMAL: GroupSerializerTest<BigDecimal, Any>(){
    override fun randomValue() = BigDecimal(BigInteger(random.nextInt(50), random), random.nextInt(100))
    override val serializer = Serializers.BIG_DECIMAL
}

class Serializer_DATE: GroupSerializerTest<Date, Any>(){
    override fun randomValue() = Date(random.nextLong())
    override val serializer = Serializers.DATE
}


//class SerializerCompressionWrapperTest(): GroupSerializerTest<ByteArray>(){
//    override fun randomValue() = TT.randomByteArray(random.nextInt(1000))
//
//    override val serializer = SerializerCompressionWrapper(Serializers.BYTE_ARRAY as GroupSerializer<ByteArray>)
//
//    @Test
//    fun compression_wrapper() {
//        var b = ByteArray(100)
//        Random().nextBytes(b)
//        assertTrue(Serializers.BYTE_ARRAY.equals(b, TT.clone(b, serializer)))
//
//        b = Arrays.copyOf(b, 10000)
//        assertTrue(Serializers.BYTE_ARRAY.equals(b, TT.clone(b, serializer)))
//
//        val out = DataOutput2()
//        serializer.serialize(out, b)
//        assertTrue(out.pos < 1000)
//    }
//
//}
//
//class Serializer_DeflateWrapperTest(): GroupSerializerTest<ByteArray>() {
//    override fun randomValue() = TT.randomByteArray(random.nextInt(1000))
//    override val serializer = SerializerCompressionDeflateWrapper(Serializers.BYTE_ARRAY as GroupSerializer<ByteArray>)
//
//
//    @Test fun deflate_wrapper() {
//        val c = SerializerCompressionDeflateWrapper(Serializers.BYTE_ARRAY  as GroupSerializer<ByteArray>, -1,
//                byteArrayOf(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 23, 4, 5, 6, 7, 8, 9, 65, 2))
//
//        val b = byteArrayOf(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 4, 5, 6, 3, 3, 3, 3, 35, 6, 67, 7, 3, 43, 34)
//
//        assertTrue(Arrays.equals(b, TT.clone(b, c)))
//    }
//
//}
//
//
//open class Serializer_Array(): GroupSerializerTest<Array<Any>>(){
//    override fun randomValue() = Array<Any>(random.nextInt(30), { TT.randomString(random.nextInt(30))})
//
//    override val serializer = SerializerArray(Serializers.STRING as Serializer<Any>)
//
//    @Test fun array() {
//        val s: Serializer<Array<Any>> = SerializerArray(Serializers.INTEGER as Serializer<Any>)
//
//        val a:Array<Any> = arrayOf(1, 2, 3, 4)
//
//        assertTrue(Arrays.equals(a, TT.clone(a, s)))
//    }
//
//}
//
//
//class Serializer_DeltaArray(): Serializer_Array(){
//
//    //TODO more tests with common prefix
//
//    override val serializer = SerializerArrayDelta(Serializers.STRING as Serializer<Any>)
//
//
//}
//
//
//
//class Serializer_ArrayTuple(): GroupSerializerTest<Array<Any>>(){
//
//    override fun randomValue() = arrayOf(intArrayOf(random.nextInt()), longArrayOf(random.nextLong()))
//
//    override val serializer = SerializerArrayTuple(Serializers.INT_ARRAY, Serializers.LONG_ARRAY)
//
//
//    @Test fun prefix_submap(){
//        val map = BTreeMap.make(keySerializer = SerializerArrayTuple(Serializers.INTEGER, Serializers.LONG), valueSerializer = Serializers.STRING)
//        for(i in 1..10) for(j in 1L..10)
//            map.put(arrayOf(i as Any,j as Any),"$i-$j")
//
//        val sub = map.prefixSubMap(arrayOf(5))
//        assertEquals(10, sub.size)
//        for(j in 1L..10)
//            assertEquals("5-$j", sub[arrayOf(5 as Any,j as Any)])
//
//        assertNull(sub[arrayOf(3 as Any,5 as Any)])
//    }
//
//    @Test fun prefix_comparator(){
//        val s = SerializerArrayTuple(Serializers.INTEGER, Serializers.INTEGER)
//        assertEquals(-1, s.compare(arrayOf(-1), arrayOf(1)))
//        assertEquals(1, s.compare(arrayOf(2), arrayOf(1, null)))
//        assertEquals(-1, s.compare(arrayOf(1), arrayOf(1, null)))
//        assertEquals(-1, s.compare(arrayOf(1), arrayOf(2, null)))
//        assertEquals(-1, s.compare(arrayOf(1,2), arrayOf(1, null)))
//
//        assertEquals(1, s.compare(arrayOf(2), arrayOf(1, 1)))
//        assertEquals(-1, s.compare(arrayOf(1), arrayOf(1, 1)))
//        assertEquals(-1, s.compare(arrayOf(1), arrayOf(2, 1)))
//        assertEquals(1, s.compare(arrayOf(1,2), arrayOf(1, 1)))
//        assertEquals(-1, s.compare(arrayOf(1), arrayOf(1, 2)))
//    }
//}




class SerializerUtilsTest(){
    @Test fun lookup(){
        assertEquals(Serializers.LONG, SerializerUtils.serializerForClass(Long::class.java))
        assertEquals(Serializers.LONG_ARRAY, SerializerUtils.serializerForClass(LongArray::class.java))
        assertEquals(Serializers.UUID, SerializerUtils.serializerForClass(UUID::class.java))
        assertEquals(Serializers.STRING, SerializerUtils.serializerForClass(String::class.java))
        assertNull(SerializerUtils.serializerForClass(Serializer::class.java))
    }

}
